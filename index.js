
const { EventEmitter } = require('events')
const crypto = require('crypto')
const util = require('util')
const awsIot = require('aws-iot-device-sdk')
const bindAll = require('bindall')
const Promise = require('any-promise')
const {
  co,
  promisify,
  post,
  genClientId,
  genNonce,
  stringify,
  prettify,
  isPromise
} = require('./utils')
// const Restore = require('@tradle/restore')
const debug = require('./debug')
const paths = {
  preauth: 'preauth',
  auth: 'auth',
}

const DEFAULT_ENCODING = 'utf8'
const SUB_TOPICS = ['message', 'ack', 'reject']

module.exports = Client

function Client ({ endpoint, node, position, clientId, encoding=DEFAULT_ENCODING }) {
  EventEmitter.call(this)
  this._endpoint = endpoint.replace(/\/+$/, '')
  this._client = null
  this._clientId = clientId
  this._encoding = encoding
  this._serverAheadMillis = 0
  this._sending = null
  this._promiseReady = new Promise(resolve => this.once('ready', resolve))
  this._promiseAuthenticated = new Promise(resolve => this.once('authenticate', resolve))
  this._promiseSubscribed = new Promise(resolve => this.once('subscribe', resolve))

  bindAll(this)

  this.setNode(node)
  this.setPosition(position)
}

util.inherits(Client, EventEmitter)

Client.prototype._debug = function (...args) {
  if (this._node) {
    args.unshift(this._node.permalink.slice(0, 6))
  }

  debug(...args)
}

Client.prototype._maybeStart = function () {
  if (!(this._node && this._position)) return

  this._auth().catch(err => {
    this._authenticating = false
    this._debug('auth failed', err.stack)
    this.emit('error', err)
    throw err
  })
}

Client.prototype.now = function () {
  return Date.now() + this._serverAheadMillis
}

Client.prototype._adjustServerTime = function ({ localStart, localEnd=Date.now(), serverStart }) {
  const requestTime = localEnd - localStart
  // adjust for delay due to API Gateway
  const adjustedServerStart = serverStart - Math.min(requestTime / 4, 500)
  const serverAheadBy = adjustedServerStart - localStart
  const prev = this._serverAheadMillis || serverAheadBy
  this._serverAheadMillis = (serverAheadBy + prev) / 2
  this._debug(`server clock ahead by ${this._serverAheadMillis}`)
}

Client.prototype.setNode = function (node) {
  if (this._node) throw new Error('node already set')

  this._node = node
  this._maybeStart()
}

Client.prototype.setPosition = function (position) {
  if (this._position) throw new Error('position already set')

  this._debug('client position:', prettify(position))
  this._position = position
  this._maybeStart()
}

/**
 * @param {Object} options.sent     { link, time } of last message queued by server
 * @param {Object} options.received { link, time } of last message received by server
 */
Client.prototype._setCatchUpTarget = function ({ sent, received }) {
  const onCaughtUp = () => {
    this._debug('all caught up!')
    this._ready = true
    this.removeListener('messages', checkIfCaughtUp)
    this.emit('ready')
  }

  const checkIfCaughtUp = messages => {
    const caughtUp = !sent || messages.some(message => {
      return message.link === sent.link || message.time >= sent.time
    })

    if (!caughtUp) {
      this._debug('not caught up yet')
      return
    }

    onCaughtUp()
    return true
  }

  if (!checkIfCaughtUp([this._position.received])) {
    this._debug(`waiting for message: ${prettify(sent)}`)
    this.on('messages', checkIfCaughtUp)
  }
}

Client.prototype.ready = function () {
  return this._promiseReady
}

Client.prototype._auth = co(function* () {
  const node = this._node
  const { permalink, identity } = node
  const clientId = this._clientId || (this._clientId = genClientId(permalink))
  this._debug('clientId', clientId)

  this._debug('fetching temporary credentials')
  let requestStart = Date.now()

  const {
    iotEndpoint,
    region,
    accessKey,
    secretKey,
    sessionToken,
    challenge,
    // timestamp of request hitting server
    time
  } = yield post(`${this._endpoint}/${paths.preauth}`, { clientId, identity })

  this._adjustServerTime({
    localStart: requestStart,
    serverStart: time
  })

  // const iotEndpoint = 'a21zoo1cfp44ha.iot.us-east-1.amazonaws.com'
  // const region = 'us-east-1'
  // const accessKey = 'abc'
  // const secretKey = 'abc'
  // const sessionToken = 'abc'
  // const challenge = 'abc'

  const signed = yield node.sign({
    object: {
      _t: 'tradle.ChallengeResponse',
      clientId,
      challenge,
      permalink,
      // add our own nonce, to mitigate the case of a malicious server
      // that wants us to sign a predetermined object)
      nonce: genNonce(),
      position: this._position
    }
  })

  requestStart = Date.now()
  this._debug('sending challenge response')
  let authResp
  try {
    authResp = yield post(`${this._endpoint}/${paths.auth}`, signed.object)
  } catch (err) {
    if (/timed\s+out/i.test(err.message)) return this._auth()

    this.emit('error', err)
    throw err
  }

  this._setCatchUpTarget(authResp.position)

  this._adjustServerTime({
    localStart: requestStart,
    serverStart: authResp.time
  })

  this._debug('authenticated')
  this.emit('authenticate')

  this._debug('initializing mqtt client')
  const client = this._client = awsIot.device({
    region,
    protocol: 'wss',
    accessKeyId: accessKey,
    secretKey: secretKey,
    sessionToken: sessionToken,
    port: 443,
    host: iotEndpoint,
    clientId: this._clientId,
    encoding: this._encoding
  })

  // override to do a manual PUBACK
  client.handleMessage = this.handleMessage

  this._publish = promisify(client.publish.bind(client))
  this._subscribe = promisify(client.subscribe.bind(client))

  client.on('connect', this._onconnect)
  client.once('connect', co(function* () {
    const topics = `${this._clientId}/#`
    // const topics = SUB_TOPICS.map(topic => `${this._clientId}/${topic}`)
    // const topics = SUB_TOPICS.map(topic => `${this._clientId}/${topic}`)
    this._debug(`subscribing to topic: ${topics}`)
    try {
      yield this._subscribe(topics, { qos: 1 })
    } catch (err) {
      this.emit('error', err)
      this._debug('failed to subscribe')
      return
    }

    this._debug('subscribed')
    this.emit('subscribe')
  }).bind(this))

  // client.on('message', this._onmessage)
  client.on('error', this._onerror)
  client.on('reconnect', this._onreconnect)
  client.on('offline', this._onoffline)
  client.on('close', this._onclose)
})

Client.prototype.publish = co(function* ({ topic, payload, qos=1 }) {
  yield this._promiseAuthenticated
  this._debug(`publishing to topic: "${topic}"`)
  return this._publish(topic, stringify(payload), { qos })
})

Client.prototype._onerror = function (err) {
  this._debug('error', err)
  this.emit('error', err)
}

Client.prototype._onconnect = function () {
  this._debug('connected')
  this.emit('connect')
}

Client.prototype.onmessage = function () {
  throw new Error('override this method')
}

Client.prototype.handleMessage = co(function* (packet, cb) {
  const { topic, payload } = packet
  try {
    yield this._handleMessage(topic.toString(), payload)
  } catch (err) {
    this._debug('message handler failed', err)
    throw err
  } finally {
    cb()
  }
})

Client.prototype._handleMessage = co(function* (topic, payload) {
  this._debug(`received "${topic}" event`)
  try {
    payload = JSON.parse(payload)
  } catch (err) {
    this._debug(`received non-JSON payload, skipping`, payload)
    return
  }

  switch (topic) {
  case `${this._clientId}/message`:
    yield this._receiveMessages(payload)
    break
  case `${this._clientId}/ack`:
    this._receiveAck(payload)
    break
  case `${this._clientId}/reject`:
    this._receiveReject(payload)
    break
  default:
    this._debug(`don't know how to handle "${topic}" events`)
    break
  // case `${this._clientId}/restore`:
  //   this._catchUpServer(payload)
  //   break
  }
})

Client.prototype._receiveAck = function (payload) {
  const { message } = payload
  this._debug(`received ack for message: ${message.link}`)
  this.emit('ack', message)
  // for fine-grained subscription
  this.emit(`ack:${message.link}`, message)
}

Client.prototype._receiveReject = function (payload) {
  const { message, reason, time } = payload
  this._debug(`server rejected message: ${stringify(reason)}`)

  // switch (reason.type) {
  // case 'ClockDrift':
  //   this._serverAheadMillis = time - Date.now() - 50 // travel time latency
  //   break
  // case 'InvalidMessageFormat':
  //   // oh shit!
  //   break
  // case 'TimeTravel'
  //   break
  // }

  const err = new Error(reason.message)
  err.type = reason.type

  this.emit('reject', payload)
  // for fine-grained subscription
  this.emit(`reject:${message.link}`, err)
}

Client.prototype._receiveMessages = co(function* ({ messages }) {
  for (let message of messages) {
    const { recipientPubKey } = message
    const { pub } = recipientPubKey
    if (!Buffer.isBuffer(pub)) {
      recipientPubKey.pub = new Buffer(pub.data)
    }

    const maybePromise = this.onmessage(message)
    if (isPromise(maybePromise)) yield maybePromise
  }

  // this.emit('messages', messages)
  // messages.forEach(message => this.emit('message', message))
})

// Client.prototype._catchUpServer = co(function* (req) {
//   let messages
//   try {
//     messages = yield Restore.conversation.respond({
//       node: this._node,
//       req,
//       sent: true
//     })
//   } catch (err) {
//     this._debug('failed to process "restore" request', err)
//   }

//   for (const message of messages) {
//     try {
//       yield this.send(message)
//     } catch (err) {
//       this._debug('failed to send message in "restore" batch', err)
//     }
//   }
// })

Client.prototype._onoffline = function () {
  this.emit('offline')
  this._debug('offline')
}

Client.prototype._onreconnect = function () {
  this.emit('reconnect')
  this._debug('reconnected')
}

Client.prototype._onclose = function () {
  this.emit('disconnect')
  this._debug('disconnected')
}

Client.prototype.request = co(function* (restore) {
  const { seqs, gt, lt } = restore
  return this.publish({
    topic: `${this._clientId}/restore`,
    payload: restore
  })
})

Client.prototype.send = co(function* ({ message, link }) {
  if (this._sending) {
    throw new Error('send one message at a time!')
  }

  if (!this._ready) {
    throw new Error(`i haven't caught up yet, wait for "ready"`)
  }

  this._sending = link
  yield this._promiseSubscribed

  try {
    yield this.publish({
      // topic: `${this._clientId}/message`,
      topic: 'message',
      payload: {
        // until AWS resolves this issue:
        // https://forums.aws.amazon.com/thread.jspa?messageID=789721
        data: message.toString('base64')
      }
    })

    this._debug(`waiting for ack:${link}`)
    yield new Promise((resolve, reject) => {
      this.once(`ack:${link}`, resolve)
      this.once(`reject:${link}`, reject)
    })

    this._debug('delivered message!')
  } finally {
    this._sending = null
  }

  // this.emit('sent', message)
})
