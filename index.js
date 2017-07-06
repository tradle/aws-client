
const { EventEmitter } = require('events')
const crypto = require('crypto')
const util = require('util')
const awsIot = require('aws-iot-device-sdk')
const bindAll = require('bindall')
const Promise = require('any-promise')
const elistener = require('elistener')
const {
  extend,
  co,
  promisify,
  post,
  put,
  genClientId,
  genNonce,
  stringify,
  prettify,
  isPromise,
  getTip
} = require('./utils')
// const Restore = require('@tradle/restore')
const debug = require('./debug')
const paths = {
  preauth: 'preauth',
  auth: 'auth',
  message: 'message'
}

const DEFAULT_ENCODING = 'utf8'
const TOPIC_PREFIX = 'tradle-'
const prefixTopic = topic => `${TOPIC_PREFIX}${topic}`
const unprefixTopic = topic => topic.slice(TOPIC_PREFIX.length)
const SUB_TOPICS = ['message', 'ack', 'reject']
// 128 KB but let's leave some wiggle room
// MQTT messages wiggle like it's 1995
const MQTT_MAX_MESSAGE_SIZE = 126 * 1000

module.exports = Client

function Client ({
  node,
  counterparty,
  endpoint,
  position,
  clientId,
  encoding=DEFAULT_ENCODING
}) {
  EventEmitter.call(this)
  bindAll(this)

  this._endpoint = endpoint.replace(/\/+$/, '')
  this._client = null
  this._clientId = clientId
  this._encoding = encoding
  this._counterparty = counterparty
  this._node = node
  this.reset({ position })
}

util.inherits(Client, EventEmitter)
elistener.install(Client.prototype)

Client.prototype._findPosition = co(function* () {
  const common = {
    node: this._node,
    counterparty: this._counterparty
  }

  const position = yield {
    sent: getTip(extend({ sent: true }, common)),
    received: getTip(common)
  }

  this._setPosition(position)
})

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

// Client.prototype.setNode = function (node) {
//   if (this._node) throw new Error('node already set')

//   this._node = node
//   this._maybeStart()
// }

Client.prototype._setPosition = function (position) {
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
    const caughtUp = messages.some(message => {
      return message.link === sent.link || message.time >= sent.time
    })

    if (!caughtUp) {
      this._debug('not caught up yet')
      return
    }

    onCaughtUp()
    return true
  }

  if (!sent) {
    return onCaughtUp()
  }

  const pos = this._position.received
  if (!(pos && checkIfCaughtUp([pos]))) {
    this._debug(`waiting for message: ${prettify(sent)}`)
    this.listenTo(this, 'messages', checkIfCaughtUp)
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

  this.listenTo(client, 'connect', this._onconnect)
  this.listenOnce(client, 'connect', co(function* () {
    const topics = SUB_TOPICS.map(topic => prefixTopic(`${this._clientId}/${topic}`))
    // const topics = prefixTopic(`${this._clientId}/*`)
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

  // this.listenTo(client, 'message', this._onmessage)
  this.listenTo(client, 'error', this._onerror)
  this.listenTo(client, 'reconnect', this._onreconnect)
  this.listenTo(client, 'offline', this._onoffline)
  this.listenTo(client, 'close', this._onclose)
  this.listenOnce(this, 'error', err => this.reset())
})

Client.prototype._promiseListen = function (event) {
  return new Promise(resolve => this.listenOnce(this, event, resolve))
}

Client.prototype.reset = co(function* (opts={}) {
  const { position } = opts
  this._ready = false
  this._serverAheadMillis = 0
  this._sending = null
  this._position = null
  this.stopListening(this)
  this._promiseReady = this._promiseListen('ready')
  this._promiseAuthenticated = this._promiseListen('authenticate')
  this._promiseSubscribed = this._promiseListen('subscribe')
  const client = this._client
  if (client) {
    this.stopListening(client)
    this._client = null
    yield promisify(client).close(true)
  }

  if (position) {
    this._setPosition(position)
  } else {
    this._findPosition()
  }
})

Client.prototype.publish = co(function* ({ topic, payload, qos=1 }) {
  yield this._promiseAuthenticated
  this._debug(`publishing to topic: "${topic}"`)
  return this._publish(prefixTopic(topic), stringify(payload), { qos })
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
    yield this._handleMessage(unprefixTopic(topic.toString()), payload)
  } catch (err) {
    this._debug('message handler failed', err)
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

  this.emit('messages', messages)
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
    topic: 'restore',
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
  try {
    // 33% overhead from converting to base64
    if (message.length * 1.34 > MQTT_MAX_MESSAGE_SIZE) {
      yield this._sendHTTP({ message, link })
    } else {
      yield this._sendMQTT({ message, link })
    }
  } finally {
    this._sending = null
  }
})

Client.prototype._sendHTTP = co(function* ({ message, link }) {
  this._debug('sending over HTTP')
  yield put(`${this._endpoint}/${paths.message}`, message)
})

Client.prototype._sendMQTT = co(function* ({ message, link }) {
  this._debug('sending over MQTT')
  yield this._promiseSubscribed
  let unsub = []
  const promiseAck = new Promise((resolve, reject) => {
    this._debug(`waiting for ack:${link}`)
    unsub.push(listen(this, `ack:${link}`, resolve, true))
    unsub.push(listen(this, `reject:${link}`, reject, true))
  })

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

    yield promiseAck
    this._debug('delivered message!')
  } finally {
    this._sending = null
    unsub.forEach(fn => fn())
  }

  // this.emit('sent', message)
})

function listen (emitter, event, handler, once) {
  emitter[once ? 'once' : 'on'](event, handler)
  return function unsubscribe () {
    emitter.removeListener(event, handler)
  }
}
