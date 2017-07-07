
const { EventEmitter } = require('events')
const crypto = require('crypto')
const util = require('util')
const awsIot = require('aws-iot-device-sdk')
const bindAll = require('bindall')
const Promise = require('any-promise')
const Ultron = require('ultron')
const utils = require('./utils')
const {
  extend,
  co,
  promisify,
  put,
  genClientId,
  genNonce,
  stringify,
  prettify,
  isPromise,
} = utils

// const Restore = require('@tradle/restore')
const debug = require('./debug')
const paths = {
  preauth: 'preauth',
  auth: 'auth',
  message: 'message'
}

const CLOSE_TIMEOUT_ERROR = new Error('close timed out')
const CLOSE_TIMEOUT = 1000
const DEFAULT_ENCODING = 'utf8'
// const TOPIC_PREFIX = 'tradle-'
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
  topicPrefix,
  encoding=DEFAULT_ENCODING,
  httpOnly=false
}) {
  EventEmitter.call(this)
  bindAll(this)

  this._endpoint = endpoint.replace(/\/+$/, '')
  this._client = null
  this._clientId = clientId
  this._topicPrefix = topicPrefix
  this._encoding = encoding
  this._counterparty = counterparty
  this._node = node
  this._httpOnly = httpOnly
  this._reset({ position })
}

util.inherits(Client, EventEmitter)
const proto = Client.prototype

proto._findPosition = co(function* () {
  const common = {
    node: this._node,
    counterparty: this._counterparty
  }

  const position = yield {
    sent: utils.getTip(extend({ sent: true }, common)),
    received: utils.getTip(common)
  }

  this._setPosition(position)
})

proto._debug = function (...args) {
  if (this._node) {
    args.unshift(this._node.permalink.slice(0, 6))
  }

  debug(...args)
}

proto._maybeStart = function () {
  if (!(this._node && this._position)) return

  this._auth().catch(err => {
    this._authenticating = false
    this._debug('auth failed', err.stack)
    this.emit('error', err)
  })
}

proto.now = function () {
  return Date.now() + this._serverAheadMillis
}

proto._adjustServerTime = function ({ localStart, localEnd=Date.now(), serverStart }) {
  const requestTime = localEnd - localStart
  // adjust for delay due to API Gateway
  const adjustedServerStart = serverStart - Math.min(requestTime / 4, 500)
  const serverAheadBy = adjustedServerStart - localStart
  const prev = this._serverAheadMillis || serverAheadBy
  this._serverAheadMillis = (serverAheadBy + prev) / 2
  this._debug(`server clock ahead by ${this._serverAheadMillis}`)
}

proto._prefixTopic = function (topic) {
  return `${this._topicPrefix || ''}${topic}`
}

proto._unprefixTopic = function (topic) {
  return topic.slice((this._topicPrefix || '').length)
}

// proto.setNode = function (node) {
//   if (this._node) throw new Error('node already set')

//   this._node = node
//   this._maybeStart()
// }

proto._setPosition = function (position) {
  if (this._position) throw new Error('position already set')

  this._debug('client position:', prettify(position))
  this._position = position
  this._maybeStart()
}

/**
 * @param {Object} options.sent     { link, time } of last message queued by server
 * @param {Object} options.received { link, time } of last message received by server
 */
proto._setCatchUpTarget = function ({ sent, received }) {
  const onCaughtUp = () => {
    this._debug('all caught up!')
    this._ready = true
    this.removeListener('messages', checkIfCaughtUp)
    this.emit('ready')
  }

  const checkIfCaughtUp = messages => {
    const caughtUp = messages.some(message => {
      return /*message.link === sent.link ||*/ message.time >= sent.time
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
    this._myEvents.on('messages', checkIfCaughtUp)
  }
}

proto.ready = function () {
  return this._promiseReady
}

proto._auth = co(function* () {
  const node = this._node
  const { permalink, identity } = node
  const clientId = this._clientId || (this._clientId = genClientId(permalink))
  this._debug('clientId', clientId)

  this._debug('fetching temporary credentials')
  let requestStart = Date.now()

  const {
    iotEndpoint,
    iotTopicPrefix,
    region,
    accessKey,
    secretKey,
    sessionToken,
    challenge,
    // timestamp of request hitting server
    time,
  } = yield utils.post(`${this._endpoint}/${paths.preauth}`, { clientId, identity })

  if (iotTopicPrefix) {
    this._topicPrefix = iotTopicPrefix
  }

  this._adjustServerTime({
    localStart: requestStart,
    serverStart: time
  })

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
    authResp = yield utils.post(`${this._endpoint}/${paths.auth}`, signed.object)
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
  const client = awsIot.device({
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

  this._client = promisify(client)
  this._publish = this._client.publish
  this._subscribe = this._client.subscribe

  // override to do a manual PUBACK
  client.handleMessage = this.handleMessage

  this._clientEvents = new Ultron(client)
  this._clientEvents.on('connect', this._onconnect)
  this._clientEvents.once('connect', co(function* () {
    const topics = SUB_TOPICS.map(topic => this._prefixTopic(`${this._clientId}/${topic}`))
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
  this._clientEvents.on('reconnect', this._onreconnect)
  this._clientEvents.on('offline', this._onoffline)
  this._clientEvents.on('close', this._onclose)
  this._clientEvents.once('error', err => {
    this._debug('error', err)
    this.emit('error', err)
  })
})

proto._promiseListen = function (event) {
  return new Promise(resolve => this._myEvents.once(event, resolve))
}

proto._reset = co(function* (opts={}) {
  const { position } = opts
  this._ready = false
  this._serverAheadMillis = 0
  this._sending = null
  this._position = null
  if (this._myEvents) {
    this._myEvents.remove()
  }

  this._myEvents = new Ultron(this)
  this._myEvents.once('error', this._reset)
  this._promiseReady = this._promiseListen('ready')
  this._promiseAuthenticated = this._promiseListen('authenticate')
  this._promiseSubscribed = this._promiseListen('subscribe')
  const client = this._client
  if (client) {
    this._clientEvents.remove()
    this._clientEvents = null
    yield this.close(true)
    this._client = null
  }

  if (position) {
    this._setPosition(position)
  } else {
    this._findPosition()
  }
})

proto.publish = co(function* ({ topic, payload, qos=1 }) {
  yield this._promiseAuthenticated
  this._debug(`publishing to topic: "${topic}"`)
  return this._publish(this._prefixTopic(topic), stringify(payload), { qos })
})

proto._onconnect = function () {
  this._debug('connected')
  this.emit('connect')
}

proto.onmessage = function () {
  throw new Error('override this method')
}

proto.handleMessage = co(function* (packet, cb) {
  const { topic, payload } = packet
  try {
    yield this._handleMessage(this._unprefixTopic(topic.toString()), payload)
  } catch (err) {
    this._debug('message handler failed', err)
  } finally {
    cb()
  }
})

proto._handleMessage = co(function* (topic, payload) {
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
  //   this._bringServerUpToDate(payload)
  //   break
  }
})

proto._receiveAck = function (payload) {
  const { message } = payload
  this._debug(`received ack for message: ${message.link}`)
  this.emit('ack', message)
  // for fine-grained subscription
  this.emit(`ack:${message.link}`, message)
}

proto._receiveReject = function (payload) {
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

proto._receiveMessages = co(function* ({ messages }) {
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

// proto._bringServerUpToDate = co(function* (req) {
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

proto._onoffline = function () {
  this.emit('offline')
  this._debug('offline')
}

proto._onreconnect = function () {
  this.emit('reconnect')
  this._debug('reconnected')
}

proto._onclose = function () {
  this.emit('disconnect')
  this._debug('disconnected')
}

proto.close = co(function* (force) {
  const client = this._client
  if (!client) return

  if (!force) {
    try {
      yield Promise.race([
        client.end(),
        timeoutIn(CLOSE_TIMEOUT)
      ])

      return
    } catch (err) {
      if (err === CLOSE_TIMEOUT_ERROR) {
        this._debug(`nice close timed out after ${CLOSE_TIMEOUT}ms, forcing`)
      } else {
        this._debug('unexpected error on close', err)
      }
    }
  }

  try {
    yield client.end(true)
  } catch (err2) {
    this._debug('failed to force close, giving up', err2)
  }
})

// proto.request = co(function* (restore) {
//   const { seqs, gt, lt } = restore
//   return this.publish({
//     topic: 'restore',
//     payload: restore
//   })
// })

proto.send = co(function* ({ message, link }) {
  if (this._sending) {
    throw new Error('send one message at a time!')
  }

  if (!this._ready) {
    throw new Error(`i haven't caught up yet, wait for "ready"`)
  }

  this._sending = link
  try {
    // 33% overhead from converting to base64
    if (this._httpOnly || message.length * 1.34 > MQTT_MAX_MESSAGE_SIZE) {
      yield this._sendHTTP({ message, link })
    } else {
      yield this._sendMQTT({ message, link })
    }
  } finally {
    this._sending = null
  }
})

proto._sendHTTP = co(function* ({ message, link }) {
  this._debug('sending over HTTP')
  yield put(`${this._endpoint}/${paths.message}`, message)
})

proto._sendMQTT = co(function* ({ message, link }) {
  this._debug('sending over MQTT')
  yield this._promiseSubscribed
  let unsub = []
  const promiseAck = new Promise((resolve, reject) => {
    this._debug(`waiting for ack:${link}`)
    unsub.push(listen(this._myEvents, `ack:${link}`, resolve, true))
    unsub.push(listen(this._myEvents, `reject:${link}`, reject, true))
  })

  try {
    yield this.publish({
      // topic: `${this._clientId}/message`,
      topic: 'message',
      payload: {
        // until AWS resolves this issue:
        // https://forums.aws.amazon.com/thread.jspa?messageID=789721
        // data: message.toString('base64')
        data: message
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
    emitter.remove(event, handler)
  }
}

function timeoutIn (millis) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      reject(CLOSE_TIMEOUT_ERROR)
    }, millis)
  })
}
