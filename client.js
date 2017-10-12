
const { EventEmitter } = require('events')
const crypto = require('crypto')
const util = require('util')
const awsIot = require('aws-iot-device-sdk')
const bindAll = require('bindall')
const Promise = require('any-promise')
const Ultron = require('ultron')
const { TYPE } = require('@tradle/constants')
const utils = require('./utils')
const {
  assert,
  extend,
  shallowClone,
  clone,
  co,
  promisify,
  processResponse,
  genClientId,
  genNonce,
  stringify,
  prettify,
  isPromise,
  replaceDataUrls,
  parsePrefix,
  resolveEmbeds,
  serializeMessage,
  extractAndUploadEmbeds
} = utils

// const Restore = require('@tradle/restore')
const debug = require('./debug')
const DATA_URL_REGEX = /data:.+\/.+;base64,.*/g
const TESTING = process.env.NODE_ENV === 'test'
const RETRY_DELAY_AFTER_ERROR = TESTING ? 100 : 5000

// const EMBEDDED_DATA_URL_REGEX = /\"data:[^/]+\/[^;]+;base64,[^"]*\"/g
const paths = {
  preauth: 'preauth',
  auth: 'auth',
  message: 'message'
}

const CLOSE_TIMEOUT_ERROR = new Error('close timed out')
const CLOSE_TIMEOUT = 1000
const SEND_TIMEOUT_ERROR = new Error('send timed out')
const SEND_TIMEOUT = 6000
const DEFAULT_ENCODING = 'utf8'
// const TOPIC_PREFIX = 'tradle-'
const SUB_TOPICS = ['message', 'ack', 'reject']
// 128 KB but let's leave some wiggle room
// MQTT messages wiggle like it's 1995
const MQTT_MAX_MESSAGE_SIZE = 126 * 1000

module.exports = Client

function Client ({
  node,
  endpoint,
  clientId,
  topicPrefix='',
  getSendPosition,
  getReceivePosition,
  encoding=DEFAULT_ENCODING,
  httpOnly
}) {
  EventEmitter.call(this)
  bindAll(this)

  assert(typeof endpoint === 'string', 'expected string "endpoint"')
  assert(typeof clientId === 'string', 'expected string "clientId"')
  assert(typeof getSendPosition === 'function', 'expected function "getSendPosition"')
  assert(typeof getReceivePosition === 'function', 'expected function "getReceivePosition"')
  assert(node &&
    typeof node.sign === 'function' &&
    typeof node.permalink === 'string', 'expected "node" with function "sign" and string "permalink"')

  this._endpoint = endpoint.replace(/\/+$/, '')
  this._client = null
  this._clientId = clientId
  this._topicPrefix = topicPrefix
  this._encoding = encoding
  this._node = node
  this._httpOnly = httpOnly
  this._getSendPosition = getSendPosition
  this._getReceivePosition = getReceivePosition
  this._reset()
}

util.inherits(Client, EventEmitter)
const proto = Client.prototype

proto._findPosition = co(function* () {
  const position = yield {
    sent: this._getSendPosition(),
    received: this._getReceivePosition()
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
    this._debug('auth failed')
    this.emit('error', err)
  })
}

proto.now = function () {
  return Date.now() + this._serverAheadMillis
}

proto._adjustServerTime = function ({ localEnd=Date.now(), serverEnd }) {
  // adjust for delay due to API Gateway
  const adjustedServerEnd = serverEnd - 200
  const serverAheadBy = adjustedServerEnd - localEnd
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
    uploadPrefix
  } = yield utils.post(`${this._endpoint}/${paths.preauth}`, { clientId, identity })

  if (iotEndpoint) {
    this._debug('no "iotEndpoint" returned, will use http only')
  }

  if (iotTopicPrefix) {
    this._topicPrefix = iotTopicPrefix
  }

  this._region = region
  this._credentials = {
    accessKeyId: accessKey,
    secretAccessKey: secretKey,
    sessionToken
  }

  if (uploadPrefix) {
    this._uploadPrefix = uploadPrefix.replace(/^s3:\/\//, '')
    if (!/^https?:\/\//.test(this._uploadPrefix)) {
      this._uploadConfig = parsePrefix(this._uploadPrefix)
    }
  }

  this._adjustServerTime({
    serverEnd: time
  })

  const signed = yield node.sign({
    object: {
      [TYPE]: 'tradle.ChallengeResponse',
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
    serverEnd: authResp.time
  })

  this._debug('authenticated')
  this.emit('authenticate')

  if (!iotEndpoint) {
    return
  }

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
  // ignore, handle in this._clientEvents
  this._client.on('error', () => {})

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
      this._debug('failed to subscribe')
      this.emit('error', err)
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
    this.emit('error', err)
  })
})

proto._promiseListen = function (event) {
  return new Promise(resolve => this._myEvents.once(event, resolve))
}

proto._reset = co(function* (opts={}) {
  const { position, delay } = opts
  this._ready = false
  this._serverAheadMillis = 0
  this._sending = null
  this._position = null
  if (this._myEvents) {
    this._myEvents.remove()
  }

  this._myEvents = new Ultron(this)
  this._myEvents.once('error', err => {
    debug('resetting due to error', err.stack)
    this._reset({
      delay: RETRY_DELAY_AFTER_ERROR
    })
  })

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

  if (delay) {
    yield wait(delay)
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
  const ret = yield this._publish(this._prefixTopic(topic), stringify(payload), { qos })
  this._debug(`published to topic: "${topic}"`)
  return ret
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
    if (cb) cb()
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
  messages = yield messages.map(this._processMessage)
  this.emit('messages', messages)
  // messages.forEach(message => this.emit('message', message))
})

proto._processMessage = co(function* (message) {
  const { recipientPubKey } = message
  const { pub } = recipientPubKey
  if (!Buffer.isBuffer(pub)) {
    recipientPubKey.pub = new Buffer(pub.data)
  }

  yield resolveEmbeds(message)
  const maybePromise = this.onmessage(message)
  if (isPromise(maybePromise)) yield maybePromise

  return message
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

  const errorWatch = new Ultron(this)
  const awaitError = new Promise((resolve, reject) => errorWatch.once('error', reject))
  if (!force) {
    const delayedThrow = delayThrow({
      error: CLOSE_TIMEOUT_ERROR,
      delay: CLOSE_TIMEOUT
    })

    try {
      yield Promise.race([
        awaitError,
        client.end(),
        delayedThrow
      ])

      return
    } catch (err) {
      if (err === CLOSE_TIMEOUT_ERROR) {
        this._debug(`polite close timed out after ${CLOSE_TIMEOUT}ms, forcing`)
      } else {
        this._debug('unexpected error on close', err)
      }
    } finally {
      delayedThrow.cancel()
    }
  }

  try {
    yield Promise.race([
      awaitError,
      client.end(true)
    ])
  } catch (err2) {
    this._debug('failed to force close, giving up', err2)
  } finally {
    errorWatch.destroy()
  }
})

// proto.request = co(function* (restore) {
//   const { seqs, gt, lt } = restore
//   return this.publish({
//     topic: 'restore',
//     payload: restore
//   })
// })

proto._replaceDataUrls = co(function* (message) {
  const copy = Buffer.isBuffer(message)
    ? clone(message.unserialized.object)
    : clone(message)

  const changed = yield extractAndUploadEmbeds(extend({
    object: copy,
    region: this._region,
    credentials: this._credentials,
  }, this._uploadConfig))

  if (!changed) return message

  const serialized = utils.serializeMessage(copy)
  serialized.unserialized = shallowClone(message.unserialized || {}, {
    object: copy
  })

  return serialized
})

proto.send = co(function* ({ message, link, timeout=SEND_TIMEOUT }) {
  if (this._sending) {
    throw new Error('send one message at a time!')
  }

  if (!this._ready) {
    throw new Error(`i haven't caught up yet, wait for "ready"`)
  }

  if (this._uploadPrefix) {
    try {
      message = yield this._replaceDataUrls(message)
    } catch (err) {
      // trigger reset
      this.emit('error', err)
      throw err
    }
  }

  this._sending = link
  const useHttp = !this._client ||
    this._httpOnly ||
    message.length * 1.34 > MQTT_MAX_MESSAGE_SIZE

  yield [
    this._promiseSubscribed,
    this._promiseAuthenticated
  ]

  try {
    // 33% overhead from converting to base64
    if (useHttp) {
      yield this._sendHTTP({ message, link, timeout })
    } else {
      yield this._sendMQTT({ message, link, timeout })
    }
  } finally {
    this._sending = null
  }
})

proto._sendHTTP = co(function* ({ message, link, timeout }) {
  this._debug('sending over HTTP')
  const errorWatch = new Ultron(this)
  const delayedThrow = delayThrow({
    error: SEND_TIMEOUT_ERROR,
    delay: timeout
  })

  try {
    yield Promise.race([
      new Promise((resolve, reject) => errorWatch.once('error', reject)),
      putMessage(`${this._endpoint}/${paths.message}`, message),
      delayedThrow
    ])
  } finally {
    errorWatch.destroy()
    delayedThrow.cancel()
  }
})

proto._sendMQTT = co(function* ({ message, link, timeout }) {
  this._debug('sending over MQTT')
  const miniSession = new Ultron(this)
  const promiseAck = new Promise((resolve, reject) => {
    this._debug(`waiting for ack:${link}`)
    miniSession.once(`ack:${link}`, resolve)
    miniSession.once(`reject:${link}`, reject)
    miniSession.once('error', reject)
    this.once('error', reject)
  })

  const delayedThrow = delayThrow({
    error: SEND_TIMEOUT_ERROR,
    delay: timeout
  })

  try {
    const promisePublish = this.publish({
      // topic: `${this._clientId}/message`,
      topic: 'message',
      payload: {
        // until AWS resolves this issue:
        // https://forums.aws.amazon.com/thread.jspa?messageID=789721
        // data: message.toString('base64')
        data: message
      }
    })

    const promiseDone = Promise.all([promisePublish, promiseAck])
    yield Promise.race([
      promiseDone,
      delayedThrow
    ])

    this._debug('delivered message!')
  } catch (err) {
    if (err === SEND_TIMEOUT_ERROR) {
      debug('publish timed out')
      // trigger reset
      this.emit('error', err)
      throw err
    }
  } finally {
    this._sending = null
    miniSession.destroy()
    delayedThrow.cancel()
  }

  // this.emit('sent', message)
})

function delayThrow ({ error, delay }) {
  let canceled
  let timeout
  const promise = new Promise((resolve, reject) => {
    timeout = setTimeout(() => {
      if (!canceled) reject(error)
    }, delay)
  })

  promise.cancel = () => {
    canceled = true
    if (timeout && timeout.unref) timeout.unref()
  }

  return promise
}

function wait (millis) {
  return new Promise(resolve => setTimeout(resolve, millis))
}

const putMessage = co(function* (url, data) {
  const res = yield utils.fetch(url, {
    method: 'PUT',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: /^https?:\/\/localhost:/.test(url)
      ? JSON.stringify({ message: data.unserialized.object })
      : JSON.stringify({ message: data.toString('base64') })
  })

  return processResponse(res)
})
