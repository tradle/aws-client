
const parseUrl = require('url').parse
const { EventEmitter } = require('events')
const crypto = require('crypto')
const util = require('util')
const pick = require('object.pick')
const awsIot = require('aws-iot-device-sdk')
const bindAll = require('bindall')
const Promise = require('any-promise')
const Ultron = require('ultron')
const { TYPE } = require('@tradle/constants')
const IotMessage = require('@tradle/iot-message')
const RESOLVED = Promise.resolve()
const utils = require('./utils')
const {
  assert,
  extend,
  shallowClone,
  clone,
  co,
  promisify,
  genClientId,
  genNonce,
  stringify,
  prettify,
  isPromise,
  replaceDataUrls,
  parsePrefix,
  resolveEmbeds,
  serializeMessage,
  extractAndUploadEmbeds,
  wait,
  delayThrow,
  putMessage,
  isDeveloperError
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
const SUB_TOPICS = ['inbox', 'ack', 'reject']
// 128 KB but let's leave some wiggle room
// MQTT messages wiggle like it's 1995
const MQTT_MAX_MESSAGE_SIZE = 126 * 1000

module.exports = Client

function Client ({
  node,
  endpoint,
  clientId,
  getSendPosition,
  getReceivePosition,
  encoding=DEFAULT_ENCODING,
  counterparty,
  httpOnly,
  retryOnSend=true
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
  this._encoding = encoding
  this._node = node
  this._counterparty = counterparty
  this._httpOnly = httpOnly
  this._getSendPosition = getSendPosition
  this._getReceivePosition = getReceivePosition
  this._retryOnSend = retryOnSend
  this._reset()
}

util.inherits(Client, EventEmitter)
const proto = Client.prototype

proto._findPosition = co(function* () {
  const promisePosition = Promise.all([
    this._getSendPosition(),
    this._getReceivePosition()
  ])

  try {
    const [sent, received] = yield this._await(promisePosition)
    this._setPosition({ sent, received })
  } catch (err) {
    this._debug('errored out while getting position', err)
  }
})

/**
 * wait for the promise to resolve
 * reject if an error is emitted on this instance
 * optionally timeout
 */
proto._await = co(function* (promise, timeout) {
  const errorWatch = new Ultron(this)
  const awaitError = new Promise((resolve, reject) => errorWatch.once('error', reject))
  const race = [
    promise,
    awaitError
  ]

  if (timeout) {
    race.push(delayThrow(timeout))
  }

  try {
    return yield Promise.race(race)
  } finally {
    errorWatch.destroy()
    if (timeout) {
      race.pop().cancel()
    }
  }
})

proto._debug = function (...args) {
  if (this._counterparty) {
    args.unshift(this._counterparty.slice(0, 6))
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
  return `${this._parentTopic}/${topic}`
}

proto._unprefixTopic = function (topic) {
  return topic.slice(this._parentTopic.length + 1) // trim slash
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
  if (this._ready) return RESOLVED

  return this._promiseListen('ready')
}

proto._authStep1 = co(function* () {
  this._step1Result = yield utils.post(`${this._endpoint}/${paths.preauth}`, {
    clientId: this._clientId,
    identity: this._node.identity
  })

  this._postProcessAuthResponse(this._step1Result)
  return this._step1Result
})

proto._postProcessAuthResponse = function (obj) {
  const { accessKey, secretKey, sessionToken, uploadPrefix } = obj
  if (accessKey) {
    this._credentials = {
      accessKeyId: accessKey,
      secretAccessKey: secretKey,
      sessionToken
    }
  }

  if (uploadPrefix) {
    this._uploadPrefix = uploadPrefix.replace(/^s3:\/\//, '')
    if (!/^https?:\/\//.test(this._uploadPrefix)) {
      this._uploadConfig = parsePrefix(this._uploadPrefix)
    }
  }
}

proto._authStep2 = co(function* () {
  const signed = yield this._node.sign({
    object: {
      [TYPE]: 'tradle.ChallengeResponse',
      clientId: this._clientId,
      challenge: this._step1Result.challenge,
      permalink: this._node.permalink,
      // add our own nonce, to mitigate the case of a malicious server
      // that wants us to sign a predetermined object)
      nonce: genNonce(),
      position: this._position
    }
  })

  this._debug('sending challenge response')
  try {
    this._step2Result = yield utils.post(`${this._endpoint}/${paths.auth}`, signed.object)
  } catch (err) {
    if (/timed\s+out/i.test(err.message)) return this._auth()

    throw err
  }

  this._setCatchUpTarget(this._step2Result.position)
  this._adjustServerTime({
    serverEnd: this._step2Result.time
  })

  this._postProcessAuthResponse(this._step2Result)
  return this._step2Result
})

proto._auth = co(function* () {
  const node = this._node
  const { permalink, identity } = node
  if (!this._clientId) this._clientId = genClientId(permalink)

  const clientId = this._clientId
  this._debug('clientId', clientId)

  this._debug('fetching temporary credentials')

  const {
    s3Endpoint,
    iotEndpoint,
    iotParentTopic,
    region,
    challenge,
    // timestamp of request hitting server
    time
  } = yield this._authStep1()

  this._s3Endpoint = s3Endpoint
  if (!iotEndpoint) {
    this._debug('no "iotEndpoint" returned, will use http only')
  }

  if (iotParentTopic) {
    this._parentTopic = iotParentTopic
  }

  this._region = region

  this._adjustServerTime({
    serverEnd: time
  })

  yield this._authStep2()

  this._debug('authenticated')
  this._authenticated = true
  this.emit('authenticated')

  if (!iotEndpoint) {
    return
  }

  this._debug('initializing mqtt client')
  const [host, port] = iotEndpoint.split(':')
  const client = awsIot.device({
    region,
    protocol: iotEndpoint.startsWith('localhost:') ? 'ws' : 'wss',
    accessKeyId: this._credentials.accessKeyId,
    secretKey: this._credentials.secretAccessKey,
    sessionToken: this._credentials.sessionToken,
    port: port ? Number(port) : 443,
    host: host,
    clientId: this._clientId,
    encoding: this._encoding
  })

  this._client = promisify(client)
  // ignore, handle in this._clientEvents
  this._client.on('error', () => {})

  // override to do a manual PUBACK
  client.handleMessage = this.handleMessage

  this._clientEvents = new Ultron(client)
  this._clientEvents.on('connect', this._onconnect)
  // this._clientEvents.on('packetreceive', (...args) => {
  //   this._debug('PACKETRECEIVE', ...args)
  // })

  // this._clientEvents.on('packetsend', (...args) => {
  //   this._debug('PACKETSEND', ...args)
  // })

  this._clientEvents.on('connect', this._subscribe)

  // this.listenTo(client, 'message', this._onmessage)
  this._clientEvents.on('reconnect', this._onreconnect)
  this._clientEvents.on('offline', this._onoffline)
  this._clientEvents.on('close', this._onclose)
  this._clientEvents.once('error', err => {
    this.emit('error', err)
  })
})

proto._promiseListen = function (event) {
  return this._await(new Promise(resolve => this._myEvents.once(event, resolve)))
}

proto.reset = function reset () {
  this._reset()
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

  const client = this._client
  if (client) {
    // temporary catch
    client.handleMessage = (packet, cb) => {
      this._debug('ignoring event received during reset', packet)
      cb(new Error('resetting'))
    }

    if (this._clientEvents) {
      this._clientEvents.remove()
      this._clientEvents = null
    }

    yield this.close(true)
    this._client = null
  }

  if (delay) {
    this._debug(`waiting ${delay} before next attempt`)
    yield wait(delay)
  }

  if (position) {
    this._setPosition(position)
  } else {
    this._findPosition()
  }
})

proto.publish = co(function* ({ topic, payload, qos=1 }) {
  if (!this._subscribed) {
    yield this._await(this._promiseListen('subscribed'))
  }

  topic = this._prefixTopic(topic)
  this._debug(`publishing to topic: "${topic}"`)
  payload = yield IotMessage.encode({
    payload,
    encoding: 'gzip'
  })

  const ret = yield this._client.publish(topic, payload, { qos })
  this._debug(`published to topic: "${topic}"`)
  return ret
})

proto._onconnect = function () {
  this._debug('connected')
  this.emit('connect')
}

proto._subscribe = co(function* () {
  const topic = this._prefixTopic(`${this._clientId}/sub/+`)
  this._debug(`subscribing to topic: ${topic}`)
  try {
    yield this._client.subscribe(topic, { qos: 1 })
  } catch (err) {
    this._debug('failed to subscribe')
    this.emit('error', err)
    return
  }

  this._debug('subscribed')
  this._subscribed = true
  this.emit('subscribed')
})

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
    payload = yield IotMessage.decode(payload)
    payload = JSON.parse(payload)
  } catch (err) {
    this._debug(`received non-JSON payload, skipping`, payload)
    return
  }

  const actualTopic = topic.slice(this._clientId.length + 5) // cut off /sub/
  switch (actualTopic) {
  case 'inbox':
    yield this._receiveMessages(payload)
    break
  case 'ack':
    this._receiveAck(payload)
    break
  case 'reject':
    this._receiveReject(payload)
    break
  case 'error':
    this.emit('error', new Error(JSON.stringify(payload || '')))
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

  if (!force) {
    this._debug('attempting polite close')
    try {
      yield this._await(client.end(), {
        error: CLOSE_TIMEOUT_ERROR,
        delay: CLOSE_TIMEOUT
      })

      return
    } catch (err) {
      if (err === CLOSE_TIMEOUT_ERROR) {
        this._debug(`polite close timed out after ${CLOSE_TIMEOUT}ms, forcing`)
      } else {
        this._debug('unexpected error on close', err)
      }
    }
  }

  try {
    this._debug('forcing close')
    yield this._await(client.end(true), {
      error: CLOSE_TIMEOUT_ERROR,
      delay: CLOSE_TIMEOUT
    })
  } catch (err2) {
    if (err2 === CLOSE_TIMEOUT_ERROR) {
      this._debug(`force close timed out after ${CLOSE_TIMEOUT}ms`)
    } else {
      this._debug('failed to force close, giving up', err2)
    }
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
    endpoint: this._s3Endpoint,
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
  let attemptsLeft = getAttemptsLeft(this._retryOnSend)
  let err
  while (attemptsLeft-- > 0) {
    try {
      yield this.ready()
      return yield this._await(this._send({ message, link, timeout }))
    } catch (e) {
      if (isDeveloperError(e)) throw e
      err = e
    }
  }

  this.emit('error', err)
  throw err
})

proto._send = co(function* ({ message, link, timeout }) {
  if (this._sending) {
    throw new Error('send one message at a time!')
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

  const useHttp = !this._client ||
    this._httpOnly ||
    message.length * 1.34 > MQTT_MAX_MESSAGE_SIZE

  const send = useHttp ? this._sendHTTP : this._sendMQTT
  this._sending = link
  try {
    return yield send({
      message,
      link,
      timeout: {
        error: SEND_TIMEOUT_ERROR,
        delay: timeout
      }
    })
  } finally {
    if (!useHttp && this._sendMQTTSession) {
      this._sendMQTTSession.destroy()
    }

    this._sending = null
  }
})

proto._sendHTTP = co(function* ({ message, link, timeout }) {
  this._debug('sending over HTTP')
  yield this._await(putMessage(`${this._endpoint}/${paths.message}`, message), timeout)
})

proto._sendMQTT = co(function* ({ message, link, timeout }) {
  this._debug('sending over MQTT')
  if (this._sendMQTTSession) {
    this._sendMQTTSession.destroy()
  }

  this._sendMQTTSession = new Ultron(this)
  const promiseAck = new Promise((resolve, reject) => {
    this._debug(`waiting for ack:${link}`)
    this._sendMQTTSession.once(`ack:${link}`, resolve)
    this._sendMQTTSession.once(`reject:${link}`, reject)
  })

  try {
    const promisePublish = this.publish({
      topic: `${this._clientId}/pub/outbox`,
      // topic: 'message',
      payload: message
    })

    yield this._await(Promise.all([promisePublish, promiseAck]), timeout)
    this._debug('delivered message!')
  } finally {
    this._sending = null
    this._sendMQTTSession.destroy()
    this._sendMQTTSession = null
  }

  // this.emit('sent', message)
})

const getAttemptsLeft = (retries) => {
  if (typeof retries === 'number') return retries
  if (retries === true) return Infinity

  return 1
}
