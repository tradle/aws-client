
const parseUrl = require('url').parse
const { EventEmitter } = require('events')
const crypto = require('crypto')
const util = require('util')
const extend = require('lodash/extend')
const once = require('lodash/once')
const cloneDeep = require('lodash/cloneDeep')
const awsIot = require('aws-iot-device-sdk')
const bindAll = require('bindall')
const Ultron = require('ultron')
const { TYPE } = require('@tradle/constants')
const IotMessage = require('@tradle/iot-message')
const Errors = require('@tradle/errors')
const utils = require('./utils')
const createState = require('./connection-state')
const CustomErrors = require('./errors')
const {
  Promise,
  RESOLVED,
  assert,
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
  processResponse,
  isLocalUrl,
  isLocalHost,
  closeAwsIotClient,
  series,
} = utils

const zlib = promisify(require('zlib'))

// const Restore = require('@tradle/restore')
const debug = require('./debug')
const DATA_URL_REGEX = /data:.+\/.+;base64,.*/g
const TESTING = process.env.NODE_ENV === 'test'

const getRetryDelay = err => {
  if (TESTING) return 100

  if (Errors.matches(err, CustomErrors.ConnectTimeout)) return 5000
  if (Errors.matches(err, CustomErrors.CloseTimeout)) return 3000
  if (Errors.matches(err, CustomErrors.SendTimeout)) return 1000
  if (Errors.matches(err, CustomErrors.CatchUpTimeout)) return 500
  if (Errors.matches(err, CustomErrors.UploadEmbed)) return 500

  return 5000
}

// const EMBEDDED_DATA_URL_REGEX = /\"data:[^/]+\/[^;]+;base64,[^"]*\"/g
const paths = {
  preauth: 'preauth',
  auth: 'auth',
  inbox: 'inbox'
}

exports = module.exports = Client
exports.AUTH_TIMEOUT = 5000
exports.CLOSE_TIMEOUT = 1000
exports.SEND_TIMEOUT = 6000
exports.CATCH_UP_TIMEOUT = 5000
exports.CONNECT_TIMEOUT = 5000

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
  iotEndpoint,
  parentTopic,
  clientId,
  getSendPosition,
  getReceivePosition,
  encoding=DEFAULT_ENCODING,
  counterparty,
  httpOnly,
  retryOnSend=true,
  maxRequestConcurrency=10,
  autostart=true,
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

  this._endpoint = trimTrailingSlashes(endpoint)
  this._iotEndpoint = iotEndpoint ? trimTrailingSlashes(iotEndpoint) : undefined
  this._parentTopic = parentTopic
  this._client = null
  this._clientId = clientId
  this._encoding = encoding
  this._node = node
  this._counterparty = counterparty
  this._httpOnly = httpOnly
  this._maxRequestConcurrency = maxRequestConcurrency
  this._getSendPosition = getSendPosition
  this._getReceivePosition = getReceivePosition
  this._retryOnSend = retryOnSend

  this._isLocalServer = isLocalUrl(this._endpoint)
  this._name = (counterparty && counterparty.slice(0, 6)) || ''
  this.setMaxListeners(0)

  // not very efficient to have so many listeners
  // maybe just use a wild emitter and log all events
  ;[
    'authenticated',
    'subscribed',
    'caughtUp',
    'resetting',
    'reset',
    'connect',
    'connected',
    'offline',
    'disconnect',
  ].forEach(e => this.on(e, () => this._debug(`event: ${e}`)))

  this.on('authenticated', () => this._state.authenticated = true)
  this.on('subscribed', () => this._state.subscribed = true)
  this.on('caughtUp', () => this._state.caughtUp = true)
  this.on('resetting', () => this._state.resetting = true)
  this.on('reset', () => this._state.resetting = false)
  this.on('connect', () => this._state.connected = true)
  this.on('connected', () => this._state.connected = true)
  this.on('offline', () => this._state.connected = false)
  this.on('disconnect', () => this._state.connected = false)

  // tmp subscription proxy
  // gets reinitialize on every reset
  this._myEvents = new Ultron(this)

  this._startPromise = new Promise(resolve => {
    this.once('start', resolve)
  })

  this._stopPromise = new Promise(resolve => {
    this.once('stop', resolve)
  })

  if (autostart) {
    this.start()
  }
}

util.inherits(Client, EventEmitter)
const proto = Client.prototype

proto._findPosition = async function () {
  const promisePosition = Promise.all([
    this._getSendPosition(),
    this._getReceivePosition()
  ])

  const [sent, received] = await this._await(promisePosition)
  this._setPosition({ sent, received })
}

/**
 * wait for "promise" to resolve. Fail if either:
 * - an error is emitted on this instance
 * - reject on timeout (optional)
 * @param {Promise} [promise] task to wait for
 * @param {Object} opts
 * @param {Object} opts.timeout
 */
proto._await = async function (promise, opts={}) {
  const { timeoutOpts } = opts
  const race = [
    promise,
    this._errorPromise,
  ]

  if (timeoutOpts) {
    race.push(delayThrow(timeoutOpts))
  }

  try {
    return await Promise.race(race)
  } finally {
    if (timeoutOpts) {
      race.pop().cancel()
    }
  }
}

proto._debug = function (...args) {
  if (this._name) {
    args.unshift(this._name)
  }

  debug(...args)
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

proto._setPosition = function (position) {
  if (this._position) {
    throw new Error('position already set')
  }

  this._debug('client position:', prettify(position))
  this._position = position
}

/**
 * @param {Object} options.sent     { link, time } of last message queued by server
 * @param {Object} options.received { link, time } of last message received by server
 */
proto._setCatchUpTarget = function ({ sent, received }) {
  const onCaughtUp = () => {
    this.emit('caughtUp')
    this.removeListener('messages', checkIfCaughtUp)
  }

  const checkIfCaughtUp = messages => {
    const caughtUp = messages.some(message => {
      return /*message.link === sent.link ||*/ getMessageTime(message) >= sent.time
    })

    if (!caughtUp) {
      this._debug('not caught up yet')
      return
    }

    onCaughtUp()
    return true
  }

  if (!sent) {
    onCaughtUp()
    return
  }

  const pos = this._position.received
  if (pos && checkIfCaughtUp([pos])) return

  this._debug(`waiting for message: ${prettify(sent)}`)
  this._myEvents.on('messages', checkIfCaughtUp)
  this._watchCatchUp().catch(err => {
    Errors.rethrow(err, 'developer')
    // otherwise, this is handled by the errorPromise listener
  })
}

proto._watchCatchUp = async function () {
  let error
  const waitForCatchUp = async () => {
    let madeProgress
    let result = await Promise.race([
      wait(exports.CATCH_UP_TIMEOUT),
      this._awaitEvent('messages').then(
        () => madeProgress = true,
        err => error = err
      )
    ])

    // poke server
    if (!madeProgress) await this._announcePosition()

    return this._state.canSend || error
  }

  let stop
  while (!stop) {
    stop = await waitForCatchUp()
  }
}

proto._authStep1 = async function () {
  this._step1Result = await this._await(utils.post({
    url: `${this._endpoint}/${paths.preauth}`,
    body: {
      clientId: this._clientId,
      identity: this._node.identity,
    },
    timeout: exports.AUTH_TIMEOUT,
  }))

  if (!this._step1Result.challenge) {
    const respStr = JSON.stringify(this._step1Result)
    throw new CustomErrors.AuthFailed(`unexpected response: ${respStr}`)
  }

  if (!this._step1Result.iotEndpoint) {
    if (!this._iotEndpoint) throw new CustomErrors.AuthFailed('missing iotEndpoint')

    this._step1Result.iotEndpoint = this._iotEndpoint
  }

  this._postProcessAuthResponse(this._step1Result)
  return this._step1Result
}

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

proto._authStep2 = async function () {
  const signed = await this._node.sign({
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
  this._step2Result = await utils.post({
    url: `${this._endpoint}/${paths.auth}`,
    body: signed.object,
    timeout: exports.AUTH_TIMEOUT,
  })

  this._setCatchUpTarget(this._step2Result.position)
  this._adjustServerTime({
    serverEnd: this._step2Result.time
  })

  this._postProcessAuthResponse(this._step2Result)
  return this._step2Result
}

proto._auth = async function () {
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
  } = await this._authStep1()

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

  await this._authStep2()

  // this._authenticated = true

  if (!iotEndpoint) {
    return
  }

  this._debug('initializing mqtt client')
  const [host, port] = iotEndpoint.split(':')
  const client = awsIot.device({
    region,
    protocol: isLocalHost(iotEndpoint) ? 'ws' : 'wss',
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
  client.handleMessage = this._handleMessage

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
  this._clientEvents.on('error', this._fail)
}

proto._awaitEvent = async function (event) {
  await this._startPromise
  return this._await(new Promise(resolve => this._myEvents.once(event, resolve)))
}

proto._reset = async function () {
  this._state = createState()
  this.emit('resetting')
  this._serverAheadMillis = 0
  this._sending = null
  this._position = null
  if (this._errorPromise && !this._errorPromise.isRejected()) {
    throw new CustomErrors.IllegalInvocation('_reset() can only be called after a failure')
  }

  let rejection
  let fail
  this._errorPromise = new Promise((resolve, reject) => {
    fail = this._fail = err => {
      rejection = err
      reject(err)
    }
  })

  this._errorPromise.catch(err => {
    this._debug('hit an error', err.stack)
  })

  this._errorPromise.isRejected = () => !!rejection

  if (this._myEvents) {
    this._myEvents.remove()
  }

  this._myEvents = new Ultron(this)
  this._myEvents.on('disconnect', async () => {
    const statePromise = this._state.await({ connected: true })
    // if we can't reconnect for a while,
    // throw to trigger reset
    try {
      await this._await(statePromise, {
        timeoutOpts: {
          createError: () => new CustomErrors.ConnectTimeout(`after ${exports.CONNECT_TIMEOUT}ms`),
          delay: exports.CONNECT_TIMEOUT
        }
      })
    } catch (err) {
      if (Errors.matches(err, CustomErrors.ConnectTimeout)) {
        fail(err)
      }

      // other errors are handled automatically
    }
  })

  await this._cleanUp()
}

proto._cleanUp = async function () {
  try {
    return await this._closeAwsClient()
  } catch (err) {
    this._debug('failed to clean up', err)
  }
}

proto._closeAwsClient = async function () {
  const client = this._client
  if (!client) return

  this._debug('closing wrapped aws iot client')
  await closeAwsIotClient({
    client,
    timeout: exports.CLOSE_TIMEOUT,
    force: true,
    log: this._debug,
  })

  if (this._clientEvents) {
    this._clientEvents.remove()
    this._clientEvents = null
  }

  this._client = null
}

proto.publish = async function ({ topic, payload, qos=1 }) {
  await this._await(this._state.await({ canPublish: true }))
  topic = this._prefixTopic(topic)
  this._debug(`publishing to topic: "${topic}"`)
  const ret = await this._await(this._client.publish(topic, payload, { qos }))
  this._debug(`published to topic: "${topic}"`)
  return ret
}

proto._subscribe = async function () {
  if (!this._client) {
    this._debug('client not set up, cannot subscribe')
    return
  }

  const topic = this._prefixTopic(`${this._clientId}/sub/+`)
  this._debug(`subscribing to topic: ${topic}`)
  try {
    await this._client.subscribe(topic, { qos: 1 })
  } catch (err) {
    this._debug('failed to subscribe')
    this._fail(err)
    return
  }

  this.emit('subscribed')
}

proto.onmessage = function () {
  throw new Error('override this method')
}

proto._handleMessage = async function (packet, cb) {
  try {
    const { topic, payload } = packet
    const shortTopic = this._unprefixTopic(topic.toString())
    // to make sure setCatchUpTarget doesn't get confused
    await this._await(this._state.await({ authenticated: true }))
    await this._await(this._tryHandleMessage(shortTopic, payload))
  } catch (err) {
    this._debug('message handler failed', err)
  } finally {
    if (cb) cb()
  }
}

proto._tryHandleMessage = async function (topic, payload) {
  this._debug(`received "${topic}" event`)
  try {
    payload = IotMessage.decodeRaw(payload)
    const { date } = payload.headers
    if (typeof date === 'number') {
      this._debug(`message travel time: ${(Date.now() - date)}, length: ${payload.body.length}`)
    }

    payload = await IotMessage.getBody(payload)
    payload = JSON.parse(payload)
  } catch (err) {
    this._debug(`received invalid payload, skipping`, payload)
    return
  }

  const actualTopic = topic.slice(this._clientId.length + 5) // cut off /sub/
  switch (actualTopic) {
  case 'inbox':
    await this._receiveMessages(payload)
    break
  case 'ack':
    this._receiveAck(payload)
    break
  case 'reject':
    this._receiveReject(payload)
    break
  case 'error':
    this._fail(new Error(JSON.stringify(payload || '')))
    break
  default:
    this._debug(`don't know how to handle "${topic}" events`)
    break
  // case `${this._clientId}/restore`:
  //   this._bringServerUpToDate(payload)
  //   break
  }
}

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

proto._receiveMessages = async function ({ messages }) {
  messages = await Promise.all(messages.map(this._processMessage))
  this.emit('messages', messages)
  // messages.forEach(message => this.emit('message', message))
}

proto._processMessage = async function (message) {
  const { recipientPubKey } = message
  if (recipientPubKey) {
    const { pub } = recipientPubKey
    if (!Buffer.isBuffer(pub)) {
      recipientPubKey.pub = new Buffer(pub.data)
    }
  }

  await resolveEmbeds(message, this._maxRequestConcurrency)
  const maybePromise = this.onmessage(message)
  if (isPromise(maybePromise)) await maybePromise

  return message
}

// proto._bringServerUpToDate = async function (req) {
//   let messages
//   try {
//     messages = await Restore.conversation.respond({
//       node: this._node,
//       req,
//       sent: true
//     })
//   } catch (err) {
//     this._debug('failed to process "restore" request', err)
//   }

//   for (const message of messages) {
//     try {
//       await this.send(message)
//     } catch (err) {
//       this._debug('failed to send message in "restore" batch', err)
//     }
//   }
// }

proto._onconnect = function () {
  this.emit('connect')
}

proto._onreconnect = function () {
  this._debug('reconnecting...')
}

proto._onoffline = function () {
  this.emit('offline')
}

proto._onclose = function () {
  this.emit('disconnect')
}

proto._announcePosition = async function () {
  await this._await(this._state.await({ authenticated: true }))
  this._debug('announcing position')
  await this.publish({
    topic: `${this._clientId}/pub/outbox`,
    payload: await IotMessage.encode({
      type: 'announcePosition',
      payload: this._position,
      encoding: 'identity'
    })
  })
}

// proto.request = async function (restore) {
//   const { seqs, gt, lt } = restore
//   return this.publish({
//     topic: 'restore',
//     payload: restore
//   })
// }

proto._replaceDataUrls = async function (message) {
  const copy = Buffer.isBuffer(message)
    ? cloneDeep(message.unserialized.object)
    : cloneDeep(message)

  const changed = await extractAndUploadEmbeds(extend({
    object: copy,
    region: this._region,
    endpoint: this._s3Endpoint,
    credentials: this._credentials,
  }, this._uploadConfig))

  if (!changed) return message

  const serialized = utils.serializeMessage(copy)
  serialized.unserialized = extend({}, message.unserialized || {}, {
    object: copy
  })

  return serialized
}

// PUBLIC API START

proto.reset = function () {
  this._fail(new CustomErrors.ResetButtonPressed('developer called reset()'))
}

// this loops forever (until client is stopped)
proto.start = async function () {
  if (this._stopping) {
    throw new CustomErrors.IllegalInvocation('already stopping/stopped, create a new instance please')
  }

  if (this._running) {
    // all good
    return
  }

  this._running = true
  this.emit('start')

  // used after first iteration
  let err
  let fail

  const steps = [
    {
      name: 'reset',
      event: 'reset',
      fn: async () => {
        await this._reset()
        fail = this._fail
      }
    },
    {
      name: 'backoff (maybe)',
      fn: () => {
        if (!err) return

        const delay = getRetryDelay(err)
        err = null
        return wait(delay)
      }
    },
    {
      name: 'get my position',
      event: 'position',
      fn: () => this._await(this._findPosition())
    },
    {
      name: 'auth',
      event: 'authenticated',
      fn: () => this._await(this._auth(), {
        timeoutOpts: {
          delay: exports.AUTH_TIMEOUT,
          createError: () => new CustomErrors.Timeout('auth timed out'),
        }
      })
    },
    {
      name: 'catch up with server',
      event: 'ready',
      fn: () => this._await(this._state.await({ canSend: true }), {
        timeoutOpts: {
          delay: exports.CATCH_UP_TIMEOUT,
          createError: () => {
            return new CustomErrors.Timeout('catch-up timed out')
          }
        }
      })
    },
    // wait to fail and start over
    {
      name: 'chill out till the next crisis',
      fn: () => this._errorPromise
    },
  ]

  const runSteps = async () => {
    let currentStepName
    try {
      for (const { name, event, fn } of steps) {
        currentStepName = name
        // this._debug(`step start: ${name}`)
        const result = fn()
        if (isPromise(result)) await result
        if (event) this.emit(event)
        // this._debug(`step end: ${name}`)
      }
    } catch (e) {
      this._debug(`step failed: ${currentStepName}, resetting main loop`)
      err = e
      fail(err)
      if (TESTING) {
        Errors.rethrow(err, 'developer')
      }
    }
  }

  while (!this._stopping) {
    await runSteps()
  }

  await this._reset()
  this.emit('stop')
}

proto.stop = async function (force) {
  if (!this._running) throw new CustomErrors.IllegalInvocation('not running!')

  this._stopping = true
  this._fail(new CustomErrors.StopButtonPressed('developer stopped me'))
  await this._stopPromise
}

proto.now = function () {
  return Date.now() + this._serverAheadMillis
}

proto.send = async function ({ message, link, timeout=exports.SEND_TIMEOUT }) {
  let attemptsLeft = getAttemptsLeft(this._retryOnSend)
  let err
  let iterationStart

  while (attemptsLeft-- > 0 && timeout > 0) {
    let state = this._state
    if (err) this._debug('retrying send')

    iterationStart = Date.now()
    try {
      await this._await(this._state.await({ canSend: true }), {
        timeoutOpts: {
          delay: timeout,
          createError: () => new CustomErrors.SendTimeout('timed out waiting for send prerequisites'),
        }
      })

      return await this._await(this._send({ message, link, timeout }))
    } catch (e) {
      Errors.rethrow(e, 'developer')
      timeout -= (Date.now() - iterationStart)
      err = e
    }
  }

  throw err
}

// PUBLIC API END

proto._send = async function ({ message, link, timeout }) {
  if (this._sending) {
    throw new Error('send one message at a time!')
  }

  if (this._uploadPrefix) {
    try {
      message = await this._replaceDataUrls(message)
    } catch (err) {
      // trigger reset
      this._fail(new CustomErrors.UploadEmbed(err.message))
      throw err
    }
  }

  const length = message.length
  let useHttp
  if (this._httpOnly) {
    this._debug('sending over http: in http-only mode')
    useHttp = true
  } else if (length * 2 > MQTT_MAX_MESSAGE_SIZE) { // gzip will cut the size 5-10x
    this._debug(`sending over http: message too big (${length} bytes)`)
    useHttp = true
  } else if (!this._client) {
    this._debug('sending over http: mqtt client not present')
    useHttp = true
  }

  const send = useHttp ? this._sendHTTP : this._sendMQTT
  this._sending = link
  try {
    return await send({
      message: message.unserialized.object,
      link,
      timeoutOpts: {
        createError: () => new CustomErrors.SendTimeout(`after ${timeout}ms`),
        delay: timeout
      }
    })
  } finally {
    if (!useHttp && this._sendMQTTSession) {
      this._sendMQTTSession.destroy()
      this._sendMQTTSession = null
    }

    this._sending = null
  }
}

proto._sendHTTP = async function ({ message, link, timeoutOpts }) {
  this._debug('sending over HTTP')
  const url = `${this._endpoint}/${paths.inbox}`
  const headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
  }

  let payload = stringify({ messages: [message] })
  if (!this._isLocalServer) {
    payload = await this._await(zlib.gzip(payload))
    headers['Content-Encoding'] = 'gzip'
  }

  const promise = utils.post({
    url,
    headers,
    body: payload
  })

  await this._await(promise, { timeoutOpts })
}

proto._sendMQTT = async function ({ message, link, timeoutOpts }) {
  this._debug('sending over MQTT')
  if (this._sendMQTTSession) {
    this._sendMQTTSession.destroy()
  }

  const payload = await this._await(IotMessage.encode({
    payload: [message],
    encoding: 'gzip'
  }))

  this._sendMQTTSession = new Ultron(this)
  const promiseAck = new Promise((resolve, reject) => {
    this._debug(`waiting for ack:${link}`)
    this._sendMQTTSession.once(`ack:${link}`, resolve)
    this._sendMQTTSession.once(`reject:${link}`, reject)
  })

  const promisePublish = this.publish({
    topic: `${this._clientId}/pub/outbox`,
    // topic: 'message',
    payload
  })

  await this._await(Promise.all([promisePublish, promiseAck]), { timeoutOpts })
  this._debug('delivered message!')
}

const getAttemptsLeft = retries => {
  if (typeof retries === 'number') return retries
  if (retries === true) return Infinity

  return 1
}

const trimTrailingSlashes = str => str.replace(/\/+$/, '')
const getMessageTime = msg => msg._time || msg.time // .time for backwards compat
