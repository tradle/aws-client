/* eslint-disable no-console */

const zlib = require('zlib')
const { EventEmitter } = require('events')
const _ = require('lodash')
const test = require('tape')
const co = require('co').wrap
const awsIot = require('aws-iot-device-sdk')
const sinon = require('sinon')
// const AWS = require('aws-sdk')
const { TYPE, SIG } = require('@tradle/constants')
const { PREFIX } = require('@tradle/embed')
const IotMessage = require('@tradle/iot-message')
const utils = require('../utils')
const Embeds = require('../embeds')
const {
  Promise,
  post,
  genClientId,
  parseUploadPrefix,
  decodeDataURI
} = utils

const endpoint = 'https://my.aws.api.gateway.endpoint'
const Client = require('../')
const sampleIdentity = require('./fixtures/identity')
const loudAsync = fn => async function (...args) {
  try {
    await fn.apply(this, args)
  } catch (err) {
    console.error(err)
    throw err
  }
}

const loudCo = gen => co(function* (...args) {
  try {
    yield co(gen).apply(this, args)
  } catch (err) {
    console.error(err)
    throw err
  }
})

const awaitReady = client => {
  if (client._state.ready) return Promise.resolve()

  return listenOnce(client, 'ready')
}

const listenOnce = (emitter, event) => new Promise(resolve => emitter.once(event, resolve))

sinon
  .stub(utils, 'serializeMessage')
  .callsFake(obj => {
    const serialized = new Buffer(JSON.stringify(obj))
    serialized.unserialized = obj
    return serialized
  })

const messageLink = '123'
const iotParentTopic = 'ooga'
const iotEndpoint = 'http://localhost:37373'
const sendFixture = {
  message: utils.serializeMessage({
    [TYPE]: 'tradle.Message',
    [SIG]: 'abcd',
    recipientPubKey: {
      curve: 'p256',
      pub: new Buffer('abcd')
    },
    object: {
      [TYPE]: 'somethingelse',
      [SIG]: 'abcd',
      something: 'else'
    }
  }),
  link: messageLink
}

test('fetch with timeout', loudAsync(async (t) => {
  const clock = sinon.useFakeTimers()
  const fetchStub = sinon.stub(utils, '_fetch').callsFake(async () => {
    clock.tick(100)
    return {
      ok: true,
      status: 200
    }
  })

  try {
    await utils.fetch('http://abc.123', { timeout: 50 })
    t.fail('expected timeout')
  } catch (err) {
    t.ok(/timed out/.test(err.message), 'fetch aborted after timeout')
  }

  const uncaughtRejectionHandler = t.fail
  process.on('uncaughtRejection', uncaughtRejectionHandler)
  await utils.fetch('http://abc.123', { timeout: 200 })
  clock.tick(200)
  process.removeListener('uncaughtRejection', uncaughtRejectionHandler)
  t.pass('timer canceled')

  fetchStub.restore()
  clock.restore()
  t.end()
}))

test('resolve embeds', loudAsync(async (t) => {
  const embedsClient = Embeds.createClient()
  const s3Url = 'https://mybucket.s3.amazonaws.com/mykey'
  const object = {
    blah: {
      habla: `${PREFIX.unsigned}${s3Url}`
    }
  }

  const dataUri = 'data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAA='
  const stubFetch = sinon
    .stub(utils, 'fetch')
    .callsFake(function (url) {
      t.equal(url, s3Url)
      return Promise.resolve({
        ok: true,
        status: 200,
        headers: {
          get: header => {
            if (header === 'content-type') {
              return 'image/jpeg'
            }
          }
        },
        arrayBuffer: () => Promise.resolve(toArrayBuffer(Embeds.decodeDataURI(dataUri)))
      })
    })

  await embedsClient.resolve(object)
  t.same(object, {
    blah: {
      habla: dataUri
    }
  })

  t.equal(stubFetch.callCount, 1)
  stubFetch.restore()
  t.end()
}))

test.skip('upload to s3', loudAsync(async (t) => {
  let {
    accessKey,
    secretKey,
    sessionToken,
    challenge,
    // timestamp of request hitting server
    time,
    uploadPrefix
  } = await post(
    'https://7hixz15a6k.execute-api.us-east-1.amazonaws.com/dev/tradle/preauth',
    {
      clientId: genClientId(sampleIdentity.permalink),
      identity: sampleIdentity.object
    }
  )

  const credentials = {
    accessKeyId: accessKey,
    secretAccessKey: secretKey,
    sessionToken
  }

  // console.log(JSON.stringify(credentials, null, 2))

  const dataUrls = Embeds.replaceDataUrls(_.extend({
    object: {
      blah: 'data:image/jpeg;base64,/8j/4AAQSkZJRgABAQAAAQABAAD'
    }
  }, parseUploadPrefix(uploadPrefix)))

  console.log(JSON.stringify(_.extend({
    bucket: dataUrls[0].bucket,
    key: dataUrls[0].key,
    body: dataUrls[0].body.toString('base64')
  }, credentials), null, 2))

  await Embeds.uploadToS3(_.extend(dataUrls[0], { credentials }))
  t.end()
}))

test('init, auth', loudAsync(async (t) => {
  const node = fakeNode()
  const { permalink, identity } = node

  let step = 0
  const preauthResp = {
    time: Date.now(),
    challenge: 'abc',
    iotEndpoint: 'bs.iot.endpoint',
    region: 'd'
  }

  const authResp = getDefaultAuthResponse()

  const stubDevice = sinon.stub(awsIot, 'device').callsFake(function (opts) {
    t.same(opts, {
      accessKeyId: authResp.accessKey,
      secretKey: authResp.secretKey,
      region: preauthResp.region,
      sessionToken: authResp.sessionToken,
      host: preauthResp.iotEndpoint,
      port: 443,
      clientId,
      encoding: 'utf8',
      protocol: 'wss'
    })

    const fakeMqttClient = new EventEmitter()
    fakeMqttClient.end = (force, cb) => process.nextTick(cb || force)
    return fakeMqttClient
  })

  const stubTip = sinon.stub(utils, 'getTip').callsFake(async () => {
    return 0
  })

  const stubPost = sinon.stub(utils, 'post').callsFake(async ({ url, body }) => {
    if (step++ === 0) {
      t.equal(body.clientId, clientId)
      t.equal(body.identity, identity)
      t.equal(url, `${endpoint}/preauth`)
      return preauthResp
    }

    t.equal(step, 2)
    t.equal(url, `${endpoint}/auth`)
    return authResp
  })

  const clientId = permalink.repeat(2)
  const client = new Client({
    endpoint,
    clientId,
    node,
    getSendPosition: () => Promise.resolve(null),
    getReceivePosition: () => Promise.resolve(null),
  })

  client.on('error', err => {
    throw err
  })

  await listenOnce(client, 'authenticated')

  await client.stop()

  stubDevice.restore()
  stubPost.restore()
  stubTip.restore()
  t.end()
}))

test('catch up with server position before sending', loudAsync(async (t) => {
  const node = fakeNode()
  const { permalink, identity } = node
  const clientId = permalink.repeat(2)

  let subscribed = false
  let published = false
  let delivered = false
  let closed = false

  const stubPost = sinon.stub(utils, 'post').callsFake(async ({ url, body }) => {
    if (/preauth/.test(url)) {
      return {
        challenge: 'abc',
        iotParentTopic,
        iotEndpoint,
        time: Date.now()
      }
    }

    return _.extend(getDefaultAuthResponse(), {
      position: serverPos,
    })
  })

  const fakeMqttClient = new EventEmitter()
  fakeMqttClient.end = (force, cb) => {
    closed = true
    process.nextTick(cb || force)
  }

  const stubDevice = sinon.stub(awsIot, 'device').returns(fakeMqttClient)
  fakeMqttClient.publish = async (topic, payload, opts, cb) => {
    t.equal(subscribed, true)
    t.equal(published, false)
    published = true
    cb()

    // artificially delay
    // to check that send() waits for ack
    await wait(100)
    delivered = true
    cb()
    await wait(100)
    fakeMqttClient.handleMessage({
      topic: `${iotParentTopic}/${clientId}/sub/ack`,
      payload: await encodePayload({
        message: {
          link: messageLink
        }
      })
    })
  }

  fakeMqttClient.subscribe = function (topics, opts, cb) {
    t.same(topics, `${iotParentTopic}/${clientId}/sub/+`)
    subscribed = true
    cb()
  }

  const serverPos = {
    sent: {
      link: 'abc',
      time: 123
    },
    received: null
  }

  const serverSentMessage = {
    _t: 'tradle.Message',
    _s: 'sig',
    time: serverPos.sent.time,
    recipientPubKey: {
      curve: 'p256',
      pub: {
        data: [1, 2, 3]
      }
    },
    object: {}
  }

  const client = new Client(_.extend({
    endpoint,
    clientId,
    node
  }, positionToGets(serverPos)))

  client.on('error', err => {
    throw err
  })

  const expected = _.clone(serverSentMessage)
  expected.recipientPubKey.pub = new Buffer(expected.recipientPubKey.pub.data)
  client.onmessage = function (message) {
    t.same(message, expected)
  }

  client.on('messages', function (messages) {
    t.same(messages, [expected])
  })

  client.on('authenticated', () => {
    process.nextTick(() => fakeMqttClient.emit('connect'))
  })

  // should wait till it's caught up to server position
  client.on('ready', t.fail)

  client._timeouts.catchUp = 500

  let sentAnnounce
  const publishStub = sinon.stub(client, 'publish').callsFake(async ({ topic, payload }) => {
    const { type } = await IotMessage.decodeRaw(payload)
    t.equal(type, IotMessage.protobuf.MessageType.announcePosition)
  })

  await wait(100)
  client.removeListener('ready', t.fail)
  const promiseSend = client.send(sendFixture)
  try {
    await Promise.race([
      promiseSend,
      timeoutIn(500)
    ])

    t.fail('sent before ready')
  } catch (err) {
    t.ok(/timed out/.test(err.message))
  }

  await wait(100)
  t.equal(publishStub.callCount, 1)
  publishStub.restore()

  fakeMqttClient.handleMessage({
    topic: `${iotParentTopic}/${clientId}/sub/inbox`,
    payload: await encodePayload({
      messages: [serverSentMessage]
    })
  })

  await awaitReady(client)
  await promiseSend

  t.equal(delivered, true)
  await client.stop()

  t.equal(closed, true)

  stubDevice.restore()
  stubPost.restore()
  t.end()
}))

test('reset on error', loudAsync(async (t) => {
  const node = fakeNode()
  const { permalink, identity } = node
  const clientId = permalink.repeat(2)

  let preauthCount = 0
  let authCount = 0
  const stubPost = sinon.stub(utils, 'post').callsFake(async ({ url, body }) => {
    if (/preauth/.test(url)) {
      preauthCount++
      return {
        time: Date.now(),
        iotParentTopic,
        iotEndpoint,
        challenge: 'abc',
      }
    }

    authCount++
    return getDefaultAuthResponse()
  })

  const fakeMqttClient = new EventEmitter()
  fakeMqttClient.subscribe = function (topics, opts, cb) {
    process.nextTick(cb)
  }

  fakeMqttClient.end = function (force, cb) {
    if (force !== true) {
      triedClose = true
      return hang()
    }

    forcedClose = true
    cb()
  }

  const stubDevice = sinon.stub(awsIot, 'device').returns(fakeMqttClient)
  const stubTip = sinon.stub(utils, 'getTip').callsFake(async () => {
    // return
  })

  let forcedClose = false
  let triedClose = false
  const client = new Client({
    endpoint,
    clientId,
    node,
    getSendPosition: () => Promise.resolve(null),
    getReceivePosition: () => Promise.resolve(null),
  })

  client.on('authenticated', () => {
    process.nextTick(() => fakeMqttClient.emit('connect'))
  })

  await awaitReady(client)
  t.equal(preauthCount, 1)
  t.equal(authCount, 1)
  fakeMqttClient.emit('error', new Error('crap'))
  await wait(0)
  t.equal(forcedClose, true)

  await awaitReady(client)
  t.equal(preauthCount, 2)
  t.equal(authCount, 2)

  await client.stop()

  stubDevice.restore()
  stubPost.restore()
  stubTip.restore()
  t.end()
}))

;[false, true].forEach(retryOnSend => {
  test(`retryOnSend (${retryOnSend})`, loudAsync(async (t) => {
    const node = fakeNode()
    const { permalink, identity } = node
    const bucket = 'mybucket'
    const keyPrefix = 'mykeyprefix'

    let authStep1Failed
    let authStep2Failed
    let subscribeFailed
    // let replaceEmbedsFailed
    let publishFailed

    const clientId = permalink.repeat(2)
    const client = new Client({
      endpoint,
      clientId,
      node,
      getSendPosition: () => Promise.resolve(null),
      getReceivePosition: () => Promise.resolve(null),
      retryOnSend
    })

    client.on('authenticated', () => {
      process.nextTick(() => fakeMqttClient.emit('connect'))
    })

    const fakeMqttClient = new EventEmitter()
    fakeMqttClient.subscribe = (topics, opts, cb) => {
      if (subscribeFailed) {
        process.nextTick(cb)
      } else {
        subscribeFailed = true
        process.nextTick(() => cb(new Error('subscribe failed (test)')))
      }
    }

    fakeMqttClient.publish = (topic, payload, opts, cb) => {
      if (publishFailed) {
        return process.nextTick(async () => {
          cb()
          await wait(100)
          fakeMqttClient.handleMessage({
            topic: `${iotParentTopic}/${clientId}/sub/ack`,
            payload: await encodePayload({
              message: {
                link: messageLink
              }
            })
          })
        })
      }

      publishFailed = true
      process.nextTick(() => cb(new Error('publish failed (test)')))
    }

    fakeMqttClient.end = (force, cb) => process.nextTick(cb || force)

    const stubDevice = sinon.stub(awsIot, 'device').returns(fakeMqttClient)
    const stubPost = sinon.stub(utils, 'post').callsFake(async ({ url, body }) => {
      if (/preauth/.test(url)) {
        if (!authStep1Failed) {
          authStep1Failed = true
          throw new Error('auth step 1 failed (test)')
        }

        return {
          time: Date.now(),
          iotEndpoint,
          iotParentTopic,
          challenge: 'abc'
        }
      }

      if (!authStep2Failed) {
        authStep2Failed = true
        throw new Error('auth step 2 failed (test)')
      }

      return _.extend(getDefaultAuthResponse(), {
        uploadPrefix: `${bucket}/${keyPrefix}`
      })
    })

    // const stubReplaceEmbeds = sinon.stub(client, '_replaceDataUrls').callsFake(async (message) => {
    //   if (!replaceEmbedsFailed) {
    //     replaceEmbedsFailed = true
    //     throw new Error('replace embeds failed (test)')
    //   }

    //   return message
    // })

    if (retryOnSend) {
      await client.send(sendFixture)
    } else {
      try {
        await client.send(sendFixture)
      } catch (err) {
        t.ok(/auth step 1/.test(err.message))
      }

      try {
        await client.send(sendFixture)
      } catch (err) {
        t.ok(/auth step 2/.test(err.message))
      }

      try {
        await client.send(sendFixture)
      } catch (err) {
        t.ok(/subscribe/.test(err.message))
      }

      // try {
      //   await client.send(sendFixture)
      // } catch (err) {
      //   t.ok(/replace embeds/.test(err.message))
      // }

      try {
        await client.send(sendFixture)
      } catch (err) {
        t.ok(/publish/.test(err.message))
      }

      await client.send(sendFixture)
    }

    await client.stop()

    stubDevice.restore()
    stubPost.restore()
    t.end()
  }))
})

test('upload', loudAsync(async (t) => {
  const node = fakeNode()
  const { permalink, identity } = node
  const stubTip = sinon.stub(utils, 'getTip').callsFake(async () => {
    return 0
  })

  const bucket = 'mybucket'
  const keyPrefix = 'mykeyprefix'
  const stubPost = sinon.stub(utils, 'post').callsFake(async ({ url, body }) => {
    if (/preauth/.test(url)) {
      return {
        time: Date.now(),
        iotEndpoint,
        iotParentTopic,
        accessKey: 'abc',
        secretKey: 'def',
        sessionToken: 'ghi',
        challenge: 'abc',
      }
    }

    return _.extend(getDefaultAuthResponse(), {
      uploadPrefix: `${bucket}/${keyPrefix}`
    })
  })

  const fakeMqttClient = new EventEmitter()
  fakeMqttClient.end = (force, cb) => {
    process.nextTick(cb || force)
  }

  fakeMqttClient.subscribe = (topics, opts, cb) => {
    process.nextTick(cb)
  }

  const stubDevice = sinon.stub(awsIot, 'device').callsFake(() => fakeMqttClient)
  const clientId = permalink.repeat(2)
  const client = new Client({
    endpoint,
    clientId,
    node,
    getSendPosition: () => Promise.resolve(null),
    getReceivePosition: () => Promise.resolve(null),
  })

  client.on('authenticated', () => {
    process.nextTick(() => fakeMqttClient.emit('connect'))
  })

  await awaitReady(client)

  const url = `https://${bucket}.s3.amazonaws.com/${keyPrefix}a30f31a6a61325012e8c25deb3bd9b59dc9a2b4350b2b18e3c02dca9a87fea0b`
  client._sendMQTT = async ({ message, link }) => {
    t.equal(message.object.photo, `${PREFIX.unsigned}${url}`)
    t.end()
    return Promise.resolve()
  }

  const stubFetch = sinon
    .stub(utils, 'fetch')
    .callsFake(async (putUrl, request) => {
      t.equal(request.method, 'PUT')
      t.equal(putUrl, url)
      t.same(request.body, new Buffer('ffd8ffe000104a46494600010100000100010000', 'hex'))
      const headers = {
        'content-type': 'application/json; charset=utf-8'
      }

      const res = {
        ok: true,
        status: 200,
        headers: {
          get: name => headers[name]
        },
        text: () => Promise.resolve('{}')
      }

      return res
    })

  const message = {
    [TYPE]: 'tradle.Message',
    object: {
      [TYPE]: 'tradle.Somethingy',
      photo: 'data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD'
    }
  }

  const serialized = new Buffer(JSON.stringify(message))
  serialized.unserialized = {
    link: 'abc',
    object: message
  }

  await client.send({
    message: serialized,
    link: 'abc'
  })

  await client.stop()

  // stubFetch.restore()
  stubDevice.restore()
  stubPost.restore()
  stubTip.restore()
}))

function fakeNode () {
  return {
    permalink: 'a'.repeat(32),
    identity: {},
    sign: obj => {
      return Promise.resolve(_.clone(obj, {
        _s: 'somesig'
      }))
    }
  }
}

function wait (millis) {
  return new Promise(resolve => setTimeout(resolve, millis))
}

function timeoutIn (millis) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      reject(new Error('timed out'))
    }, millis)
  })
}

function tick () {
  return new Promise(process.nextTick)
}

function hang () {
  return new Promise(resolve => {
    // hang
  })
}

function toArrayBuffer (buf) {
  const ab = new ArrayBuffer(buf.length)
  const view = new Uint8Array(ab)
  for (let i = 0; i < buf.length; ++i) {
    view[i] = buf[i]
  }

  return ab
}

function positionToGets (position) {
  return {
    getSendPosition: () => Promise.resolve(position.sent),
    getReceivePosition: () => Promise.resolve(position.received)
  }
}

function encodePayload (payload) {
  return IotMessage.encode({ payload })
}

function getCredentials () {
  return {
    accessKey: 'abc',
    secretKey: 'def',
    sessionToken: 'ghi'
  }
}

function getDefaultAuthResponse () {
  return _.extend(getCredentials(), {
    time: Date.now(),
    position: {
      sent: null,
      received: null
    }
  })
}

// process.on('unhandledRejection', (...args) => {
//   throw args[0]
// })
