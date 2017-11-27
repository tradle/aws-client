const nock = require('nock')
const { EventEmitter } = require('events')
const test = require('tape')
const clone = require('xtend')
const co = require('co').wrap
const awsIot = require('aws-iot-device-sdk')
const sinon = require('sinon')
// const AWS = require('aws-sdk')
const { TYPE, SIG } = require('@tradle/constants')
const { PREFIX } = require('@tradle/embed')
const utils = require('../utils')
const {
  extend,
  post,
  genClientId,
  replaceDataUrls,
  uploadToS3,
  parsePrefix,
  decodeDataURI
} = utils

const endpoint = 'https://my.aws.api.gateway.endpoint'
const Client = require('../')
const sampleIdentity = require('./fixtures/identity')
const loudCo = gen => {
  return co(function* (...args) {
    try {
      yield co(gen).apply(this, args)
    } catch (err) {
      console.error(err)
      throw err
    }
  })
}

sinon
  .stub(utils, 'serializeMessage')
  .callsFake(obj => new Buffer(JSON.stringify(obj)))

const messageLink = '123'
const iotParentTopic = 'ooga'
const iotEndpoint = 'http://localhost:37373'
const sendFixture = {
  message: {
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
  },
  link: messageLink
}

test('resolve embeds', loudCo(function* (t) {
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
        headers: {
          get: header => {
            if (header === 'content-type') {
              return 'image/jpeg'
            }
          }
        },
        arrayBuffer: () => Promise.resolve(toArrayBuffer(decodeDataURI(dataUri)))
      })
    })

  yield utils.resolveEmbeds(object)
  t.same(object, {
    blah: {
      habla: dataUri
    }
  })

  t.equal(stubFetch.callCount, 1)
  stubFetch.restore()
  t.end()
}))

test.skip('upload to s3', loudCo(function* (t) {
  let {
    accessKey,
    secretKey,
    sessionToken,
    challenge,
    // timestamp of request hitting server
    time,
    uploadPrefix
  } = yield post(
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

  const dataUrls = replaceDataUrls(extend({
    object: {
      blah: 'data:image/jpeg;base64,/8j/4AAQSkZJRgABAQAAAQABAAD'
    }
  }, parsePrefix(uploadPrefix)))

  console.log(JSON.stringify(extend({
    bucket: dataUrls[0].bucket,
    key: dataUrls[0].key,
    body: dataUrls[0].body.toString('base64')
  }, credentials), null, 2))

  // nock(dataUrls[0].s3Url)
  //   .put(function (url) {
  //     debugger
  //     console.log(url)
  //     return true
  //   })
  //   .reply(200)

  yield uploadToS3(extend(dataUrls[0], { credentials }))
  t.end()
}))

test('init, auth', loudCo(function* (t) {
  const node = fakeNode()
  const { permalink, identity } = node

  let step = 0
  const preauthResp = {
    time: Date.now(),
    challenge: 'abc',
    iotEndpoint: 'bs.iot.endpoint',
    accessKey: 'a',
    secretKey: 'b',
    sessionToken: 'c',
    region: 'd'
  }

  const stubDevice = sinon.stub(awsIot, 'device').callsFake(function (opts) {
    t.same(opts, {
      accessKeyId: preauthResp.accessKey,
      secretKey: preauthResp.secretKey,
      region: preauthResp.region,
      sessionToken: preauthResp.sessionToken,
      host: preauthResp.iotEndpoint,
      port: 443,
      clientId,
      encoding: 'utf8',
      protocol: 'wss'
    })

    return new EventEmitter()
  })

  const stubTip = sinon.stub(utils, 'getTip').callsFake(co(function* () {
    return 0
  }))

  const stubPost = sinon.stub(utils, 'post').callsFake(function (url, data) {
    if (step++ === 0) {
      t.equal(data.clientId, clientId)
      t.equal(data.identity, identity)
      t.equal(url, `${endpoint}/preauth`)
      return preauthResp
    }

    t.equal(step, 2)
    t.equal(url, `${endpoint}/auth`)
    return {
      time: Date.now(),
      position: {
        sent: null,
        received: null
      }
    }
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

  yield client._promiseListen('authenticated')

  stubDevice.restore()
  stubPost.restore()
  stubTip.restore()
  t.end()
}))

test('catch up with server position before sending', loudCo(function* (t) {
  const node = fakeNode()
  const { permalink, identity } = node
  const clientId = permalink.repeat(2)

  let subscribed = false
  let published = false
  let delivered = false
  let closed = false

  const stubPost = sinon.stub(utils, 'post').callsFake(co(function* (url, data) {
    if (/preauth/.test(url)) {
      return {
        iotParentTopic,
        iotEndpoint,
        time: Date.now()
      }
    }

    return {
      time: Date.now(),
      position: serverPos
    }
  }))

  const fakeMqttClient = new EventEmitter()
  fakeMqttClient.end = (force, cb) => {
    process.nextTick(cb || force)
  }

  const stubDevice = sinon.stub(awsIot, 'device').returns(fakeMqttClient)
  fakeMqttClient.publish = co(function* (topic, payload, opts, cb) {
    t.equal(subscribed, true)
    t.equal(published, false)
    published = true
    cb()

    // artificially delay
    // to check that send() waits for ack
    yield wait(100)
    delivered = true
    cb()
    yield wait(100)
    fakeMqttClient.handleMessage({
      topic: `${iotParentTopic}/${clientId}/sub/ack`,
      payload: JSON.stringify({
        message: {
          link: messageLink
        }
      })
    })
  })

  fakeMqttClient.subscribe = function (topics, opts, cb) {
    t.equal(subscribed, false)
    t.same(topics, `${iotParentTopic}/${clientId}/sub/+`)
    subscribed = true
    cb()
  }

  fakeMqttClient.end = function (force, cb) {
    closed = true
    ;(cb || force)()
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

  const client = new Client(extend({
    endpoint,
    clientId,
    node
  }, positionToGets(serverPos)))

  client.on('error', err => {
    throw err
  })

  const expected = clone(serverSentMessage)
  expected.recipientPubKey.pub = new Buffer(expected.recipientPubKey.pub.data)
  client.onmessage = function (message) {
    t.same(message, expected)
  }

  client.on('messages', function (messages) {
    t.same(messages, [expected])
  })

  // should wait till it's caught up to server position
  client.on('ready', t.fail)
  yield wait(100)
  client.removeListener('ready', t.fail)
  const promiseSend = client.send(sendFixture)
  try {
    yield Promise.race([
      promiseSend,
      timeoutIn(500)
    ])

    t.fail('sent before ready')
  } catch (err) {
    t.ok(/timed out/.test(err.message))
  }

  fakeMqttClient.handleMessage({
    topic: `${iotParentTopic}/${clientId}/sub/inbox`,
    payload: JSON.stringify({
      messages: [serverSentMessage]
    })
  })

  yield client.ready()

  fakeMqttClient.emit('connect')

  yield promiseSend

  t.equal(delivered, true)
  yield client.close()

  t.equal(closed, true)

  stubDevice.restore()
  stubPost.restore()
  t.end()
}))

test('reset on error', loudCo(function* (t) {
  const node = fakeNode()
  const { permalink, identity } = node
  const clientId = permalink.repeat(2)

  let preauthCount = 0
  let authCount = 0
  const stubPost = sinon.stub(utils, 'post').callsFake(co(function* (url, data) {
    if (/preauth/.test(url)) {
      preauthCount++
      return {
        time: Date.now(),
        iotParentTopic,
        iotEndpoint
      }
    }

    authCount++
    return {
      time: Date.now(),
      position: {
        sent: null,
        received: null
      }
    }
  }))

  const fakeMqttClient = new EventEmitter()
  fakeMqttClient.end = function (force, cb) {
    if (force !== true) {
      triedClose = true
      return hang()
    }

    forcedClose = true
    cb()
  }

  const stubDevice = sinon.stub(awsIot, 'device').returns(fakeMqttClient)
  const stubTip = sinon.stub(utils, 'getTip').callsFake(co(function* () {
    // return
  }))

  let forcedClose = false
  let triedClose = false
  const client = new Client({
    endpoint,
    clientId,
    node,
    getSendPosition: () => Promise.resolve(null),
    getReceivePosition: () => Promise.resolve(null),
  })

  yield client.ready()
  t.equal(preauthCount, 1)
  t.equal(authCount, 1)
  fakeMqttClient.emit('error', new Error('crap'))
  t.equal(forcedClose, true)

  yield client.ready()
  t.equal(preauthCount, 2)
  t.equal(authCount, 2)

  stubDevice.restore()
  stubPost.restore()
  stubTip.restore()
  t.end()
}))

test('retryOnSend', loudCo(function* (t) {
  const node = fakeNode()
  const { permalink, identity } = node
  const clientId = permalink.repeat(2)
  const fakeMqttClient = new EventEmitter()

  let authStep1Failed
  let authStep2Failed
  let subscribeFailed
  let publishFailed
  let client

  const setup = ({ retryOnSend }) => {
    authStep1Failed = false
    authStep2Failed = false
    subscribeFailed = false
    publishFailed = false
    client = new Client({
      endpoint,
      clientId,
      node,
      getSendPosition: () => Promise.resolve(null),
      getReceivePosition: () => Promise.resolve(null),
      retryOnSend
    })

    client.on('authenticated', () => {
      process.nextTick(() => {
        fakeMqttClient.emit('connect')
      })
    })
  }

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
      return process.nextTick(co(function* () {
        cb()
        yield wait(100)
        fakeMqttClient.handleMessage({
          topic: `${iotParentTopic}/${clientId}/sub/ack`,
          payload: JSON.stringify({
            message: {
              link: messageLink
            }
          })
        })
      }))
    }

    publishFailed = true
    process.nextTick(() => cb(new Error('publish failed (test)')))
  }

  fakeMqttClient.end = (force, cb) => process.nextTick(cb || force)

  const stubDevice = sinon.stub(awsIot, 'device').returns(fakeMqttClient)
  const stubPost = sinon.stub(utils, 'post').callsFake(co(function* (url, data) {
    if (/preauth/.test(url)) {
      if (!authStep1Failed) {
        authStep1Failed = true
        throw new Error('auth step 1 failed (test)')
      }

      return {
        time: Date.now(),
        iotEndpoint,
        iotParentTopic
      }
    }

    if (!authStep2Failed) {
      authStep2Failed = true
      throw new Error('auth step 2 failed (test)')
    }

    return {
      time: Date.now(),
      position: {
        sent: null,
        received: null
      }
    }
  }))

  setup({ retryOnSend: false })

  try {
    yield client.send(sendFixture)
  } catch (err) {
    t.ok(/auth step 1/.test(err.message))
  }

  try {
    yield client.send(sendFixture)
  } catch (err) {
    t.ok(/auth step 2/.test(err.message))
  }

  try {
    yield client.send(sendFixture)
  } catch (err) {
    t.ok(/subscribe/.test(err.message))
  }

  try {
    yield client.send(sendFixture)
  } catch (err) {
    t.ok(/publish/.test(err.message))
  }

  yield client.send(sendFixture)

  setup({ retryOnSend: true })
  yield client.send(sendFixture)

  stubDevice.restore()
  stubPost.restore()
  t.end()
}))

test('upload', loudCo(function* (t) {
  const node = fakeNode()
  const { permalink, identity } = node
  const stubTip = sinon.stub(utils, 'getTip').callsFake(co(function* () {
    return 0
  }))

  const bucket = 'mybucket'
  const keyPrefix = 'mykeyprefix'
  const stubPost = sinon.stub(utils, 'post').callsFake(co(function* (url, data) {
    if (/preauth/.test(url)) {
      return {
        time: Date.now(),
        iotEndpoint,
        iotParentTopic,
        uploadPrefix: `${bucket}/${keyPrefix}`,
        accessKey: 'abc',
        secretKey: 'def',
        sessionToken: 'ghi'
      }
    }

    return {
      time: Date.now(),
      position: {
        sent: null,
        received: null
      }
    }
  }))

  const fakeMqttClient = new EventEmitter()
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

  yield client.ready()
  fakeMqttClient.emit('connect')

  const url = `https://${bucket}.s3.amazonaws.com/${keyPrefix}a30f31a6a61325012e8c25deb3bd9b59dc9a2b4350b2b18e3c02dca9a87fea0b`
  client._sendMQTT = function ({ message, link }) {
    const messageObj = message.unserialized.object
    t.equal(messageObj.object.photo, `${PREFIX.unsigned}${url}`)
    t.end()
    return Promise.resolve()
  }

  const stubFetch = sinon
    .stub(utils, 'fetch')
    .callsFake(co(function* (putUrl, request) {
      t.equal(request.method, 'PUT')
      t.equal(putUrl, url)
      t.same(request.body, new Buffer('ffd8ffe000104a46494600010100000100010000', 'hex'))
      const headers = {
        'content-type': 'application/json; charset=utf-8'
      }

      const res = {
        status: 200,
        headers: {
          get: name => headers[name]
        },
        text: () => Promise.resolve('{}')
      }

      return res
    }))

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

  yield client.send({
    message: serialized,
    link: 'abc'
  })

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
      return Promise.resolve(clone(obj, {
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
  for (var i = 0; i < buf.length; ++i) {
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
