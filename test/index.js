const nock = require('nock')
const { EventEmitter } = require('events')
const test = require('tape')
const clone = require('xtend')
const co = require('co').wrap
const awsIot = require('aws-iot-device-sdk')
const sinon = require('sinon')
// const AWS = require('aws-sdk')
const { TYPE, SIG } = require('@tradle/constants')
const utils = require('../utils')
const {
  extend,
  post,
  genClientId,
  replaceDataUrls,
  uploadToS3,
  parsePrefix
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

test('replace data urls', function (t) {
  const prefix = 'mybucket/mykeyprefix'
  const message = {
    [TYPE]: 'tradle.Message',
    object: {
      blah: {
        habla: [{
          photo: "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD"
        }]
      },
      gooblae: "data:image/jpeg;base64,/8j/4AAQSkZJRgABAQAAAQABAAD"
    }
  }

  const photo = 'https://mybucket.s3.amazonaws.com/mykeyprefixa30f31a6a61325012e8c25deb3bd9b59dc9a2b4350b2b18e3c02dca9a87fea0b'
  const gooblae = 'https://mybucket.s3.amazonaws.com/mykeyprefixffd81ef52c22fd853b1db477ceec2a735ef4875a17e18daa8d48a7ce1040c398'
  const dataUrls = replaceDataUrls(extend({
    object: message
  }, parsePrefix(prefix)))

  t.same(dataUrls, [
    {
      "dataUrl": "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD",
      "hash": "a30f31a6a61325012e8c25deb3bd9b59dc9a2b4350b2b18e3c02dca9a87fea0b",
      "body": new Buffer('ffd8ffe000104a46494600010100000100010000', 'hex'),
      "host": "mybucket.s3.amazonaws.com",
      "mimetype": "image/jpeg",
      "path": "object.blah.habla.0.photo",
      "s3Url": "https://mybucket.s3.amazonaws.com/mykeyprefixa30f31a6a61325012e8c25deb3bd9b59dc9a2b4350b2b18e3c02dca9a87fea0b",
      "bucket": "mybucket",
      "key": "mykeyprefixa30f31a6a61325012e8c25deb3bd9b59dc9a2b4350b2b18e3c02dca9a87fea0b"
    },
    {
      "dataUrl": "data:image/jpeg;base64,/8j/4AAQSkZJRgABAQAAAQABAAD",
      "hash": "ffd81ef52c22fd853b1db477ceec2a735ef4875a17e18daa8d48a7ce1040c398",
      "body": new Buffer('ffc8ffe000104a46494600010100000100010000', 'hex'),
      "host": "mybucket.s3.amazonaws.com",
      "mimetype": "image/jpeg",
      "path": "object.gooblae",
      "s3Url": "https://mybucket.s3.amazonaws.com/mykeyprefixffd81ef52c22fd853b1db477ceec2a735ef4875a17e18daa8d48a7ce1040c398",
      "bucket": "mybucket",
      "key": "mykeyprefixffd81ef52c22fd853b1db477ceec2a735ef4875a17e18daa8d48a7ce1040c398"
    }
  ])

  t.same(message, {
    [TYPE]: 'tradle.Message',
    object: {
      blah: {
        habla: [{
          photo
        }]
      },
      gooblae
    }
  })

  t.end()
})

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
    node
  })

  client.on('error', err => {
    throw err
  })

  yield client.ready()

  stubDevice.restore()
  stubPost.restore()
  stubTip.restore()
  t.end()
}))

test('catch up with server position before sending', loudCo(function* (t) {
  const node = fakeNode()
  const { permalink, identity } = node
  const clientId = permalink.repeat(2)
  const messageLink = '123'
  const iotTopicPrefix = 'ooga'

  let subscribed = false
  let published = false
  let delivered = false
  let closed = false

  const stubPost = sinon.stub(utils, 'post').callsFake(co(function* (url, data) {
    if (/preauth/.test(url)) {
      return {
        iotTopicPrefix,
        time: Date.now()
      }
    }

    return {
      time: Date.now(),
      position: serverPos
    }
  }))

  const fakeMqttClient = new EventEmitter()
  const stubDevice = sinon.stub(awsIot, 'device').callsFake(() => {
    return fakeMqttClient
  })

  fakeMqttClient.publish = co(function* (topic, payload, opts, cb) {
    t.equal(subscribed, true)
    t.equal(published, false)
    published = true
    cb()

    // artificially delay
    // to check that send() waits for ack
    yield wait(100)
    delivered = true
    fakeMqttClient.handleMessage({
      topic: `${iotTopicPrefix}${clientId}/ack`,
      payload: JSON.stringify({
        message: {
          link: messageLink
        }
      })
    })
  })

  fakeMqttClient.subscribe = function (topics, opts, cb) {
    t.equal(subscribed, false)
    t.same(topics, ['message', 'ack', 'reject'].map(topic => `${iotTopicPrefix}${clientId}/${topic}`))
    subscribed = true
    cb()
  }

  fakeMqttClient.end = function (cb) {
    closed = true
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

  const client = new Client({
    endpoint,
    clientId,
    node,
    position: serverPos
  })

  client.on('error', err => {
    throw err
  })

  client.onmessage = function (message) {
    t.same()
  }

  // should wait till it's caught up to server position
  client.on('ready', t.fail)
  yield wait(100)
  client.removeListener('ready', t.fail)
  try {
    yield client.send({})
    t.fail('sent before ready')
  } catch (err) {
    t.ok(/ready/.test(err))
  }

  fakeMqttClient.handleMessage({
    topic: `${iotTopicPrefix}${clientId}/message`,
    payload: JSON.stringify({
      messages: [serverSentMessage]
    })
  })

  yield client.ready()

  fakeMqttClient.emit('connect')

  yield client.send({
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
  })

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
        time: Date.now()
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

  const stubTip = sinon.stub(utils, 'getTip').callsFake(co(function* () {
    // return
  }))

  const stubDevice = sinon.stub(awsIot, 'device').callsFake(function (opts) {
    return fakeMqttClient
  })

  let forcedClose = false
  let triedClose = false
  const fakeMqttClient = new EventEmitter()
  fakeMqttClient.end = function (force, cb) {
    if (force !== true) {
      triedClose = true
      return hang()
    }

    forcedClose = true
    cb()
  }

  const client = new Client({
    endpoint,
    clientId,
    node
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

test('upload', loudCo(function* (t) {
  const node = fakeNode()
  const { permalink, identity } = node
  const messageLink = '123'
  const iotTopicPrefix = 'ooga'

  const stubTip = sinon.stub(utils, 'getTip').callsFake(co(function* () {
    return 0
  }))

  const bucket = 'mybucket'
  const keyPrefix = 'mykeyprefix'
  const stubPost = sinon.stub(utils, 'post').callsFake(co(function* (url, data) {
    if (/preauth/.test(url)) {
      return {
        time: Date.now(),
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

  const stubDevice = sinon
    .stub(awsIot, 'device')
    .callsFake(opts => new EventEmitter())

  const stubSerialize = sinon
    .stub(utils, 'serializeMessage')
    .callsFake(obj => new Buffer(JSON.stringify(obj)))

  const clientId = permalink.repeat(2)
  const client = new Client({ endpoint, clientId, node })
  yield client.ready()

  const url = `https://${bucket}.s3.amazonaws.com/${keyPrefix}a30f31a6a61325012e8c25deb3bd9b59dc9a2b4350b2b18e3c02dca9a87fea0b`
  client._sendMQTT = function ({ message, link }) {
    t.equal(message.unserialized.object.photo, url)
    t.end()
    return Promise.resolve()
  }

  const stubFetch = sinon
    .stub(utils, 'fetch')
    .callsFake(function (putUrl, request) {
      t.equal(request.method, 'PUT')
      t.equal(putUrl, url)
      t.same(request.body, new Buffer('ffd8ffe000104a46494600010100000100010000', 'hex'))
      return Promise.resolve({
        text: () => Promise.resolve()
      })
    })

  yield client.send({
    message: {
      [TYPE]: 'tradle.Message',
      object: {
        [TYPE]: 'tradle.Somethingy',
        photo: 'data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD'
      }
    },
    link: 'abc'
  })

  // stubFetch.restore()
  stubSerialize.restore()
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

function tick () {
  return new Promise(process.nextTick)
}

function hang () {
  return new Promise(resolve => {
    // hang
  })
}
