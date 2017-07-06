const { EventEmitter } = require('events')
const test = require('tape')
const clone = require('xtend')
const co = require('co').wrap
const awsIot = require('aws-iot-device-sdk')
const sinon = require('sinon')
const utils = require('../utils')
const endpoint = 'https://my.aws.api.gateway.endpoint'
const Client = require('../')
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

  let subscribed = false
  let published = false
  let delivered = false
  let closed = false

  const stubPost = sinon.stub(utils, 'post').callsFake(function (url, data) {
    if (/preauth/.test(url)) {
      return Promise.resolve({
        time: Date.now()
      })
    }

    return Promise.resolve({
      position: serverPos
    })
  })

  const fakeMqttClient = new EventEmitter()
  const stubDevice = sinon.stub(awsIot, 'device').callsFake(() => {
    return fakeMqttClient
  })

  fakeMqttClient.publish = function (topic, payload, opts, cb) {
    t.equal(subscribed, true)
    t.equal(published, false)
    published = true
    cb()

    // artificially delay
    // to check that send() waits for ack
    setTimeout(function () {
      delivered = true
      fakeMqttClient.handleMessage({
        topic: `tradle-${clientId}/ack`,
        payload: JSON.stringify({
          message: {
            link: messageLink
          }
        })
      })
    }, 100)
  }

  fakeMqttClient.subscribe = function (topics, opts, cb) {
    t.equal(subscribed, false)
    t.same(topics, ['message', 'ack', 'reject'].map(topic => `tradle-${clientId}/${topic}`))
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
    topic: `tradle-${clientId}/message`,
    payload: JSON.stringify({
      messages: [serverSentMessage]
    })
  })

  yield client.ready()

  fakeMqttClient.emit('connect')

  yield client.send({
    message: {
      _t: 'hello'
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
  return new Promise(resolve => setTimeout(resolve, 100))
}
