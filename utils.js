const fetch = require('isomorphic-fetch')
const crypto = require('crypto')
const stringify = JSON.stringify.bind(JSON)
const co = Promise.coroutine || require('co').wrap
const promisify = require('pify')

const post = co(function* (url, data) {
  const res = yield fetch(url, {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: stringify(data)
  })

  const text = yield res.text()
  if (res.status > 300) {
    throw new Error(text)
  }

  if (text.length) return JSON.parse(text)

  return text
})

function genClientId (permalink) {
  return permalink + crypto.randomBytes(20).toString('hex')
}

function genNonce () {
  return crypto.randomBytes(32).toString('hex')
}

function prettify (obj) {
  return stringify(obj, null, 2)
}

function isPromise (obj) {
  return obj && typeof obj.then === 'function'
}

module.exports = {
  co,
  promisify,
  post,
  genClientId,
  genNonce,
  prettify,
  isPromise,
  stringify
}
