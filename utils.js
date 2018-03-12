const parseURL = require('url').parse
const IP = require('ip')
const Promise = require('bluebird')
const { AssertionError } = require('assert')
const crypto = require('crypto')
const omit = require('lodash/omit')
const extend = require('lodash/extend')
const fetch = require('isomorphic-fetch')
const stringify = JSON.stringify.bind(JSON)
const co = require('co').wrap
const promisify = require('pify')
const { AwsSigner } = require('aws-sign-web')
// const minio = require('minio')
const { serializeMessage } = require('@tradle/engine').utils
const {
  replaceDataUrls,
  resolveEmbeds,
  decodeDataURI,
  encodeDataURI
} = require('@tradle/embed')

const fetchImpl = require('./fetch')

const RESOLVED = Promise.resolve()
const FETCH_TIMED_OUT = new Error('fetch timed out')

const redirectTypeErrors = err => {
  if (err instanceof TypeError) {
    throw new Error(err.message)
  }

  throw err
}

const wrappedFetch = co(function* (url, opts={}) {
  const { timeout } = opts
  if (!timeout) return yield utils._fetch(url, opts).catch(redirectTypeErrors)

  const timeBomb = delayThrow({
    error: new Error(`fetch timed out after: ${timeout}ms`),
    delay: timeout
  })

  const result = yield Promise.race([
    timeBomb,
    utils._fetch(url, omit(opts, 'timeout')).catch(redirectTypeErrors)
  ])

  timeBomb.cancel()
  return result
})

const post = co(function* (url, data, opts={}) {
  const res = yield utils.fetch(url, extend({
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: stringify(data)
  }, opts))

  return processResponse(res)
})

const processResponse = co(function* (res) {
  if (!res.ok || res.status > 300) {
    throw new Error(res.statusText)
  }

  let text = yield res.text()
  const contentType = res.headers.get('content-type') || ''
  if (contentType.startsWith('application/json')) {
    try {
      return JSON.parse(text)
    } catch (err) {
      // hack to support serverless-offline targets
      text = new Buffer(text, 'base64').toString()
      return JSON.parse(text)
    }
  }

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

function getTip ({ node, counterparty, sent }) {
  const from = sent ? node.permalink : counterparty
  const to = sent ? counterparty : node.permalink
  const seqOpts = {}
  const base = from + '!' + to
  seqOpts.gte = base + '!'
  seqOpts.lte = base + '\xff'
  seqOpts.reverse = true
  seqOpts.limit = 1
  // console.log(seqOpts)
  const source = node.objects.bySeq(seqOpts)
  return new Promise((resolve, reject) => {
    source.on('error', reject)
    source.on('data', data => resolve({
      time: data.timestamp,
      link: data.link
    }))

    source.on('end', () => resolve(null))
  })
}

function parsePrefix (prefix) {
  prefix = prefix.replace(/^(?:https?|s3):\/\//, '')
  const idx = prefix.indexOf('/')
  const bucket = prefix.slice(0, idx)
  const keyPrefix = prefix.slice(idx + 1)
  return { bucket, keyPrefix }
}

const extractAndUploadEmbeds = co(function* (opts) {
  const { object, region, credentials } = opts
  const replacements = replaceDataUrls(opts)
  if (replacements.length) {
    yield replacements.map(replacement => {
      replacement.region = region
      replacement.credentials = credentials
      return uploadToS3(replacement)
    })

    return true
  }
})

function sha256 (strOrBuffer) {
  return crypto.createHash('sha256').update(strOrBuffer).digest('hex')
}

const uploadToS3 = co(function* ({
  region='us-east-1',
  credentials,
  bucket,
  key,
  body,
  mimetype,
  host,
  s3Url
}) {
  const signer = new AwsSigner(extend({
    service: 's3',
    region,
  }, credentials))

  const request = {
    method: 'PUT',
    url: s3Url,
    headers: {
      "Content-Type": mimetype,
      "Content-Length": body.length,
      "Host": host,
      "x-amz-content-sha256": 'UNSIGNED-PAYLOAD',
    },
    body
  }

  if (credentials.sessionToken) {
    request.headers['x-amz-security-token'] = credentials.sessionToken
  }

  request.headers = signer.sign(request)
  const res = yield utils.fetch(request.url, request)
  yield processResponse(res)
  return res
})

const download = co(function* ({ url }) {
  const res = yield utils.fetch(url)
  if (res.status > 300) {
    const text = yield res.text()
    throw new Error(text)
  }

  const arrayBuffer = yield res.arrayBuffer()
  const buf = new Buffer(arrayBuffer)
  buf.mimetype = res.headers.get('content-type')
  return buf
})

const resolveS3Urls = (object, concurrency=10) => {
  return resolveEmbeds({ object, resolve: download, concurrency })
}

const assert = (statement, errMsg) => {
  if (!statement) throw new Error(errMsg || 'assertion failed')
}

const delayThrow = ({ error, delay }) => {
  const promise = defer()
  const timeout = setTimeout(() => promise.reject(error), delay)
  promise.cancel = () => {
    clearTimeout(timeout)
    promise.resolve()
  }

  return promise
}

const wait = millis => {
  return new Promise(resolve => setTimeout(resolve, millis))
}

const defer = () => {
  let _resolve
  let _reject
  let p = new Promise((resolve, reject) => {
    [_resolve, _reject] = [resolve, reject]
  })

  p.resolve = _resolve
  p.reject = _reject
  return p
}

const defineGetter = (obj, prop, getter) => {
  Object.defineProperty(obj, prop, {
    get: getter
  })
}

const isLocalUrl = url => {
  const { hostname } = parseURL(url)
  return isLocalHost(hostname)
}

const isLocalHost = host => {
  host = host.split(':')[0]
  if (host === 'localhost') return true

  const isIP = IP.isV4Format(host) || IP.isV6Format(host)
  return isIP && IP.isPrivate(host)
}

const utils = module.exports = {
  Promise,
  RESOLVED,
  co,
  promisify,
  post,
  processResponse,
  genClientId,
  genNonce,
  prettify,
  isPromise,
  stringify,
  getTip,
  replaceDataUrls,
  resolveEmbeds: resolveS3Urls,
  sha256,
  serializeMessage,
  uploadToS3,
  extractAndUploadEmbeds,
  parsePrefix,
  _fetch: fetchImpl,
  fetch: wrappedFetch,
  encodeDataURI,
  decodeDataURI,
  assert,
  wait,
  delayThrow,
  defer,
  defineGetter,
  isLocalHost,
  isLocalUrl
}
