const parseURL = require('url').parse
const IP = require('ip')
const { AssertionError } = require('assert')
const crypto = require('crypto')
const omit = require('lodash/omit')
const extend = require('lodash/extend')
const fetch = require('isomorphic-fetch')
const Errors = require('@tradle/errors')
const stringify = JSON.stringify.bind(JSON)
const promisify = require('pify')
const { serializeMessage } = require('@tradle/engine').utils
const { isPromise } = require('@tradle/promise-utils')

const CustomErrors = require('./errors')
const fetchImpl = require('./fetch')
const debug = require('./debug')

const RESOLVED = Promise.resolve()

const redirectTypeErrors = err => {
  if (err instanceof TypeError) {
    throw new Error(err.message)
  }

  throw err
}

const runWithTimeout = async (fn, timeout) => {
  if (!timeout) {
    return fn()
  }

  const timeBomb = delayThrow({
    createError: () => new CustomErrors.Timeout(`${fn.name} timed out after: ${timeout}ms`),
    delay: timeout
  })

  try {
    return await Promise.race([
      timeBomb,
      fn(),
    ])
  } finally {
    timeBomb.cancel()
  }
}

const wrappedFetch = (url, opts={}) => runWithTimeout(
  () => utils._fetch(url, omit(opts, 'timeout')).catch(redirectTypeErrors),
  opts.timeout
)

const post = async ({ url, body, headers={}, timeout }) => {
  const res = await utils.fetch(url, {
    method: 'POST',
    headers: extend({
     'Accept': 'application/json',
     'Content-Type': 'application/json'
    }, headers),
    body: JSON.stringify(body),
    timeout,
  })

  return processResponse(res)
}

const processResponse = async (res) => {
  if (!res.ok || res.status > 300) {
    throw new Error(res.statusText)
  }

  let text = await res.text()
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
}

function genClientId (permalink) {
  return permalink + crypto.randomBytes(20).toString('hex')
}

function genNonce () {
  return crypto.randomBytes(32).toString('hex')
}

function prettify (obj) {
  return stringify(obj, null, 2)
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

function parseUploadPrefix (prefix) {
  prefix = prefix.replace(/^(?:https?|s3):\/\//, '')
  const idx = prefix.indexOf('/')
  const bucket = prefix.slice(0, idx)
  const keyPrefix = prefix.slice(idx + 1)
  return { bucket, keyPrefix }
}

function sha256 (strOrBuffer) {
  return crypto.createHash('sha256').update(strOrBuffer).digest('hex')
}

const assert = (statement, errMsg) => {
  if (!statement) throw new Error(errMsg || 'assertion failed')
}

const createTimeoutError = delay => new CustomErrors.Timeout(`timed out after ${delay}`)
const delayThrow = ({ delay, createError=createTimeoutError }) => {
  assert(typeof delay === 'number', 'expected number "delay"')
  if (createError) {
    assert(typeof createError === 'function', 'expected function "createError"')
  }

  let cancel
  const promise = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(createError(delay))
    }, delay)

    cancel = () => {
      clearTimeout(timeout)
      resolve()
    }
  })

  promise.cancel = cancel
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

const closeAwsIotClient = async ({ client, timeout, force, log=debug }) => {
  // temporary catch
  client.handleMessage = (packet, cb) => {
    log('ignoring packet received during close', packet)
    cb(new Error('closing'))
  }

  const timeout1 = timeout * 2 / 3
  const timeout2 = force ? timeout : timeout * 1 / 3
  if (!force) {
    log('attempting polite close')
    try {
      await runWithTimeout(() => client.end(), timeout1)
      return
    } catch (err) {
      if (Errors.matches(err, CustomErrors.CloseTimeout)) {
        log(`polite close timed out after ${timeout1}ms, forcing`)
      } else {
        log('unexpected error on close', err)
      }
    }
  }

  try {
    log('forcing close')
    await runWithTimeout(() => client.end(true), timeout2)
  } catch (err2) {
    if (Errors.matches(err2, CustomErrors.CloseTimeout)) {
      log(`force close timed out after ${timeout2}ms`)
    } else {
      log('failed to force close, giving up', err2)
    }
  }
}

const utils = module.exports = {
  Promise,
  RESOLVED,
  promisify,
  post,
  genClientId,
  genNonce,
  prettify,
  stringify,
  getTip,
  sha256,
  serializeMessage,
  parseUploadPrefix,
  _fetch: fetchImpl,
  fetch: wrappedFetch,
  assert,
  wait,
  delayThrow,
  defer,
  defineGetter,
  isLocalHost,
  isLocalUrl,
  closeAwsIotClient,
  isPromise,
  processResponse,
}
