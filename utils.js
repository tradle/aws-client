const parseURL = require('url').parse
const IP = require('ip')
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

const runWithTimeout = co(function* (fn, timeout) {
  if (!timeout) {
    return fn()
  }

  const timeBomb = delayThrow({
    createError: () => new CustomErrors.Timeout(`${fn.name} timed out after: ${timeout}ms`),
    delay: timeout
  })

  try {
    return yield Promise.race([
      timeBomb,
      fn(),
    ])
  } catch (err) {
    throw err
  } finally {
    timeBomb.cancel()
  }
})

const wrappedFetch = (url, opts={}) => runWithTimeout(
  () => utils._fetch(url, omit(opts, 'timeout')),
  opts.timeout,
).then(processResponse, redirectTypeErrors)

const post = co(function* ({ url, body, headers={}, timeout }) {
  return utils.fetch(url, {
    method: 'POST',
    headers: extend({
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    }, headers),
    body: stringify(body),
    timeout,
  })
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
    yield Promise.all(replacements.map(replacement => {
      replacement.region = region
      replacement.credentials = credentials
      return uploadToS3(replacement)
    }))

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
  return yield utils.fetch(request.url, request)
})

const download = co(function* ({ url }) {
  const res = yield utils.fetch(url)
  if (!res.ok || res.status > 300) {
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

const delayThrow = ({ createError, delay }) => {
  let cancel
  const promise = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(createError())
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

const preventConcurrency = fn => {
  let promise = RESOLVED
  return co(function* (...args) {
    try {
      yield promise
    } finally {
      return promise = fn.apply(this, args)
    }
  })
}

const closeAwsIotClient = co(function* ({ client, timeout, force, log=debug }) {
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
      yield Promise.race([
        client.end(),
        wait(timeout1).then(() => {
          throw new CustomErrors.CloseTimeout(`after ${timeout1}ms`)
        })
      ])

      return
    } catch (err) {
      if (Errors.matches(err, CustomErrors.CloseTimeout)) {
        log(`polite close timed out after ${CLOSE_TIMEOUT}ms, forcing`)
      } else {
        log('unexpected error on close', err)
      }
    }
  }

  try {
    log('forcing close')
    yield Promise.race([
      client.end(true),
      wait(timeout2).then(() => {
        throw new CustomErrors.CloseTimeout(`(forced) after ${timeout2}ms`)
      })
    ])
  } catch (err2) {
    if (Errors.matches(err2, CustomErrors.CloseTimeout)) {
      log(`force close timed out after ${timeout2}ms`)
    } else {
      log('failed to force close, giving up', err2)
    }
  }
})

const series = co(function* (fns) {
  for (const fn of fns) {
    yield fn()
  }
})

const utils = module.exports = {
  Promise,
  RESOLVED,
  co,
  promisify,
  post,
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
  isLocalUrl,
  preventConcurrency,
  closeAwsIotClient,
  series,
}
