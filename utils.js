const crypto = require('crypto')
const extend = require('xtend/mutable')
const shallowClone = require('xtend')
const fetch = require('isomorphic-fetch')
const clone = require('clone')
const stringify = JSON.stringify.bind(JSON)
const co = Promise.coroutine || require('co').wrap
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

const post = co(function* (url, data) {
  const res = yield utils.fetch(url, {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: stringify(data)
  })

  return processResponse(res)
})

const put = co(function* (url, data) {
  const res = yield utils.fetch(url, {
    method: 'PUT',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
      // 'Content-Type': 'application/octet-stream'
    },
    body: JSON.stringify(data.toString('base64'))
  })

  return processResponse(res)
})

const processResponse = co(function* (res) {
  if (res.status > 300) {
    throw new Error(res.statusText)
  }

  return res.json()
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

const resolveS3Urls = object => resolveEmbeds({ object, resolve: download })

const utils = module.exports = {
  extend,
  shallowClone,
  clone,
  co,
  promisify,
  post,
  put,
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
  fetch,
  encodeDataURI,
  decodeDataURI
}
