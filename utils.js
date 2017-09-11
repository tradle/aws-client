const crypto = require('crypto')
const extend = require('xtend/mutable')
const fetch = require('isomorphic-fetch')
const clone = require('clone')
const stringify = JSON.stringify.bind(JSON)
const co = Promise.coroutine || require('co').wrap
const promisify = require('pify')
const parseDataURI = require('strong-data-uri').decode
const traverse = require('traverse')
const { AwsSigner } = require('aws-sign-web')
const { serializeMessage } = require('@tradle/engine').utils

const post = co(function* (url, data) {
  const res = yield fetch(url, {
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
  const res = yield fetch(url, {
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

function replaceDataUrls ({ object, bucket, keyPrefix }) {
  return traverse(object).reduce(function (replacements, value) {
    if (this.isLeaf && typeof value === 'string') {
      if (!value.startsWith('data:')) {
        return replacements
      }

      let body
      try {
        body = parseDataURI(value)
      } catch (err) {
        // not a data uri
        return replacements
      }

      const hash = sha256(body)
      const key = keyPrefix + hash
      const host = `${bucket}.s3.amazonaws.com`
      const s3Url = `https://${host}/${key}`
      replacements.push({
        dataUrl: value,
        hash,
        body,
        host,
        mimetype: body.mimetype,
        path: this.path.join('.'),
        s3Url,
        bucket,
        key
      })

      this.update(s3Url)
    }

    return replacements
  }, [])
}

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
      "x-amz-security-token": credentials.sessionToken,
    },
    body
  }

  request.headers = signer.sign(request)
  const res = yield utils.fetch(request.url, request)
  const text = yield res.text()
  if (res.status > 300) {
    const err = new Error(text)
    err.response = res
    throw err
  }

  return res
})

const utils = module.exports = {
  extend,
  clone,
  traverse,
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
  sha256,
  serializeMessage,
  parseDataURI,
  uploadToS3,
  parsePrefix,
  fetch
}
