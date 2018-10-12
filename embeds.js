const extend = require('lodash/extend')
const Embed = require('@tradle/embed')
const { memoize } = require('@tradle/promise-utils')
const { AwsSigner } = require('aws-sign-web')
const utils = require('./utils')
const resolveEmbeds = (object, concurrency=10) => Embed.resolveEmbeds({ object, resolve: download, concurrency })
const download = async ({ url }) => {
  const res = await utils.fetch(url)
  if (!res.ok || res.status > 300) {
    const text = await res.text()
    throw new Error(text)
  }

  const arrayBuffer = await res.arrayBuffer()
  const buf = new Buffer(arrayBuffer)
  buf.mimetype = res.headers.get('content-type')
  return buf
}

const uploadToS3 = async ({
  region='us-east-1',
  credentials,
  bucket,
  key,
  body,
  mimetype,
  host,
  s3Url
}) => {
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
  const res = await utils.fetch(request.url, request)
  return await utils.processResponse(res)
}

const extractAndUploadEmbeds = async (opts) => {
  const { object, region, credentials, upload=uploadToS3 } = opts
  const replacements = Embed.replaceDataUrls(opts)
  if (!replacements.length) return false

  await Promise.all(replacements.map(replacement => {
    replacement.region = region
    replacement.credentials = credentials
    return upload(replacement)
  }))

  return true
}

class EmbedsClient {
  constructor () {
    this.uploadToS3 = memoize(uploadToS3, {
      cacheKey: ({ bucket, key }) => `${bucket}/${key}`
    })

    this.resolve = resolveEmbeds
    this.extractAndUpload = opts => extractAndUploadEmbeds({ upload: this.uploadToS3, ...opts })
  }
}

module.exports = {
  createClient: opts => new EmbedsClient(opts),
  uploadToS3,
  resolveEmbeds,
  extractAndUploadEmbeds,
  replaceDataUrls: Embed.replaceDataUrls,
  encodeDataURI: Embed.encodeDataURI,
  decodeDataURI: Embed.decodeDataURI,
}
