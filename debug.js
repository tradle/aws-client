const { name, version } = require('./package.json')
module.exports = require('debug')(`${name}@${version}`)
