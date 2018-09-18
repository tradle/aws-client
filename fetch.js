/* global self */

const obj = typeof global === 'undefined' ? self : global

module.exports = obj.fetch.bind(obj)
