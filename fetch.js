const obj = typeof global !== 'undefined' ? global : self

module.exports = obj.fetch.bind(obj)
