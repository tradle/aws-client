const Promise = require('bluebird')
const { EventEmitter } = require('events')
const _ = require('lodash')
const {
  RESOLVED
} = require('./utils')

module.exports = function createState (initial) {
  const waiting = []
  const internal = _.clone(initial)
  const proxy = new EventEmitter()
  Object.keys(internal).forEach(prop => {
    Object.defineProperty(proxy, prop, {
      enumerable: true,
      get () {
        return internal[prop]
      },
      set (value) {
        internal[prop] = value
        proxy.emit('change')
        proxy.emit(prop)
      }
    })
  })

  const stateMatches = state => {
    return Object.keys(state).every(prop => proxy[prop] === state[prop])
  }

  proxy.await = state => {
    return new Promise(resolve => {
      if (stateMatches(state)) {
        return resolve()
      }

      waiting.unshift({ state, resolve })
    })
  }

  proxy.on('change', function () {
    let i = waiting.length
    while (i--) {
      let { state, resolve } = waiting[i]
      if (stateMatches(state)) {
        // remove
        waiting.pop()
        resolve()
      }
    }
  })

  return proxy
}
