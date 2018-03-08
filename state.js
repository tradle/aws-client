const Promise = require('bluebird')
const { EventEmitter } = require('events')
const _ = require('lodash')
const {
  RESOLVED
} = require('./utils')

module.exports = function createState (initial) {
  const waiting = []
  const internal = _.clone(initial)
  const myState = new EventEmitter()

  myState.is = state => {
    return Object.keys(state).every(prop => myState[prop] === state[prop])
  }

  myState.await = state => {
    return new Promise(resolve => {
      if (myState.is(state)) {
        return resolve()
      }

      waiting.unshift({ state, resolve })
    })
  }

  myState.on('change', function () {
    let i = waiting.length
    while (i--) {
      let { state, resolve } = waiting[i]
      if (myState.is(state)) {
        // remove
        waiting.pop()
        resolve()
      }
    }
  })

  Object.keys(internal).forEach(prop => {
    if (myState[prop]) {
      throw new Error(`"${prop}" is a reserved key`)
    }

    Object.defineProperty(myState, prop, {
      enumerable: true,
      get () {
        return internal[prop]
      },
      set (value) {
        internal[prop] = value
        myState.emit('change')
        myState.emit(prop)
      }
    })
  })

  return myState
}
