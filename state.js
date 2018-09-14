const { EventEmitter } = require('events')
const clone = require('lodash/clone')
const cloneDeep = require('lodash/cloneDeep')
const partition = require('lodash/partition')
const {
  Promise,
  RESOLVED,
} = require('./utils')

module.exports = function createState (initial) {
  let waiting = []
  const internal = clone(initial)
  const myState = new EventEmitter()

  myState.toJSON = () => cloneDeep(internal)
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
    const [done, notYet] = partition(waiting, ({ state }) => myState.is(state))
    waiting = notYet
    done.forEach(waiter => waiter.resolve())
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
