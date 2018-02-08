const createState = require('./state')
const {
  defineGetter
} = require('./utils')

module.exports = function createConnectionState () {
  const state = createState({
    resetting: false,
    authenticated: false,
    connected: false,
    subscribed: false,
    caughtUp: false
  })

  defineGetter(state, 'canSend', () => state.caughtUp && state.canPublish)
  defineGetter(state, 'canPublish', () => state.connected)
  return state
}
