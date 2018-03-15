
const ex = require('error-ex')

module.exports = {
  CloseTimeout: ex('CloseTimeout'),
  SendTimeout: ex('SendTimeout'),
  CatchUpTimeout: ex('CatchUpTimeout'),
  ConnectTimeout: ex('ConnectTimeout'),
  UploadEmbed: ex('UploadEmbed'),
  AuthFailed: ex('AuthFailed')
}
