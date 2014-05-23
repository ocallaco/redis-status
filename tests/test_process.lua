local async = require 'async'

async.setInterval(4000, function()
   io.write("STATUS: my status is ", os.time(), "\n")
   io.flush()
end)

async.go()
