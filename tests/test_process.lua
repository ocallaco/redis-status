local statusapi = require '../api'
local async = require 'async'

async.setInterval(4000, function()
   statusapi.writeStatus({time=os.time(), randvalue = torch.random(10)})
end)

async.go()
