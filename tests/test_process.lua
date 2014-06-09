local statusapi = require '../api'
local async = require 'async'

local opt = lapp([[
thnode: a Torch compute node
   -t, --time (default 4) interval
]])



async.setInterval(opt.time * 1000, function()
   statusapi.writeStatus({time=os.time(), state = torch.random(10)})
end)

async.go()
