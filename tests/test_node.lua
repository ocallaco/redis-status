local rn = require '../node'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

redis_details = {host='localhost', port=6379}

local opt = lapp([[
   -n,--name (default TEST)
]])

local standardconfig = {
   groupname = "RQ",
   nodename = opt.name,
   replport = 10001,
   replportnext = true,
}


fiber(function()

   local writecli = wait(rc.connect, {redis_details})
   local subcli = wait(rc.connect, {redis_details})

   local proccount = 1

   standardconfig.addcommands = function(commands)
      commands.spawn_test = function(time)
         commands.spawn("th", {"./test_process.lua", '-t', time}, {name = "test_process" .. proccount})
         proccount = proccount + 1
      end
   end

   node = rn(writecli, subcli, standardconfig, function() print("READY") end)
end)

async.repl()

async.go()

--global_commands.spawn_test()
--global_commands.killall()
