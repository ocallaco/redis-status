local rn = require '../node'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

redis_details = {host='localhost', port=6379}

local standardconfig = {
   groupname = "RQ",
   nodename = "TEST",
   parseStatus = function(data)
         local _,_,status = data:find("^STATUS:%s*(.*)")
         return status
   end
}



global_commands = nil

fiber(function()

   local writecli = wait(rc.connect, {redis_details})
   local subcli = wait(rc.connect, {redis_details})

   local proccount = 1

   standardconfig.addcommands = function(commands)
      commands.spawn_test = function()
         commands.spawn("th", {"./test_process.lua"}, {name = "test_process" .. proccount})
         proccount = proccount + 1
      end
      global_commands = commands
   end

   node = rn(writecli, subcli, standardconfig, function() print("READY") end)
end)

async.repl()

async.go()

--global_commands.spawn_test()
