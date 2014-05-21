require 'underscore'
local thnode = require 'thmap.node'

local standardconfig = {
   parseStatus = function(data)
         local _,_,status = data:find("STATUS:(.*)")
         return status
   end
}

local new = function(redis, config)
   
   local statusnode= {}

   -- TODO: take killdelay, etc from redis stored config?
   local node = thnode({killdelay = 1200})


   statusnode.spawn = node.spawn
   statusnode.restart = node.restart
   statusnode.killall = node.killall
   statusnode.ps = node.ps
   statusnode.git = node.git
   statusnode.update = node.update
   statusnode.zombies = node.zombies


   config = config or standardconfig
      
   node.setlogpreprocess(function(name,data)
      local status = config.parseStatus(data)
      
      processes[name].last_seen = async.hrtime()
      processes[name].last_status = status

      -- TODO:jsonify and write the status to redis
      print(name, status)
   end)

   local processes = {}
   node.onnewprocess(function(name)
      local time = async.hrtime()
      processes[name] = {
         born = time,
         last_seen = time,
         last_status = "NEW",
      }
   end)

   function statusnode.dump()
      print(processes)
   end

   return statusnode
end
