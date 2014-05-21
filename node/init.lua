require 'underscore'
local async = require 'async'
local thnode = require 'thmap.node'
--local protocol = require 'redis-status.protocol'
local protocol = require '../protocol/'

local standardconfig = {
   groupname = "RQ",
   parseStatus = function(data)
         local _,_,status = data:find("^STATUS:%s*(.*)")
         return status
   end
}

local new = function(rediswrite, redissub, config, cb)
   
   config = config or standardconfig

   
   local commands = {}

   -- TODO: take killdelay, etc from redis stored config?
   local node = thnode({killdelay = 1200})


   commands.spawn = node.spawn
   commands.restart = node.restart
   commands.killall = node.killall
   commands.ps = node.ps
   commands.git = node.git
   commands.update = node.update
   commands.zombies = node.zombies

   -- if you want to add more commands, do provide a function addcommands in your config
   if config.addcommands then
      config.addcommands(commands)
   end

   local client = protocol.newnode(rediswrite, redissub, config.groupname, commands, cb)
   
   local processes = {}
      
   node.setlogpreprocess(function(name,data)
      local status = config.parseStatus(data)
      
      if status then
         processes[name].last_seen = async.hrtime()
         processes[name].last_status = status

         client.sendstatus(name, processes[name])
      end

      return true

   end)

   node.onnewprocess(function(name)
      local time = async.hrtime()
      processes[name] = {
         born = time,
         last_seen = time,
         last_status = "NEW",
      }

      --TODO: send some new process info to server
   end)

   local rnode = {client = client}

   function rnode.dump()
      print(processes)
   end

   rnode.thnode = node

   return rnode
end

return new
