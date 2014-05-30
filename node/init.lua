require 'underscore'
local async = require 'async'
local thnode = require 'thmap.node'
local protocol = require 'redis-status.protocol'
--local protocol = require '../protocol/'
local api = require '../api'

local standardconfig = {
   groupname = "RQ",
   nodename = "RQNODE",
   api = api,
}


local new = function(rediswrite, redissub, config)


   -- config overrides standard config
   config = config or standardconfig
   assert(config.nodename, "must give node a name")

   for k,v in pairs(standardconfig) do
      config[k] = config[k] or v
   end
   

   local commands = {}

   -- TODO: take killdelay, etc from redis stored config?
   local node = thnode({killdelay = 1200})


   -- set our commands to match up with thnode
   for i,comname in ipairs(protocol.standard_commands) do
      commands[comname] = node[comname]
   end

   -- if you want to add more commands, do provide a function addcommands in your config
   if config.addcommands then
      config.addcommands(commands)
   end

   -- simple handler
   local commandHandler = function(commandtype, ...)
      print("COMMAND RECEIVED", commandtype)
      commands[commandtype](...)
   end

   --TODO: figure out how to get node names
   local client = protocol.node(config.groupname, config.nodename, rediswrite, redissub)

   client.init(commandHandler)
   

   -- override the thnode's logpreprocess to intercept messages of specified type
   local processes = {}   

   node.setlogpreprocess(function(name,data)
      local status,passthrough = config.api.parseStatus(data)
      
      if status then
         processes[name].last_seen = async.hrtime()
         processes[name].last_status = status

         client.sendstatus(name, processes[name], function(res) print(res) end)
      end

      return passthrough or true

   end)

   node.onnewprocess(function(name)
      local time = async.hrtime()
      processes[name] = {
         born = time,
         last_seen = time,
         last_status = "NEW",
      }

      print("NEW WORKER", name)

      client.initworker(name, function(res) print("NEWPROC", name, res) end)

   end)

   node.ondeadprocess(function(name)
      local time = async.hrtime()
      processes[name] = {
         born = time,
         last_seen = time,
         last_status = "NEW",
      }

      print("WORKER DEAD", name)

      client.deadworker(name, function(res) print(res) end)

   end)

   local rnode = {client = client}

   function rnode.dump()
      print(processes)
   end

   rnode.thnode = node

   return rnode
end

return new
