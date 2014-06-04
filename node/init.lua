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
   replport = 10001,
   replportnext = true,
}


local startrepl = function(port)
   while true do
      local ok = pcall(function()
         async.repl.listen({host='0.0.0.0', port=port}, function(client)
            local s = client.sockname
            hostname = s.address .. ':' .. s.port
         end)
      end)
      if ok then break end
      port = port + 1
   end
   print('thnode> waiting for jobs @ localhost:' .. port)
end

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

   if config.replport then
      local port = config.replport
      while true do
         local ok = pcall(function()
            async.repl.listen({host='0.0.0.0', port=port}, function(client)
               local s = client.sockname
               hostname = s.address .. ':' .. s.port
            end)
         end)
         -- TODO: should probably notify server somehow that repl isn't available...
         if ok then
            print("REPL listening on port " .. port) 
            break
         else
            if config.replportnext then
               port = port + 1
            else 
               break
            end
         end
      end
   end


   return rnode
end

return new
