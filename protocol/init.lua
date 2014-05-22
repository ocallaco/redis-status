local json = require 'cjson'

local CLUSTERNODES = "CLUSTERNODES" -- (.. clustername), set nodes
local NODESTATUS = "NODESTATUS:" -- (.. clustername .. ":" .. nodename), hash containing info about node
local NODEWORKERS = "NODEWORKERS:" -- (.. clustername .. ":" .. nodename), set containing worker names
local WORKERSTATUS = "WORKERSTATUS:" -- (.. clustername .. ":" .. nodename),  hash workername to status json
local INFOCHANNEL = "INFOCHANNEL:" -- (.. clustername), channel for updates from node to server
local CONTROLCHANNEL = "CONTROLCHANNEL:" -- (.. clustername .. ":" .. nodename) channel for commands from server to node

-- functions for building KEYS

local function clusternodes(clustername)
   return CLUSTERNODES .. clustername
end
local function nodestatus(clustername, nodename)
   return NODESTATUS .. clustername .. ":" .. nodename
end

local function nodeworkers(clustername, nodename)
   return NODEWORKERS .. clustername .. ":" .. nodename
end

local function workerstatus(clustername, nodename)
   return WORKERSTATUS .. clustername .. ":" .. nodename
end

local function infochannel(clustername)
   return INFOCHANNEL .. clustername
end

local function controlchannel(clustername, nodename)
   return CONTROLCHANNEL .. clustername .. ":" .. nodename
end

-- Message Types
   -- ALL CHANNELS
local TYPE_ACK = "ACK"

   -- CONTROL ONLY
local TYPE_COMAND = "COMMAND"

   -- INFO ONLY
local TYPE_STATUS = "STATUS"
local TYPE_NODE = "NODE"
local TYPE_WORKER = "WORKER" -- different from worker status, which indicates job progress, etc.  this is at the process level


-- NODE/WORKER STATES
local STATE_READY = "READY"
local STATE_ERROR = "ERROR"
local STATE_BLOCK = "BLOCK"
local STATE_DEAD = "DEAD"




--------------------------------------------------------------------------------
-- pack messages
--------------------------------------------------------------------------------

local function pack_message(messageType, messageData)
   local msg = {msgtype = messageType, data = messageData}
   return json.encode(msg)
end

local function status_message(nodename, workername, status)
   return pack_message(TYPE_STATUS, {worker=workername, node=nodename, status=status})
end

local function command_message(commandName, ...)
   local arg = {...}
   if #arg > 1 then
      return pack_message(TYPE_COMMAND, {name=commandName, args = arg, unpack = true})
   else
      return pack_message(TYPE_COMMAND, {name=commandName, args = arg[1], unpack = false})
   end
end

local function node_message(nodename, nodestate)
   return pack_message(TYPE_NODE, {name=nodename, state=nodestate})
end

local function worker_message(nodename, workername, workerstate)
   return pack_message(TYPE_WORKER, {nodename=nodename, workername=workername, state=workerstate})
end

--------------------------------------------------------------------------------



--------------------------------------------------------------------------------
-- evals
--------------------------------------------------------------------------------
local evals = {}


-- NODE EVALS:
--
-- remove node from clusternodes set.  clear all node vars.  publish "not ready" message
evals.nodeinitStart = function(clustername, nodename, cb)
   local cmdstr = [[

      local notreadymsg = ARGV[1]
      local nodename = ARGV[2]

      local nodestatus = KEYS[1]
      local nodeworkers = KEYS[2]
      local infochannel = KEYS[3]
      local clusternodes = KEYS[4]

      redis.call('publish', infochannel, notreadymsg) 
      redis.call('del', nodestatus)
      redis.call('del', nodeworkers)
      redis.call('srem', clusternodes, nodename)
   ]]

   return cmdstr, 4, nodestatus(clustername, nodename), nodeworkers(clustername, nodename), infochannel(clustername), clusternodes(clustername),
                     node_message(nodename, STATE_BLOCK), nodename, cb
end

-- add node to clusternodes set.  publish ready message
evals.nodeinitFinish = function(clustername, nodename, cb)
   local cmdstr = [[

      local readymsg = ARGV[1]
      local nodename = ARGV[2]

      local clusternodes = KEYS[1]
      local infochannel = KEYS[2]


      redis.call('sadd', clusternodes, nodename)
      redis.call('publish', infochannel, readymsg) 
   ]]

   return cmdstr, 2, clusternodes(clustername), infochannel(clustername), node_message(nodename, STATE_READY), nodename, cb
end

-- add worker to nodeworkers.  clear worker status. publish new worker message
evals.nodenewWorker = function(clustername, nodename, workername, cb)
   local cmdstr = [[
      
      local readymsg = ARGV[1]
      local workername = ARGV[2]

      local nodeworkers = KEYS[1]
      local workerstatus = KEYS[2]
      local infochannel = KEYS[3]

      redis.call('sadd', nodeworkers, workername)
      redis.call('hdel', workerstatus, workername)

      redis.call('publish', infochannel, readymsg) 
   ]]

   return cmdstr, 3, nodeworkers(clustername, nodename), workerstatus(clustername, nodename), infochannel(clustername), 
                     worker_message(nodename, workername, STATE_READY), workername, cb
end

-- remove worker from nodeworkers.  clear worker status.  publish dead worker message
evals.nodedeadWorker = function(clustername, nodename, workername, cb)
   local cmdstr = [[

      local deadmsg = ARGV[1]
      local workername = ARGV[2]

      local nodeworkers = KEYS[1]
      local workerstatus = KEYS[2]
      local infochannel = KEYS[3]

      redis.call('srem', nodeworkers, workername)
      redis.call('hdel', workerstatus, workername)

      redis.call('publish', infochannel, deadmsg) 
   ]]

   return cmdstr, 3, nodeworkers(clustername, nodename), workerstatus(clustername, nodename), infochannel(clustername), 
                     worker_message(nodename, workername, STATE_DEAD), workername, cb
end


-- set worker status in hash, publish to info channel
evals.nodeworkerStatus = function(clustername, nodename, workername, statusmsg, statusstr,cb)
   local cmdstr = [[
     
      local workername = ARGV[1]
      local statusmsg = ARGV[2]
      local statusstr = ARGV[3]

      local workerstatus = KEYS[1]
      local infochannel = KEYS[2]

      redis.call('hset', workerstatus, workername, statusstr)
      redis.call('publish', infochannel, statusmsg) 
   ]]

   return cmdstr, 2, workerstatus(clustername, nodename), infochannel(clustername), workername, statusmsg, statusstr, cb
end

-- SERVER EVALS
--

-- not implemented -- could just do a smembers on nodes, then pull each one individually -- might be preferable when lots of workers are running
-- get list of live nodes and workers per node, packed into a list
evals.rewindServer = function(clustername)
end


-- send command to specified channel(s) 
evals.issueCommand = function(message, channellist) 
   
   local cmdstr = [[
         
      local message = ARGV[1]
      
      for i=2,#ARGV do
         redis.call('publish', ARGV[i], readymsg) 
      end
   ]]

   comargs = {cmdstr, 0, message, unpack(channellist)} 
   table.insert(comargs, cb)
   return unpack(comargs)
end

--------------------------------------------------------------------------------

local newNodeClient = function(clustername, nodename, redis_rw, redis_sub)
   local node = {}

   -- be very careful not to give two nodes the same name
   function node.init()
      -- clear out node's stored info on redis and send not ready signal to server

      redis_rw.eval(evals.nodeinitStart(clustername, nodename))
      
      -- subscribe to channels
      
      redis_sub.subscribe(controlchannel(clustername, nodename), function(command)
         print(command)
      end)

      -- set new node info on redis and send ready signal to server

      redis_rw.eval(evals.nodeinitFinish(clustername, nodename))

   end

   function node.initworker(workername, cb)
      -- set worker status to INIT

      redis_rw.eval(evals.nodenewWorker(clustername, nodename, workername, cb))
      
      -- subscribe to worker specific channels ?
   end

   function node.deadworker(workername, cb)
      -- clear out worker info
      redis_rw.eval(evals.nodedeadWorker(clustername, nodename, workername, cb))
   end

   function node.sendstatus(workername, status, cb)

      local statusmsg = status_message(nodename, workername, status)
      local statusstr = json.encode(status)

      -- set stored info on redis for worker, publish update to info channel 
      redis_rw.eval(evals.nodeworkerStatus(clustername, nodename, workername, statusmsg, statusstr,cb))
   end

   return node

end

local newServerClient = function(clustername, redis_rw, redis_sub)

   local server = {}

   function server.init()
      -- subscribe to info channel, temporarily locally queue messages

      -- rewind info stored on redis

      -- unblock channel, bind callbacks, and drain queued messages

      redis_sub.subscribe(infochannel(clustername), function(message)
         print(message)
      end)
   end

   function server.issueCommand(channellist, commandName, ...)

      local cmdargs = {...}
      local cb
      if type(cmdargs[#cmdargs]) == "function" then
         cb = cmdargs[#cmdargs]
         cmdargs[#cmdargs] = nil
      end
      -- build command message.  send to specified channels
      local message,cb = command_message(commandName, unpack(cmdargs)) 

      redis_rw.eval(evals.issueCommand(message, channellist, cb))
   end

   return server
end

return {
   node = newNodeClient,
   server = newServerClient,
}
