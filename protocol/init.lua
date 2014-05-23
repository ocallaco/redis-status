local async = require 'async'
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
local TYPE_COMMAND = "COMMAND"

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

local function command_message(commandName, arg)
   return pack_message(TYPE_COMMAND, {name=commandName, args = arg})
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
-- TODO: also delete worker statuses?
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
         redis.call('publish', ARGV[i], message) 
      end
   ]]

   comargs = {cmdstr, 0, message, unpack(channellist)} 
   table.insert(comargs, cb)
   return unpack(comargs)
end

--------------------------------------------------------------------------------
-- helper function for doing rewind
--------------------------------------------------------------------------------

local generateRewind = function(clustername, redis_rw, redis_sub)
  
   local rewind = {}

   local nodenames = async.fiber.wait(redis_rw.smembers, {clusternodes(clustername)})

   local node_workers = {}

   for i,nodename in ipairs(nodenames) do
      -- put the new node messages into rewind
      table.insert(rewind, node_message(nodename, STATE_READY))

      node_workers[nodename] = {}

      local workers = async.fiber.wait(redis_rw.smembers, {nodeworkers(clustername, nodename)})
      local worker_status = async.fiber.wait(redis_rw.hgetall, {workerstatus(clustername, nodename)})

      local knownworkers = {}
      for j,workername in ipairs(workers) do
         table.insert(rewind, worker_message(nodename, workername, STATE_READY))
         knownworkers[workername] = true
      end
      
      for j = 1,#worker_status,2 do
         -- dont put status when node doesnt exist
         local workername = worker_status[j]
         if knownworkers[workername] then
            local ok,status = pcall(json.decode, worker_status[j+1])
            if ok then 
               table.insert(rewind, status_message(nodename, workername, status))
            end
         end
      end

   end

   return rewind
end


--------------------------------------------------------------------------------

local newNodeClient = function(clustername, nodename, redis_rw, redis_sub)
   local node = {}

   -- be very careful not to give two nodes the same name
   function node.init(commandHandler)
      -- clear out node's stored info on redis and send not ready signal to server

      redis_rw.eval(evals.nodeinitStart(clustername, nodename))
      
      -- subscribe to channels
    
      local messageHandler = {}

      messageHandler[TYPE_ACK] = function(data)
      end

      messageHandler[TYPE_COMMAND] = function(command)
         xpcall(function()
            commandHandler(command.name, unpack(command.args))
         end, 
         function(er) 
            local err = debug.traceback(er)
            print("Failed to execute command", message[3])
            print(err)
         end)
      end


      redis_sub.subscribe(controlchannel(clustername, nodename), function(message)
         if message[1] == "message" then
            local ok,msg = pcall(json.decode, message[3])
               messageHandler[msg.msgtype](msg.data)
            if not ok then
               print("Failed to decode command message", message[3])
               return
            end

                     end
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

   -- must be in a managed async fiber
   function server.init(callbackTable)

      -- subscribe to info channel, temporarily locally queue messages
      local message_queue = {}

      redis_sub.subscribe(infochannel(clustername), function(message)
         if message[1] == "message" then
            table.insert(message_queue, message[3])
         end
      end)

      -- rewind info stored on redis

      local rewind = generateRewind(clustername, redis_rw, redis_sub)

      -- set up message handler
      local messageHandler = {
      }

      messageHandler[TYPE_NODE] = function(data)
         if data.state == STATE_READY then
            callbackTable.onNodeReady(data.name)
         elseif data.state == STATE_BLOCK then
            callbackTable.onNodeBlock(data.name)
         elseif data.state == STATE_ERROR then
            callbackTable.onNodeError(data.name, data.error)
         elseif data.state == STATE_DEAD then
            callbackTable.onNodeDead(data.name)
         end
      end

      messageHandler[TYPE_WORKER] = function(data)
         if data.state == STATE_READY then
            callbackTable.onWorkerReady(data.nodename, data.workername)
         elseif data.state == STATE_BLOCK then
            callbackTable.onWorkerBlock(data.nodename, data.workername)
         elseif data.state == STATE_ERROR then
            callbackTable.onWorkerError(data.nodename, data.workername, data.error)
         elseif data.state == STATE_DEAD then
            callbackTable.onWorkerDead(data.nodename, data.workername)
         end
      end

      messageHandler[TYPE_STATUS] = function(data)
         callbackTable.onStatus(data.node, data.worker, data.status)
      end


      -- drain queued messages
      local parseAndRun = function(msg)
         local ok, msgtable = pcall(json.decode, msg)
         if ok then
            messageHandler[msgtable.msgtype](msgtable.data)
         else
            print("Failed to parse message", msg)
         end
      end

      for i,msg in ipairs(rewind) do
         parseAndRun(msg)
      end

      for i,msg in ipairs(message_queue) do
         parseAndRun(msg)
      end


      -- unblock channel
      redis_sub.subscribe(infochannel(clustername), function(message)
         if message[1] == "message" then
            parseAndRun(message[3])
         end
      end)
   end

   -- args must be the args passed to the command packed into a table
   function server.issueCommand(channellist, commandName, args, cb)

      if type(args) == "function" then
         cb = args
         args = nil
      end

      args = args or {}

      -- build command message.  send to specified channels
      local message,cb = command_message(commandName, args) 

      redis_rw.eval(evals.issueCommand(message, channellist, cb))
   end

   return server
end

return {
   node = newNodeClient,
   server = newServerClient,
}
