local _ = require 'underscore'

--local protocol = require 'redis-status.protocol'
local protocol = require '../protocol/'

--callbackTable: onNodeReady, onNodeBlock, onNodeError, onNodeDead, 
--               onWorkerReady, onWorkerBlock, onWorkerError, onWorkerDead, 
--               onStatus, onAck

local NODE_READY = "READY"
local NODE_BLOCKED = "BLOCKED"
local NODE_ERROR = "ERROR"
local NODE_DEAD = nil -- for now just remove dead

local function buildServerEnv(callbackTable)
   local cbt = {}
   cbt.nodeState = {}
   cbt.nodeWorkers = {}
   cbt.standingErrors = {}

   local addError = function(...)
      local errmsg = table.concat(_.map({...}, function(t) return tostring(t) .. ", " end))
      table.insert(cbt.standingErrors, errmsg)
      print("Error:", errmsg)
   end
   
   cbt.onNodeReady = function(nodename)

      cbt.nodeWorkers[nodename] = {}
      cbt.nodeState[nodename] = NODE_READY

      if callbackTable.onNodeReady then
         callbackTable.onNodeReady(nodename)
      end
   end

   cbt.onNodeBlock = function(nodename)
      cbt.nodeState[nodename] = NODE_BLOCK

      if callbackTable.onNodeBlock then
         callbackTable.onNodeBlock(nodename)
      end
   end

   cbt.onNodeError = function(nodename, errormsg)
      cbt.nodeState[nodename] = NODE_ERROR

      addError("Error from worker", nodename, errormsg)

      if callbackTable.onNodeError then
         callbackTable.onNodeError(nodename)
      end
   end

   cbt.onNodeDead = function(nodename)
      cbt.nodeState[nodename] = NODE_DEAD
      cbt.nodeWorkers[nodename] = nil

      if callbackTable.onNodeDead then
         callbackTable.onNodeDead(nodename)
      end
   end

   -- node must be in ready state to be spawning workers.  TODO: review the logic of this assumption
   cbt.onWorkerReady = function(nodename, workername)
      if cbt.nodeState[nodename] ~= NODE_READY then
         addError("Worker added to dead/nonexistent node", nodename, workername)
         return
      end

      cbt.nodeWorkers[nodename][workername] = {}

      if callbackTable.onWorkerReady then
         callbackTable.onWorkerReady(nodename, workername)
      end

   end

   --not sure this ever happens yet, so until it does, no need to really implement
   cbt.onWorkerBlock = function(nodename, workername)   
      if callbackTable.onWorkerBlock then
         callbackTable.onWorkerBlock(nodename, workername)
      end 
   end

   -- just log the error i guess...
   cbt.onWorkerError  = function(nodename, workername, errormsg)   
      addError("Error from worker", nodename, workername, errormsg)

      if callbackTable.onWorkerError then
         callbackTable.onWorkerError(nodename, workername, errormsg)
      end 
   end

   cbt.onWorkerDead = function(nodename, workername)   
      if cbt.nodeState[nodename] ~= NODE_READY then
         addError("Worker removed from dead/nonexistent node", nodename, workername)
         return
      end

      cbt.nodeWorkers[nodename][workername] = nil

      if callbackTable.onWorkerBlock then
         callbackTable.onWorkerBlock(nodename, workername)
      end 
   end

   cbt.onStatus = function(nodename, workername, status)
      if cbt.nodeState[nodename] ~= NODE_READY then
         addError("Worker status message on dead/nonexistent node", nodename, workername)
         return
      elseif cbt.nodeWorkers[nodename][workername] == nil then
         addError("Worker status message on dead/nonexistent worker", nodename, workername)
         return
      elseif status == nil then
         addError("Bad worker status received", nodename, workername)
         return
      end


      cbt.nodeWorkers[nodename][workername] = status

      if callbackTable.onStatus then
         callbackTable.onStatus(nodename, workername, status)
      end 
   end
   cbt.onAck = function(message)
      if callbackTable.onAck then
         callbackTable.onAck(message)
      end 
   end

   return cbt
end

local new = function(rediswrite, redissub, clustername, callbackTable)

   local server = protocol.server(clustername, rediswrite, redissub)

   server.init( buildServerEnv(callbackTable) )

   return server 
end

return new
