local json = require 'cjson'

local STATUS = "WORKERSTATUS:" -- hash workername to status json
local STATUSCHANNEL = "STATUSCHANNEL:"  -- channel for updates to worker status
local COMMANDCHANNEL = "COMMANDCHANNEL:" -- channel for commands to nodes

local function newnode(rediswrite, redissub, groupname, commands, cb)

   redissub.subscribe(COMMANDCHANNEL .. groupname, function(data)
      if data[1] == "message" then
         local ok,cmddata = pcall(json.decode, data[3])
         if ok then
            xpcall(function()
               if cmddata.unpackargs then
                  commands[cmddata.command](unpack(cmddata.commandargs))
               else
                  commands[cmddata.command](cmddata.commandargs)
               end
            end,
            function(er)
               local err = debug.traceback(er)
               print("ERROR ON COMMAND", cmddata.command, err)
            end)
         else
            print("failed to decode command", data[3])
         end
      end
   end)


   local statusnode = {}

   function statusnode.sendstatus(workername, status)
      local status_table = json.encode(status)

      rediswrite.hset(STATUS .. groupname, workername, status_table)
      rediswrite.publish(STATUSCHANNEL .. groupname, workername)
   end

   return statusnode
end

local function newserver(rediswrite, redissub, groupname, statuscallback)

   assert(groupname, "must provide a groupname when creating new statusserver")
   assert(type(statuscallback) == "function", "must provide status callback function when creating new statusserver")

   redissub.subscribe(STATUSCHANNEL .. groupname, function(data)
      if data[1] == "message" then
         local workername = data[3]
         rediswrite.hget(STATUS .. groupname, workername, function(res)
            local ok,status = pcall(json.decode, res)
            if ok then
               statuscallback(status)
            else
               print("failed to decode status", workername, res)
            end
         end)
      end
   end)
   
   local statusserver = {
      issuecommand = function(nodegroup, command, ...)
         cmdtable = {nodegroup = nodegroup, command = command}
         if #arg > 1 then
            cmdtable.unpackargs = true
            cmdtable.args = arg
         else
            cmdtable.args = arg[1]
         end
   
         local cmdjson = json.encode(cmdtable)
         rediswrite.publish(COMMANDCHANNEL .. groupname, cmdjson)
      end
   }


   return statusserver
end

return {
   newnode = newnode,
   newserver = newserver
}
