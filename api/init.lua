local STATUS_PREFIX = "STATUS:"
local api = {}
local json = require 'cjson'

function api.writeStatus(statusMessage)
   statusMessage.time = statusMessage.time or os.time()
   io.write("STATUS:",json.encode(statusMessage),"\n")
   io.flush()
end

function api.parseStatus(statusString)
   local _,_,status = statusString:find("^STATUS:%s*(.*)")
   return status,false
end


return api
