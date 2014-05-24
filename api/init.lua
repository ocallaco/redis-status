local STATUS_PREFIX = "STATUS:"
local api = {}
local json = require 'cjson'

function api.writeStatus(statusMessage)
   io.write("STATUS:",json.encode(statusMessage),"\n")
   io.flush()
end


return api
