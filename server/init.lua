require 'underscore'

--local protocol = require 'redis-status.protocol'
local protocol = require '../protocol/'

local new = function(rediswrite, redissub, clustername, cb)

   local server = protocol.server(clustername, rediswrite, redissub)

   server.init()

   return server 
end

return new
