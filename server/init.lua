require 'underscore'

--local protocol = require 'redis-status.protocol'
local protocol = require '../protocol/'

local new = function(rediswrite, redissub, groupname, cb)

   local server = protocol.newserver(rediswrite, redissub, groupname, cb)
   
   return server 
end

return new
