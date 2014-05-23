local rs = require '../server'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

local curses = require 'ncurses'

redis_details = {host='localhost', port=6379}

local opt = lapp([[
thnode: a Torch compute node
   -p,--print dont use ncurses
]])



local status_callback 

if not opt.print then
   curses.initscr()

   local next_row = 1
   local screen_level = {}

   local putstatus = function(status)
      for k,v in pairs(status) do
         local row = screen_level[k]
         if not row then
            row = next_row
            screen_level[k] = next_row
            next_row = next_row + 1
         end

         curses.mvprintw(row, 1, tostring(v))
      end
      curses.refresh()
   end
   
   status_callback = function(nodename, workername, status) putstatus(status) end
else
   status_callback = function(nodename, workername, status) print("STATUS", status) end
end



fiber(function()

   local writecli = wait(rc.connect, {redis_details})
   local subcli = wait(rc.connect, {redis_details})


   local server = rs(writecli, subcli, "RQ", {onStatus = status_callback})

   server.issueCommand({"CONTROLCHANNEL:RQ:TEST"}, "spawn_test", function(res) print("sent") end)

end)


async.go()
