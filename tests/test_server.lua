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


local nodes = {}
local node_names = {}

local node_area = {}

local updateNode = function(nodename, workername)
   local box = node_area[nodename]
   local nodeEntry = nodes[nodename]
   if opt.print then
      print(box)
      print(nodeEntry)
      return 
   end
   curses.mvprintw(box.startx, box.starty, nodename)
   curses.mvprintw(box.startx + 1, box.starty, "Number of workers: " .. #nodeEntry.worker_names)
   curses.mvprintw(box.startx + 2, box.starty, "Last Seen: " .. nodeEntry.last_seen)

   for i,workername in ipairs(nodeEntry.worker_names) do
      curses.mvprintw(box.startx + 2 + i, box.starty, "Worker: " .. workername .. " " .. tostring(pretty.write(nodeEntry.workers[workername].status or "")))   
   end

   curses.refresh()

end

local onNewNode = function(name)
   table.insert(node_names,name)
   nodes[name] = {workers = {}, last_seen = os.time(), worker_names = {}}
   
   local numnodes = #node_names
   local startx = (numnodes * 10) % 60
   local starty = math.floor(numnodes / 40) * 25
   node_area[name] = {startx = startx, starty = starty}
end

local onNewWorker = function(nodename, workername)
   nodes[nodename].workers[workername] = {last_seen = os.time()}
   table.insert(nodes[nodename].worker_names, workername)
end

local onStatus = function(nodename, workername, status)
   --print(nodes)
   nodes[nodename].workers[workername].status = status
   nodes[nodename].workers[workername].last_seen = os.time()
   nodes[nodename].last_seen = os.time()
   updateNode(nodename, workername)
end

fiber(function()

   local writecli = wait(rc.connect, {redis_details})
   local subcli = wait(rc.connect, {redis_details})


   local server = rs(writecli, subcli, "RQ", {onStatus = onStatus, onWorkerReady = onNewWorker, onNodeReady = onNewNode})

   server.issueCommand({"CONTROLCHANNEL:RQ:TEST"}, "spawn_test", function(res) print("sent") end)

end)


async.go()
