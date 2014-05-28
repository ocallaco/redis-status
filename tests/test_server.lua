local rs = require '../server'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

local curses = require 'ncurses'

redis_details = {host='localhost', port=6379}

local WIDTH = 40
local HEIGHT = 10
local ROWS = 6

local opt = lapp([[
thnode: a Torch compute node
   -p,--print dont use ncurses
]])

local function create_newwin(height, width, starty, startx)
   local local_win;

	local_win = curses.newwin(height, width, starty, startx)
	curses.box(local_win, 0 , 0)		
   curses.wrefresh(local_win)
	return local_win;
end



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
   local win = node_area[nodename]
   local nodeEntry = nodes[nodename]

   curses.mvwprintw(win, 1, 1, nodename)
   curses.mvwprintw(win, 2, 1, "Number of workers: " .. #nodeEntry.worker_names)
   curses.mvwprintw(win, 3, 1, "Last Seen: " .. nodeEntry.last_seen)

   for i,workername in ipairs(nodeEntry.worker_names) do
      curses.mvwprintw(win, 3 + i, 1, "Worker: " .. workername)   
   end

   curses.box(win, 0 , 0)	
   curses.wrefresh(win)
   curses.refresh()


end

local onNewNode = function(name)
   table.insert(node_names,name)
   nodes[name] = {workers = {}, last_seen = os.time(), worker_names = {}}
   
   local numnodes = #node_names - 1
   local startx = math.floor(numnodes / ROWS) * WIDTH
   local starty = math.floor(numnodes * HEIGHT) % (ROWS * HEIGHT)
   node_area[name] = create_newwin(HEIGHT, WIDTH, starty, startx)
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

   server.issueCommand({"CONTROLCHANNEL:RQ:t1"}, "spawn_test", function(res) print("sent") end)
   server.issueCommand({"CONTROLCHANNEL:RQ:t2"}, "spawn_test", function(res) print("sent") end)

end)


async.go()
