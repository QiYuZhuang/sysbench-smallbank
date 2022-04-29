-- Copyright (C) 2006-2017 Vadim Tkachenko, Percona

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- -----------------------------------------------------------------------------
-- Common code for TPCC benchmarks.
-- -----------------------------------------------------------------------------

ffi = require("ffi")

ffi.cdef[[
void sb_counter_inc(int, sb_counter_type);
typedef uint32_t useconds_t;
int usleep(useconds_t useconds);
]]

function random_float(lower, greater)
   return lower + math.random()  * (greater - lower);
end

function init()
   assert(event ~= nil,
          "this script is meant to be included by other Smallbank scripts and " ..
             "should not be called directly.")
end

if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: prepare, run, cleanup, help")
end

MAXITEMS=10000000

-- Command line options
sysbench.cmdline.options = {
   scale =
      {"Scale factor (banks)", 1},
   tables =
      {"Number of tables", 1},
   use_fk =
      {"Use foreign keys", 1},
   force_pk =
      {"Force using auto-inc PK on history table", 0},
   trx_level =
      {"Transaction isolation level (RC, RR or SER)", "RC"},
   enable_purge =
      {"Use purge transaction (yes, no)", "no"},
   report_csv =
      {"Report output in csv (yes, no)", "no"},
   mysql_storage_engine =
      {"Storage engine, if MySQL is used", "innodb"},
   mysql_table_options =
      {"Extra table options, if MySQL is used. e.g. 'COLLATE latin1_bin'", ""}
}

function sleep(n)
  os.execute("sleep " .. tonumber(n))
end

-- Create the tables and Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --scale > 1
function cmd_prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()
   local show_query="SHOW TABLES"

   if drv:name() == "mysql" then 
      con:query("SET FOREIGN_KEY_CHECKS=0")
   end

   -- create tables in parallel table per thread
   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
   sysbench.opt.threads do
     create_tables(drv, con, i)
   end

   -- make sure all tables are created before we load data

   print("Waiting on tables 30 sec\n")
   sleep(30)

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.scale,
   sysbench.opt.threads do
     load_tables(drv, con, i)
   end
end

-- Implement parallel prepare and prewarm commands
sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND}
}

function create_tables(drv, con, table_num)
   local engine_def = ""
   local extra_table_options = os.getenv("pgsql_table_options") or ""
   local extra_index_options = os.getenv("pgsql_index_options") or ""
   local query
   
   if drv:name() == "mysql" or drv:name() == "attachsql" or
      drv:name() == "drizzle"
   then
      engine_def = "/*! ENGINE = " .. sysbench.opt.mysql_storage_engine .. " */"
      extra_table_options = sysbench.opt.mysql_table_options or ""
   end

   print(string.format("Creating tables: %d\n", table_num))

   -- TABLE ACCOUNTS
   if drv:name() == "pgsql"
   then
   query = string.format([[
      CREATE TABLE IF NOT EXISTS accounts%d (
         a_custid bigint      NOT NULL,
         a_name   varchar(64) NOT NULL,
         CONSTRAINT pk_accounts%d PRIMARY KEY (a_custid)
      ) %s %s]],
      table_num, table_num, engine_def, extra_table_options)
   else
      -- HAVE NOT SUPPORT MYSQL
   end

   con:query(query)

   -- TABLE SAVING
   if drv:name() == "pgsql"
   then
   query = string.format([[
      CREATE TABLE IF NOT EXISTS saving%d (
         s_custid bigint NOT NULL,
         s_bal    float  NOT NULL,
         CONSTRAINT pk_savings%d PRIMARY KEY (s_custid),
         FOREIGN KEY (s_custid) REFERENCES accounts%d (a_custid)
      ) %s %s]],  
      table_num, table_num, table_num, engine_def, extra_table_options)
   else
      -- HAVE NOT SUPPORT MYSQL
   end

   con:query(query)

   -- CHECKING

   if drv:name() == "pgsql"
   then
   query = string.format([[
      CREATE TABLE IF NOT EXISTS checking%d (
         c_custid bigint NOT NULL,
         c_bal    float  NOT NULL,
         CONSTRAINT pk_checking%d PRIMARY KEY (c_custid),
         FOREIGN KEY (c_custid) REFERENCES accounts%d (a_custid)
      ) %s %s]],
      table_num, table_num, table_num, engine_def, extra_table_options)
   else
      -- HAVE NOT SUPPORT MYSQL
   end

   con:query(query)
end


function set_isolation_level(drv,con)
   if drv:name() == "mysql"
   then
        if sysbench.opt.trx_level == "RR" then
            isolation_level="REPEATABLE-READ"
        elseif sysbench.opt.trx_level == "RC" then
            isolation_level="READ-COMMITTED"
        elseif sysbench.opt.trx_level == "SER" then
            isolation_level="SERIALIZABLE"
        end
       
        isolation_variable=con:query_row("SHOW VARIABLES LIKE 't%_isolation'")

        con:query("SET SESSION " .. isolation_variable .. "='".. isolation_level .."'")
   end

   if drv:name() == "pgsql"
   then
        if sysbench.opt.trx_level == "RR" then
            isolation_level="REPEATABLE READ"
        elseif sysbench.opt.trx_level == "RC" then
            isolation_level="READ COMMITTED"
        elseif sysbench.opt.trx_level == "SER" then
            isolation_level="SERIALIZABLE"
        end
       
        con:query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " .. isolation_level )
   end

end



function load_tables(drv, con, bank_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query

   set_isolation_level(drv,con)

   for table_num = 1, sysbench.opt.tables do 

      print(string.format("loading tables: %d for bank: %d\n", table_num, bank_num))

      con:bulk_insert_init("INSERT INTO accounts" .. table_num .. " (a_custid, a_name) values")
      for j = 1, MAXITEMS do
         query = string.format([[(%d, '%s')]], j, sysbench.rand.string("name-@@@@@"))
         con:bulk_insert_next(query)
      end
      con:bulk_insert_done()

      con:bulk_insert_init("INSERT INTO saving" .. table_num .. " (s_custid, s_bal) values")
      for j = 1 , MAXITEMS do
         query = string.format([[(%d, %f)]], j, random_float(0, 1000000))
         con:bulk_insert_next(query)
      end
      con:bulk_insert_done()

      con:bulk_insert_init("INSERT INTO checking" .. table_num .. "(c_custid, c_bal) values")
      for j = 1 , MAXITEMS do
         query = string.format([[(%d, %f)]], j, random_float(0, 1000000))
         con:bulk_insert_next(query)
      end
      con:bulk_insert_done()
   end
end

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
   con:query("SET AUTOCOMMIT=0")
end

function thread_done()
   con:disconnect()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   if drv:name() == "mysql" then 
      con:query("SET FOREIGN_KEY_CHECKS=0")
   end

   for i = 1, sysbench.opt.tables do
      print(string.format("Dropping tables '%d'...", i))
      con:query("DROP TABLE IF EXISTS accounts" .. i )
      con:query("DROP TABLE IF EXISTS saving" .. i )
      con:query("DROP TABLE IF EXISTS checking" .. i )
   end
end

function Lastname(num)
  local n = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"}

  name =n[math.floor(num / 100) + 1] .. n[ math.floor(num / 10)%10 + 1] .. n[num%10 + 1]

  return name
end

local init_rand=1
local C_255
local C_1023
local C_8191

function NURand (A, x, y)
	local C

	if init_rand 
	then
		C_255 = sysbench.rand.uniform(0, 255)
		C_1023 = sysbench.rand.uniform(0, 1023)
		C_8191 = sysbench.rand.uniform(0, 8191)
		init_rand = 0
	end

	if A==255
	then
		C = C_255
	elseif A==1023
	then
		C = C_1023
	elseif A==8191
	then
		C = C_8191
	end

	-- return ((( sysbench.rand.uniform(0, A) | sysbench.rand.uniform(x, y)) + C) % (y-x+1)) + x;
	return ((( bit.bor(sysbench.rand.uniform(0, A), sysbench.rand.uniform(x, y))) + C) % (y-x+1)) + x;
end

-- vim:ts=4 ss=4 sw=4 expandtab
