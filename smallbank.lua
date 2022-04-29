#!/usr/bin/env sysbench

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

-- ----------------------------------------------------------------------
-- TPCC-like workload
-- ----------------------------------------------------------------------

require("smallbank_common")
require("smallbank_run")
-- require("small_check")

function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()

  set_isolation_level(drv,con) 

  if drv:name() == "mysql" then 
    con:query("SET autocommit=0")
  end


  -- prepare statement for postgresql

  if drv:name() == "pgsql" then 
    for table_num = 1, sysbench.opt.tables
    do

      con:query(([[prepare balance1_%d(int4) as SELECT a_custid, a_name FROM accounts%d WHERE a_custid = $1]]):format(table_num, table_num))
    
      con:query(([[prepare balance2_%d(int4) as SELECT s_bal FROM saving%d WHERE s_custid = $1]]):format(table_num, table_num))
      
      con:query(([[prepare balance3_%d(int4) as SELECT c_bal FROM checking%d WHERE c_custid = $1]]):format(table_num, table_num))     

      con:query(([[prepare depositChecking1_%d(int4) as SELECT a_custid, a_name FROM accounts%d WHERE a_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare depositChecking2_%d(int4, int4) as UPDATE checking%d SET c_bal=c_bal + $2 WHERE c_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare transactSaving1_%d(int4) as SELECT a_custid, a_name FROM accounts%d WHERE a_custid = $1]]):format(table_num, table_num))
      
      con:query(([[prepare transactSaving2_%d(int4) as SELECT s_bal FROM saving%d WHERE s_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare transactSaving3_%d(int4, int4) as UPDATE saving%d SET s_bal = $2 WHERE s_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare amalgamate1_%d(int4) as SELECT a_custid, a_name FROM accounts%d WHERE a_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare amalgamate2_%d(int4) as SELECT s_bal FROM saving%d WHERE s_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare amalgamate3_%d(int4) as SELECT c_bal FROM checking%d WHERE c_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare amalgamate4_%d(int4) as UPDATE saving%d SET s_bal=0 WHERE s_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare amalgamate5_%d(int4) as UPDATE checking%d SET c_bal=0 WHERE c_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare amalgamate6_%d(int4, int4) as UPDATE checking%d SET c_bal=c_bal + $2 WHERE c_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare writeCheck1_%d(int4) as SELECT a_custid, a_name FROM accounts%d WHERE a_custid = $1]]):format(table_num, table_num))

      con:query(([[prepare writeCheck2_%d(int4) as SELECT s_bal FROM saving%d WHERE s_custid = $1]]):format(table_num, table_num))
      
      con:query(([[prepare writeCheck3_%d(int4) as SELECT c_bal FROM checking%d WHERE c_custid = $1]]):format(table_num, table_num))
      
      con:query(([[prepare writeCheck4_%d(int4, int4) as UPDATE checking%d SET c_bal = $2 WHERE c_custid = $1]]):format(table_num, table_num))   

    end
  end 
end

function event()
  local max_trx =  50
  local trx_type = sysbench.rand.uniform(1,max_trx)
  if trx_type <= 5 then
    trx="balance"
  elseif trx_type <= 15 then
    trx="depositChecking"
  elseif trx_type <= 25 then
    trx="transactSaving"
  elseif trx_type <= 40 then
    trx="amalgamate"
  elseif trx_type <= 50 then
    trx="writeCheck"
  end

-- Execute transaction
   _G[trx]()

end

function sysbench.hooks.before_restart_event(err)
  con:query("ROLLBACK")
end

function sysbench.hooks.report_intermediate(stat)
-- --   print("my stat: ", val)
   if  sysbench.opt.report_csv == "yes" then
        sysbench.report_csv(stat)
   else
        sysbench.report_default(stat)
   end
end

-- vim:ts=4 ss=4 sw=4 expandtab
