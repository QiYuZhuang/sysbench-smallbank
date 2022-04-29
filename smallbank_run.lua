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


function balance()

-- prep work

	local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
	local cust_id = sysbench.rand.zipfian(1, MAXITEMS)
	
	con:query("BEGIN")

	local id, name = con:query(([[execute balance1_%d(%d)]]):format(table_num, cust_id))

	if id == cust_id then
		local saving = con:query(([[execute balance2_%d(%d)]]):format(table_num, cust_id))
		local checking = con:query(([[execute balance3_%d(%d)]]):format(table_num, cust_id))
		-- local sum = checking + saving
	end

	con:query("COMMIT")

end

function depositChecking()
-- prep work

	local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
	local cust_id = sysbench.rand.zipfian(1, MAXITEMS)
	local deposit = random_float(0, 1000000)
	-- print(cust_id)
  con:query("BEGIN")
	con:query(([[execute depositChecking1_%d(%d)]]):format(table_num, cust_id))
	con:query(([[execute depositChecking2_%d(%d, %d)]]):format(table_num, cust_id, deposit))
  con:query("COMMIT")

end

function transactSaving()
	local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
	local cust_id = sysbench.rand.zipfian(1, MAXITEMS)
	local deposit = random_float(0, 1000000)

	if random_float(0, 1) < 0.2 then
		deposit = -1 * deposit
	end

	con:query("BEGIN")
	local id, name = con:query(([[execute transactSaving1_%d(%d)]]):format(table_num, cust_id))
	if id == cust_id then 
		local s_bal = con:query_row(([[execute transactSaving2_%d(%d)]]):format(table_num, cust_id))
		s_bal = tonumber(s_bal)
		if s_bal + deposit < 0 then 
			con:query("ROLLBACK")
		else 
			con:query(([[execute transactSaving3_%d(%d, %d)]]):format(table_num, cust_id, s_bal+deposit))
			con:query("COMMIT")
		end
	else
		con:query("ROLLBACK")
	end
end

function amalgamate()
	local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
	local cust_id1 = sysbench.rand.zipfian(1, MAXITEMS)
	local cust_id2 = sysbench.rand.zipfian(1, MAXITEMS)

	while cust_id1 == cust_id2 do
		cust_id2 = sysbench.rand.zipfian(1, MAXITEMS)
	end

	con:query("BEGIN")
	con:query(([[execute amalgamate1_%d(%d)]]):format(table_num, cust_id1))
	local s_bal = con:query_row(([[execute amalgamate2_%d(%d)]]):format(table_num, cust_id1))
	local c_bal = con:query_row(([[execute amalgamate3_%d(%d)]]):format(table_num, cust_id1))
	s_bal = tonumber(s_bal)
	c_bal = tonumber(c_bal)
	con:query(([[execute amalgamate4_%d(%d)]]):format(table_num, cust_id1))
	con:query(([[execute amalgamate5_%d(%d)]]):format(table_num, cust_id1))
	con:query(([[execute amalgamate6_%d(%d, %d)]]):format(table_num, cust_id1, s_bal+c_bal))
  con:query("COMMIT")
end

function writeCheck()
	local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
	local cust_id1 = sysbench.rand.zipfian(1, MAXITEMS)
	local deposit = random_float(0, 1000000)

	con:query("BEGIN")
	con:query(([[execute writeCheck1_%d(%d)]]):format(table_num, cust_id1))
	local s_bal = con:query_row(([[execute writeCheck2_%d(%d)]]):format(table_num, cust_id1))
	local c_bal = con:query_row(([[execute writeCheck3_%d(%d)]]):format(table_num, cust_id1))
	s_bal = tonumber(s_bal)
	c_bal = tonumber(c_bal)
	if s_bal + c_bal < deposit then
		con:query(([[execute writeCheck4_%d(%d, %d)]]):format(table_num, cust_id1, c_bal-deposit))
	else
		con:query(([[execute writeCheck4_%d(%d, %d)]]):format(table_num, cust_id1, c_bal-deposit-1))
	end
	
  con:query("COMMIT")
end
-- vim:ts=4 ss=4 sw=4 expandtab
