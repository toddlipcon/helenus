/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.infrastructure.test;

import java.util.Random;

import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.db.ColumnFamily;
import com.facebook.infrastructure.db.IColumn;
import com.facebook.infrastructure.db.Table;

public class testRead
{
	static long read(String sKey, String sColumnFamily, String sColumn) throws Exception
	{
        long timeDelta = 0;

        // get the table name
    	String sTableName = DatabaseDescriptor.getTables().get(0);
    	long startTime, endTime;

        try
        {
            startTime = System.currentTimeMillis();

	    	Table table = Table.open(sTableName);
	        ColumnFamily cf = table.get(sKey, sColumnFamily + ":" + sColumn);
	        IColumn column = cf.getColumn(sColumn);

	        endTime = System.currentTimeMillis();
	        timeDelta = endTime - startTime;

	        // System.out.println("Read: TIME = " + timeDelta + "ms, key = " + sKey + ", column family = " + sColumnFamily + ", column = " + sColumn + ", data = " + new String(column.value()));
        }
        catch (Exception e)
        {
        	e.printStackTrace();
        	throw e;
        }

        return timeDelta;
	}

	static void readSimpleCols(String columnFamily, String key, int numCols)
	{
        Random random = new Random();
        int numRuns = 1000;
        String colName;
        long readTimeSum;

        // Read the un-indexed simple column
        readTimeSum = 0;
        for(int i = 0; i < numRuns; ++i)
        {
        	colName = "Column-" + random.nextInt(numCols);
        	try
        	{
        		readTimeSum += read(key, columnFamily, colName);
	    	}
	    	catch (Exception e)
	    	{
	    		e.printStackTrace();
	    	}
        }
        System.out.println("Average read for " + columnFamily + ": " + (readTimeSum/numRuns));
	}

	static void readEntireSuperCols(String columnFamily, String key, int numCols)
	{
        Random random = new Random();
        int numRuns = 1000;
        String colName;
        long readTimeSum;

        // read the un-indexed super columns
        readTimeSum = 0;
        for(int i = 0; i < numRuns; ++i)
        {
        	colName = "SuperColumn-" + random.nextInt(numCols);
        	try
        	{
        		readTimeSum += read(key, columnFamily, colName);
        	}
        	catch (Exception e)
        	{
        		e.printStackTrace();
        	}
        }
        System.out.println("Average read for " + columnFamily + ": " + (readTimeSum/numRuns));
	}

	static void readSuperCols(String columnFamily, String key, int numCols, int numSubCols)
	{
        Random random = new Random();
        int numRuns = 1000;
        String colName;
        long readTimeSum;

        // read the un-indexed super columns
        readTimeSum = 0;
        for(int i = 0; i < numRuns; ++i)
        {
        	colName = "SuperColumn-" + random.nextInt(numCols) + ":" + "Column-" + random.nextInt(numSubCols);
        	try
        	{
        		readTimeSum += read(key, columnFamily, colName);
        	}
        	catch (Exception e)
        	{
        		e.printStackTrace();
        	}
        }
        System.out.println("Average read for " + columnFamily + ": " + (readTimeSum/numRuns));
	}
}
