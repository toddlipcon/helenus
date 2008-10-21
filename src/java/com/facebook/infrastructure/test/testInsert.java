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
import com.facebook.infrastructure.db.RowMutation;
import com.facebook.infrastructure.db.Table;

public class testInsert
{
    static void insert(String sKey, String sColumnFamily, String sColumn,
            String sDataToInsert)
    {
        // get the table name
        String sTableName = DatabaseDescriptor.getTables().get(0);

        try
        {
            // do the insert first
            RowMutation rm = new RowMutation(sTableName, sKey);
            rm.add(sColumnFamily + ":" + sColumn, sDataToInsert.getBytes(), 0);
            rm.apply();
            System.out.println("Finished apply ...");

        }
        catch (Exception e)
        {
            // write failed - return the reason
            e.printStackTrace();
        }
    }

    static void bulkInsertSuperColumns(String columnFamily, int numKeys,
            int startCols, int endCol, int startSubCol, int endSubCol)
    {
        try
        {
            Random random = new Random();
            byte[] bytes = new byte[1];

            // insert data for the super columns
            for (int i = 1; i < numKeys + 1; ++i)
            {
                String key = new Integer(i).toString();
                RowMutation rm = new RowMutation("Mailbox", key);
                for (int j = startCols; j < endCol; ++j)
                {
                    for (int k = startSubCol; k < endSubCol; ++k)
                    {
                        random.nextBytes(bytes);
                        rm.add(columnFamily + ":" + "SuperColumn-" + j + ":"
                                + "Column-" + k, bytes, k);
                    }
                }
                rm.apply();
            }

            System.out.println("Write done ...");
            Table table = Table.open("Mailbox");
            table.flush(false);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    static void bulkInsertSimpleColumns(String columnFamily, int numKeys,
            int startCol, int endCol)
    {
        try
        {
            Random random = new Random();
            byte[] bytes = new byte[24];

            // first insert data for simple columns
            for (int i = 1; i < numKeys + 1; ++i)
            {
                String key = new Integer(i).toString();
                RowMutation rm = new RowMutation("Mailbox", key);
                for (int j = startCol; j < endCol; ++j)
                {
                    random.nextBytes(bytes);
                    rm.add(columnFamily + ":" + "Column-" + j, bytes, j);
                }
                rm.apply();
            }

            System.out.println("Write done ...");
            Table table = Table.open("Mailbox");
            table.flush(false);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
