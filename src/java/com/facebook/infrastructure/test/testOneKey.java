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

import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.db.Table;

public class testOneKey {
  static void testCompactions() throws Exception {
    testInsert.bulkInsertSuperColumns("MailboxThreadList0Indexed", 1, 0,
        64 * 1024, 0, 1);
    testInsert.bulkInsertSuperColumns("MailboxThreadList0Indexed", 1,
        64 * 1024, 128 * 1024, 0, 1);

    String sTableName = DatabaseDescriptor.getTables().get(0);
    Table.open(sTableName).forceCompaction();

    testRead.readSuperCols("MailboxThreadList0Indexed", "1", 128 * 1024, 1);
  }

  static void testBulkInsertAndRead() {
    testInsert.bulkInsertSimpleColumns("MailboxUserList", 1, 0, 32 * 1024);
    testInsert.bulkInsertSimpleColumns("MailboxUserListIndexed", 1, 0,
        32 * 1024);
    testInsert.bulkInsertSuperColumns("MailboxThreadList0", 1, 0, 16 * 1024, 0,
        2);
    testInsert.bulkInsertSuperColumns("MailboxThreadList0Indexed", 1, 0,
        16 * 1024, 0, 2);

    System.out.println("Reading data...");
    testRead.readSimpleCols("MailboxUserListIndexed", "1", 32 * 1024);
    testRead.readSuperCols("MailboxThreadList0Indexed", "1", 16 * 1024, 2);
  }
}
