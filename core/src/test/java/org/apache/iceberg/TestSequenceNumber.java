/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import org.junit.Assert;
import org.junit.Test;


public class TestSequenceNumber extends TableTestBase {

  @Test
  public void testWriteSequenceNumber() {
    table.newFastAppend().appendFile(FILE_A).commit();

    Assert.assertEquals("sequence number should be 1", 1,
        table.currentSnapshot().sequenceNumber().longValue());

    table.newFastAppend().appendFile(FILE_B).commit();

    Assert.assertEquals("sequence number should be 2", 2,
        table.currentSnapshot().sequenceNumber().longValue());

  }

  @Test
  public void testReadSequenceNumber() {
    long curSeqNum;

    if (table.currentSnapshot() == null) {
      curSeqNum = 0;
    } else {
      curSeqNum = table.currentSnapshot().sequenceNumber();
    }
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    Assert.assertEquals(curSeqNum + 2,
        TestTables.load(tableDir, "test")
            .currentSnapshot()
            .sequenceNumber()
            .longValue());
  }
}
