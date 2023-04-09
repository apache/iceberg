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
package org.apache.iceberg.flink.source.reader;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

public class TestArrayBatchRecords {

  @Test
  public void testFullRange() {
    String[] elements = new String[] {"0", "1", "2", "3"};
    testArray(elements, elements.length, 2, 119);
  }

  @Test
  public void testSubRange() {
    String[] elements = new String[] {"0", "1", "2", "3"};
    testArray(elements, 2, 0, 0);
  }

  private void testArray(
      String[] elements, int numberOfRecords, int fileOffset, long startingRecordOffset) {
    String splitId = "iceberg_split_1";
    AtomicBoolean recycled = new AtomicBoolean();

    ArrayBatchRecords<String> recordsWithSplitIds =
        ArrayBatchRecords.forRecords(
            splitId,
            ignored -> recycled.set(true),
            elements,
            numberOfRecords,
            fileOffset,
            startingRecordOffset);

    Assert.assertEquals(splitId, recordsWithSplitIds.nextSplit());

    for (int i = 0; i < numberOfRecords; i++) {
      RecordAndPosition<String> recAndPos = recordsWithSplitIds.nextRecordFromSplit();
      Assert.assertEquals(elements[i], recAndPos.record());
      Assert.assertEquals(fileOffset, recAndPos.fileOffset());
      // recordOffset points to the position after this one
      Assert.assertEquals(startingRecordOffset + i + 1, recAndPos.recordOffset());
    }

    Assert.assertNull(recordsWithSplitIds.nextRecordFromSplit());
    Assert.assertNull(recordsWithSplitIds.nextSplit());
    recordsWithSplitIds.recycle();
    Assert.assertTrue(recycled.get());
  }
}
