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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

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

    assertThat(splitId).isEqualTo(recordsWithSplitIds.nextSplit());

    for (int i = 0; i < numberOfRecords; i++) {
      RecordAndPosition<String> recAndPos = recordsWithSplitIds.nextRecordFromSplit();
      assertThat(elements[i]).isEqualTo(recAndPos.record());
      assertThat(fileOffset).isEqualTo(recAndPos.fileOffset());
      assertThat(startingRecordOffset + i + 1).isEqualTo(recAndPos.recordOffset());
    }

    assertThat(recordsWithSplitIds.nextRecordFromSplit()).isNull();
    assertThat(recordsWithSplitIds.nextSplit()).isNull();
    recordsWithSplitIds.recycle();
    assertThat(recycled.get()).isTrue();
  }
}
