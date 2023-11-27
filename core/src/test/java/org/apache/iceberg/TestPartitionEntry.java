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

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionEntry {

  @Test
  public void testPartitionBuilder() {
    Types.StructType partitionType =
        Types.StructType.of(
            Types.NestedField.required(1000, "field1", Types.StringType.get()),
            Types.NestedField.required(1001, "field2", Types.IntegerType.get()));
    PartitionData partitionData = new PartitionData(partitionType);
    partitionData.set(0, "value1");
    partitionData.set(1, 42);

    PartitionEntry partition =
        PartitionEntry.builder()
            .withPartitionData(partitionData)
            .withSpecId(123)
            .withDataRecordCount(1000L)
            .withDataFileCount(5)
            .withDataFileSizeInBytes(1024L * 1024L)
            .withPosDeleteRecordCount(50L)
            .withPosDeleteFileCount(2)
            .withEqDeleteRecordCount(20L)
            .withEqDeleteFileCount(1)
            .withTotalRecordCount(3000L)
            .withLastUpdatedAt(1627900200L)
            .withLastUpdatedSnapshotId(456789L)
            .build();

    // Verify the get method
    Assertions.assertEquals(partitionData, partition.get(0));
    Assertions.assertEquals(123, partition.get(1));
    Assertions.assertEquals(1000L, partition.get(2));
    Assertions.assertEquals(5, partition.get(3));
    Assertions.assertEquals(1024L * 1024L, partition.get(4));
    Assertions.assertEquals(50L, partition.get(5));
    Assertions.assertEquals(2, partition.get(6));
    Assertions.assertEquals(20L, partition.get(7));
    Assertions.assertEquals(1, partition.get(8));
    Assertions.assertEquals(3000L, partition.get(9));
    Assertions.assertEquals(1627900200L, partition.get(10));
    Assertions.assertEquals(456789L, partition.get(11));

    // Verify the put method
    PartitionEntry newPartition = new PartitionEntry();
    int size = partition.getSchema().getFields().size();
    for (int i = 0; i < size; i++) {
      newPartition.put(i, partition.get(i));
    }

    Assertions.assertEquals(newPartition, partition);
  }
}
