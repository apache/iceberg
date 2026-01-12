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

import static org.apache.iceberg.PartitionStatsHandler.DATA_FILE_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.DATA_RECORD_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.EQUALITY_DELETE_FILE_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.EQUALITY_DELETE_RECORD_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.LAST_UPDATED_AT;
import static org.apache.iceberg.PartitionStatsHandler.LAST_UPDATED_SNAPSHOT_ID;
import static org.apache.iceberg.PartitionStatsHandler.PARTITION_FIELD_NAME;
import static org.apache.iceberg.PartitionStatsHandler.POSITION_DELETE_FILE_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.POSITION_DELETE_RECORD_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.SPEC_ID;
import static org.apache.iceberg.PartitionStatsHandler.TOTAL_DATA_FILE_SIZE_IN_BYTES;
import static org.apache.iceberg.PartitionStatsHandler.TOTAL_RECORD_COUNT;
import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;

public abstract class PartitionStatisticsTestBase {

  @TempDir private File temp;

  protected static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("c2").identity("c3").build();

  private static final Random RANDOM = ThreadLocalRandom.current();

  protected Schema invalidOldSchema(Types.StructType unifiedPartitionType) {
    // field ids starts from 0 instead of 1
    return new Schema(
        Types.NestedField.required(0, PARTITION_FIELD_NAME, unifiedPartitionType),
        Types.NestedField.required(1, SPEC_ID.name(), Types.IntegerType.get()),
        Types.NestedField.required(2, DATA_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.required(3, DATA_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.required(4, TOTAL_DATA_FILE_SIZE_IN_BYTES.name(), Types.LongType.get()),
        Types.NestedField.optional(5, POSITION_DELETE_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(6, POSITION_DELETE_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.optional(7, EQUALITY_DELETE_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(8, EQUALITY_DELETE_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.optional(9, TOTAL_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(10, LAST_UPDATED_AT.name(), Types.LongType.get()),
        Types.NestedField.optional(11, LAST_UPDATED_SNAPSHOT_ID.name(), Types.LongType.get()));
  }

  protected PartitionStatistics randomStats(Types.StructType partitionType) {
    PartitionData partitionData = new PartitionData(partitionType);
    partitionData.set(0, RANDOM.nextInt());

    return randomStats(partitionData);
  }

  protected PartitionStatistics randomStats(PartitionData partitionData) {
    PartitionStatistics stats = new BasePartitionStatistics(partitionData, RANDOM.nextInt(10));
    stats.set(PartitionStatistics.DATA_RECORD_COUNT_POSITION, RANDOM.nextLong());
    stats.set(PartitionStatistics.DATA_FILE_COUNT_POSITION, RANDOM.nextInt());
    stats.set(
        PartitionStatistics.TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION, 1024L * RANDOM.nextInt(20));
    return stats;
  }

  protected File tempDir(String folderName) throws IOException {
    return java.nio.file.Files.createTempDirectory(temp.toPath(), folderName).toFile();
  }

  protected static StructLike partitionRecord(
      Types.StructType partitionType, String val1, String val2) {
    GenericRecord record = GenericRecord.create(partitionType);
    record.set(0, val1);
    record.set(1, val2);
    return record;
  }
}
