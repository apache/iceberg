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
package org.apache.iceberg.data;

import static org.apache.iceberg.PartitionStatsUtil.Column;

import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsUtil;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPartitionStatsWriterUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TestPartitionStatsWriterUtil.class);

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "binary", Types.BinaryType.get()));

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testPartitionStats() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Table testTable =
        TestTables.create(
            temp.newFolder("test_partition_stats"),
            "test_partition_stats",
            SCHEMA,
            spec,
            SortOrder.unsorted(),
            2);

    Types.StructType partitionSchema = Partitioning.partitionType(testTable);
    Schema dataSchema = PartitionStatsUtil.schema(partitionSchema);

    ImmutableList.Builder<Record> partitionListBuilder = ImmutableList.builder();

    long seed = System.currentTimeMillis();
    LOG.info("Seed used for random generator is {}", seed);
    Random random = new Random(seed);

    for (int i = 0; i < 42; i++) {
      PartitionData partitionData =
          new PartitionData(
              dataSchema.findField(Column.PARTITION_DATA.name()).type().asStructType());
      partitionData.set(0, random.nextLong());

      Record record = GenericRecord.create(dataSchema);
      record.set(
          Column.PARTITION_DATA.ordinal(),
          PartitionStatsUtil.partitionDataToRecord(partitionSchema, partitionData));
      record.set(Column.SPEC_ID.ordinal(), random.nextInt(10));
      record.set(Column.DATA_RECORD_COUNT.ordinal(), random.nextLong());
      record.set(Column.DATA_FILE_COUNT.ordinal(), random.nextInt());
      record.set(Column.DATA_FILE_SIZE_IN_BYTES.ordinal(), 1024L * random.nextInt(20));
      record.set(Column.POSITION_DELETE_RECORD_COUNT.ordinal(), random.nextLong());
      record.set(Column.POSITION_DELETE_FILE_COUNT.ordinal(), random.nextInt());
      record.set(Column.EQUALITY_DELETE_RECORD_COUNT.ordinal(), random.nextLong());
      record.set(Column.EQUALITY_DELETE_FILE_COUNT.ordinal(), random.nextInt());
      record.set(Column.TOTAL_RECORD_COUNT.ordinal(), random.nextLong());
      record.set(Column.LAST_UPDATED_AT.ordinal(), random.nextLong());
      record.set(Column.LAST_UPDATED_SNAPSHOT_ID.ordinal(), random.nextLong());

      partitionListBuilder.add(record);
    }
    List<Record> records = partitionListBuilder.build();

    OutputFile outputFile = PartitionStatsWriterUtil.newPartitionStatsFile(testTable, 42L);
    PartitionStatsWriterUtil.writePartitionStatsFile(testTable, records.iterator(), outputFile);

    Assertions.assertThat(Paths.get(outputFile.location())).exists();

    List<Record> rows;
    try (CloseableIterable<Record> recordIterator =
        PartitionStatsWriterUtil.readPartitionStatsFile(
            dataSchema, Files.localInput(outputFile.location()))) {
      rows = Lists.newArrayList(recordIterator);
    }

    Assertions.assertThat(rows).hasSize(records.size());
    for (int i = 0; i < records.size(); i++) {
      Assertions.assertThat(rows.get(i)).isEqualTo(records.get(i));
    }
  }
}
