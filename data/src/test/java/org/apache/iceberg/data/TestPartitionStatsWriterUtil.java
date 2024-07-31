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
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIterable;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsUtil;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestPartitionStatsWriterUtil {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "binary", Types.BinaryType.get()));

  private static final Random RANDOM = ThreadLocalRandom.current();

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

    for (int i = 0; i < 42; i++) {
      PartitionData partitionData =
          new PartitionData(dataSchema.findField(Column.PARTITION.name()).type().asStructType());
      partitionData.set(0, RANDOM.nextLong());

      Record record = GenericRecord.create(dataSchema);
      record.set(
          Column.PARTITION.ordinal(),
          PartitionStatsUtil.partitionDataToRecord(partitionSchema, partitionData));
      record.set(Column.SPEC_ID.ordinal(), RANDOM.nextInt(10));
      record.set(Column.DATA_RECORD_COUNT.ordinal(), RANDOM.nextLong());
      record.set(Column.DATA_FILE_COUNT.ordinal(), RANDOM.nextInt());
      record.set(Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.ordinal(), 1024L * RANDOM.nextInt(20));
      record.set(Column.POSITION_DELETE_RECORD_COUNT.ordinal(), RANDOM.nextLong());
      record.set(Column.POSITION_DELETE_FILE_COUNT.ordinal(), RANDOM.nextInt());
      record.set(Column.EQUALITY_DELETE_RECORD_COUNT.ordinal(), RANDOM.nextLong());
      record.set(Column.EQUALITY_DELETE_FILE_COUNT.ordinal(), RANDOM.nextInt());
      record.set(Column.TOTAL_RECORD_COUNT.ordinal(), RANDOM.nextLong());
      record.set(Column.LAST_UPDATED_AT.ordinal(), RANDOM.nextLong());
      record.set(Column.LAST_UPDATED_SNAPSHOT_ID.ordinal(), RANDOM.nextLong());

      partitionListBuilder.add(record);
    }

    testPartitionStats(testTable, partitionListBuilder.build(), dataSchema);
  }

  @Test
  public void testPartitionStatsOptionalFields() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Table testTable =
        TestTables.create(
            temp.newFolder("test_partition_stats_optional"),
            "test_partition_stats_optional",
            SCHEMA,
            spec,
            SortOrder.unsorted(),
            2);

    Types.StructType partitionSchema = Partitioning.partitionType(testTable);
    Schema dataSchema = PartitionStatsUtil.schema(partitionSchema);

    ImmutableList.Builder<Record> partitionListBuilder = ImmutableList.builder();

    for (int i = 0; i < 5; i++) {
      PartitionData partitionData =
          new PartitionData(dataSchema.findField(Column.PARTITION.name()).type().asStructType());
      partitionData.set(0, RANDOM.nextLong());

      Record record = GenericRecord.create(dataSchema);
      record.set(
          Column.PARTITION.ordinal(),
          PartitionStatsUtil.partitionDataToRecord(partitionSchema, partitionData));
      record.set(Column.SPEC_ID.ordinal(), RANDOM.nextInt(10));
      record.set(Column.DATA_RECORD_COUNT.ordinal(), RANDOM.nextLong());
      record.set(Column.DATA_FILE_COUNT.ordinal(), RANDOM.nextInt());
      record.set(Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.ordinal(), 1024L * RANDOM.nextInt(20));
      record.set(Column.POSITION_DELETE_RECORD_COUNT.ordinal(), null);
      record.set(Column.POSITION_DELETE_FILE_COUNT.ordinal(), null);
      record.set(Column.EQUALITY_DELETE_RECORD_COUNT.ordinal(), null);
      record.set(Column.EQUALITY_DELETE_FILE_COUNT.ordinal(), null);
      record.set(Column.TOTAL_RECORD_COUNT.ordinal(), null);
      record.set(Column.LAST_UPDATED_AT.ordinal(), null);
      record.set(Column.LAST_UPDATED_SNAPSHOT_ID.ordinal(), null);

      partitionListBuilder.add(record);
    }

    testPartitionStats(testTable, partitionListBuilder.build(), dataSchema);
  }

  @Test
  public void testAllDatatypePartition() throws Exception {
    Schema schema =
        new Schema(
            required(100, "id", Types.LongType.get()),
            optional(101, "data", Types.StringType.get()),
            required(102, "b", Types.BooleanType.get()),
            optional(103, "i", Types.IntegerType.get()),
            required(104, "l", Types.LongType.get()),
            optional(105, "f", Types.FloatType.get()),
            required(106, "d", Types.DoubleType.get()),
            optional(107, "date", Types.DateType.get()),
            required(108, "ts", Types.TimestampType.withoutZone()),
            required(110, "s", Types.StringType.get()),
            required(111, "uuid", Types.UUIDType.get()),
            required(112, "fixed", Types.FixedType.ofLength(7)),
            optional(113, "bytes", Types.BinaryType.get()),
            required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
            required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
            required(116, "dec_38_10", Types.DecimalType.of(38, 10)), // maximum precision
            required(117, "time", Types.TimeType.get()));

    PartitionSpec spec =
        PartitionSpec.builderFor(schema)
            .identity("b")
            .identity("i")
            .identity("l")
            .identity("f")
            .identity("d")
            .identity("date")
            .identity("ts")
            .identity("s")
            .identity("uuid")
            .identity("fixed")
            .identity("bytes")
            .identity("dec_9_0")
            .identity("dec_11_2")
            .identity("dec_38_10")
            .identity("time")
            .build();
    Table testTable =
        TestTables.create(
            temp.newFolder("test_all_type"),
            "test_all_type",
            schema,
            spec,
            SortOrder.unsorted(),
            2);

    Types.StructType partitionSchema = Partitioning.partitionType(testTable);
    Schema dataSchema = PartitionStatsUtil.schema(partitionSchema);

    PartitionData partitionData =
        new PartitionData(dataSchema.findField(Column.PARTITION.name()).type().asStructType());
    partitionData.set(0, true);
    partitionData.set(1, 42);
    partitionData.set(2, 42L);
    partitionData.set(3, 3.14f);
    partitionData.set(4, 3.141592653589793);
    partitionData.set(5, Literal.of("2022-01-01").to(Types.DateType.get()).value());
    partitionData.set(
        6, Literal.of("2017-12-01T10:12:55.038194").to(Types.TimestampType.withoutZone()).value());
    partitionData.set(7, "string");
    partitionData.set(8, UUIDUtil.convertToByteBuffer(UUID.randomUUID()));
    partitionData.set(9, new byte[] {0, 1, 2, 3, 4, 5, 6});
    partitionData.set(10, new byte[] {1, 2, 3});
    partitionData.set(11, Literal.of("123456789").to(Types.DecimalType.of(9, 0)).value());
    partitionData.set(12, Literal.of("1234567.89").to(Types.DecimalType.of(11, 2)).value());
    partitionData.set(
        13, Literal.of("12345678901234567890.1234567890").to(Types.DecimalType.of(38, 10)).value());
    partitionData.set(14, Literal.of("10:10:10").to(Types.TimeType.get()).value());

    Record record = GenericRecord.create(dataSchema);
    record.set(
        Column.PARTITION.ordinal(),
        PartitionStatsUtil.partitionDataToRecord(partitionSchema, partitionData));
    record.set(Column.SPEC_ID.ordinal(), RANDOM.nextInt(10));
    record.set(Column.DATA_RECORD_COUNT.ordinal(), RANDOM.nextLong());
    record.set(Column.DATA_FILE_COUNT.ordinal(), RANDOM.nextInt());
    record.set(Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.ordinal(), 1024L * RANDOM.nextInt(20));

    testPartitionStats(testTable, Collections.singletonList(record), dataSchema);
  }

  private static void testPartitionStats(
      Table testTable, List<Record> expectedRecords, Schema dataSchema) throws IOException {
    OutputFile outputFile = PartitionStatsWriterUtil.newPartitionStatsFile(testTable, 42L);
    PartitionStatsWriterUtil.writePartitionStatsFile(
        testTable, expectedRecords.iterator(), outputFile);
    assertThat(Paths.get(outputFile.location())).exists();

    List<Record> writtenRecords;
    try (CloseableIterable<Record> recordIterator =
        PartitionStatsWriterUtil.readPartitionStatsFile(
            dataSchema, Files.localInput(outputFile.location()))) {
      writtenRecords = Lists.newArrayList(recordIterator);
    }
    assertThatIterable(writtenRecords).isEqualTo(expectedRecords);
  }
}
