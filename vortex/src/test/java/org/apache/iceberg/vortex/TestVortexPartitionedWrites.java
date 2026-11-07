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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestVortexPartitionedWrites {

  private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0);
  private static final OffsetDateTime EPOCH_TZ =
      OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

  @TempDir private File tempDir;

  private int fileCounter = 0;

  @BeforeAll
  static void registerFormatModel() {
    try {
      FormatModelRegistry.register(
          VortexFormatModel.create(
              Record.class,
              Void.class,
              (icebergSchema, fileSchema, engineSchema) ->
                  GenericVortexWriter.buildWriter(icebergSchema),
              (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader));
    } catch (IllegalArgumentException e) {
      // Already registered from another test class
    }
  }

  @Test
  public void testMultiFileWriteWithTimestampDayPartition() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "ts", Types.TimestampType.withoutZone()));

    PartitionSpec daySpec = PartitionSpec.builderFor(schema).day("ts").build();

    LocalDateTime day1Ts = LocalDateTime.of(2023, 6, 15, 10, 30);
    LocalDateTime day2Ts = LocalDateTime.of(2023, 7, 20, 14, 0);

    int expectedDay1 = (int) LocalDate.of(2023, 6, 15).toEpochDay();
    int expectedDay2 = (int) LocalDate.of(2023, 7, 20).toEpochDay();

    DataFile df1 = writeVortexFile(schema, daySpec, expectedDay1, makeRecord(schema, 1L, day1Ts));
    DataFile df2 = writeVortexFile(schema, daySpec, expectedDay2, makeRecord(schema, 2L, day2Ts));

    assertThat(df1.partition().get(0, Integer.class)).isEqualTo(expectedDay1);
    assertThat(df2.partition().get(0, Integer.class)).isEqualTo(expectedDay2);
    assertThat(df1.partition().get(0, Integer.class))
        .isNotEqualTo(df2.partition().get(0, Integer.class));
    assertThat(df1.recordCount()).isEqualTo(1);
    assertThat(df2.recordCount()).isEqualTo(1);
    assertThat(df1.format()).isEqualTo(FileFormat.VORTEX);
    assertThat(df2.format()).isEqualTo(FileFormat.VORTEX);
  }

  @Test
  public void testMultiFileWriteWithTimestampHourPartition() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "ts", Types.TimestampType.withoutZone()));

    PartitionSpec hourSpec = PartitionSpec.builderFor(schema).hour("ts").build();

    LocalDateTime hour1 = LocalDateTime.of(2023, 6, 15, 10, 30);
    LocalDateTime hour2 = LocalDateTime.of(2023, 6, 15, 14, 0);
    LocalDateTime hour3 = LocalDateTime.of(2023, 7, 20, 8, 45);

    int expectedHour1 = (int) ChronoUnit.HOURS.between(EPOCH, hour1);
    int expectedHour2 = (int) ChronoUnit.HOURS.between(EPOCH, hour2);
    int expectedHour3 = (int) ChronoUnit.HOURS.between(EPOCH, hour3);

    DataFile df1 = writeVortexFile(schema, hourSpec, expectedHour1, makeRecord(schema, 1L, hour1));
    DataFile df2 = writeVortexFile(schema, hourSpec, expectedHour2, makeRecord(schema, 2L, hour2));
    DataFile df3 = writeVortexFile(schema, hourSpec, expectedHour3, makeRecord(schema, 3L, hour3));

    assertThat(df1.partition().get(0, Integer.class)).isEqualTo(expectedHour1);
    assertThat(df2.partition().get(0, Integer.class)).isEqualTo(expectedHour2);
    assertThat(df3.partition().get(0, Integer.class)).isEqualTo(expectedHour3);

    // Same day but different hours should produce different partition values
    assertThat(expectedHour1).isNotEqualTo(expectedHour2);
    // Different days should also differ
    assertThat(expectedHour1).isNotEqualTo(expectedHour3);
  }

  @Test
  public void testMultiFileWriteWithTimestampTzDayPartition() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "ts", Types.TimestampType.withZone()));

    PartitionSpec daySpec = PartitionSpec.builderFor(schema).day("ts").build();

    OffsetDateTime day1 = OffsetDateTime.of(2023, 6, 15, 10, 30, 0, 0, ZoneOffset.UTC);
    OffsetDateTime day2 = OffsetDateTime.of(2023, 7, 20, 14, 0, 0, 0, ZoneOffset.UTC);

    int expectedDay1 = (int) LocalDate.of(2023, 6, 15).toEpochDay();
    int expectedDay2 = (int) LocalDate.of(2023, 7, 20).toEpochDay();

    DataFile df1 = writeVortexFile(schema, daySpec, expectedDay1, makeRecordTz(schema, 1L, day1));
    DataFile df2 = writeVortexFile(schema, daySpec, expectedDay2, makeRecordTz(schema, 2L, day2));

    assertThat(df1.partition().get(0, Integer.class)).isEqualTo(expectedDay1);
    assertThat(df2.partition().get(0, Integer.class)).isEqualTo(expectedDay2);
    assertThat(df1.recordCount()).isEqualTo(1);
    assertThat(df2.recordCount()).isEqualTo(1);
  }

  @Test
  public void testMultiFileWriteWithTimestampTzHourPartition() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "ts", Types.TimestampType.withZone()));

    PartitionSpec hourSpec = PartitionSpec.builderFor(schema).hour("ts").build();

    OffsetDateTime hour1 = OffsetDateTime.of(2023, 6, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    OffsetDateTime hour2 = OffsetDateTime.of(2023, 6, 15, 22, 0, 0, 0, ZoneOffset.UTC);

    int expectedHour1 = (int) ChronoUnit.HOURS.between(EPOCH_TZ, hour1);
    int expectedHour2 = (int) ChronoUnit.HOURS.between(EPOCH_TZ, hour2);

    DataFile df1 =
        writeVortexFile(schema, hourSpec, expectedHour1, makeRecordTz(schema, 1L, hour1));
    DataFile df2 =
        writeVortexFile(schema, hourSpec, expectedHour2, makeRecordTz(schema, 2L, hour2));

    assertThat(df1.partition().get(0, Integer.class)).isEqualTo(expectedHour1);
    assertThat(df2.partition().get(0, Integer.class)).isEqualTo(expectedHour2);
    assertThat(expectedHour1).isNotEqualTo(expectedHour2);
  }

  @Test
  public void testMultipleRecordsPerPartitionFile() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "ts", Types.TimestampType.withoutZone()));

    PartitionSpec daySpec = PartitionSpec.builderFor(schema).day("ts").build();

    // Multiple records in the same day partition
    LocalDateTime ts1 = LocalDateTime.of(2023, 6, 15, 8, 0);
    LocalDateTime ts2 = LocalDateTime.of(2023, 6, 15, 12, 0);
    LocalDateTime ts3 = LocalDateTime.of(2023, 6, 15, 18, 30);

    int expectedDay = (int) LocalDate.of(2023, 6, 15).toEpochDay();

    DataFile df =
        writeVortexFile(
            schema,
            daySpec,
            expectedDay,
            makeRecord(schema, 1L, ts1),
            makeRecord(schema, 2L, ts2),
            makeRecord(schema, 3L, ts3));

    assertThat(df.partition().get(0, Integer.class)).isEqualTo(expectedDay);
    assertThat(df.recordCount()).isEqualTo(3);
  }

  @Test
  public void testMultiplePartitionsWithMultipleRecordsEach() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "ts", Types.TimestampType.withoutZone()));

    PartitionSpec daySpec = PartitionSpec.builderFor(schema).day("ts").build();

    int day1 = (int) LocalDate.of(2023, 1, 10).toEpochDay();
    int day2 = (int) LocalDate.of(2023, 6, 15).toEpochDay();
    int day3 = (int) LocalDate.of(2023, 12, 25).toEpochDay();

    DataFile df1 =
        writeVortexFile(
            schema,
            daySpec,
            day1,
            makeRecord(schema, 1L, LocalDateTime.of(2023, 1, 10, 9, 0)),
            makeRecord(schema, 2L, LocalDateTime.of(2023, 1, 10, 17, 30)));

    DataFile df2 =
        writeVortexFile(
            schema,
            daySpec,
            day2,
            makeRecord(schema, 3L, LocalDateTime.of(2023, 6, 15, 6, 0)),
            makeRecord(schema, 4L, LocalDateTime.of(2023, 6, 15, 23, 59)));

    DataFile df3 =
        writeVortexFile(
            schema,
            daySpec,
            day3,
            makeRecord(schema, 5L, LocalDateTime.of(2023, 12, 25, 0, 0)),
            makeRecord(schema, 6L, LocalDateTime.of(2023, 12, 25, 12, 0)),
            makeRecord(schema, 7L, LocalDateTime.of(2023, 12, 25, 23, 59)));

    assertThat(df1.partition().get(0, Integer.class)).isEqualTo(day1);
    assertThat(df1.recordCount()).isEqualTo(2);

    assertThat(df2.partition().get(0, Integer.class)).isEqualTo(day2);
    assertThat(df2.recordCount()).isEqualTo(2);

    assertThat(df3.partition().get(0, Integer.class)).isEqualTo(day3);
    assertThat(df3.recordCount()).isEqualTo(3);

    // All three partitions should be distinct
    assertThat(df1.partition().get(0, Integer.class))
        .isNotEqualTo(df2.partition().get(0, Integer.class));
    assertThat(df2.partition().get(0, Integer.class))
        .isNotEqualTo(df3.partition().get(0, Integer.class));
  }

  private DataFile writeVortexFile(
      Schema schema, PartitionSpec spec, int partitionValue, Record... records) throws IOException {
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, partitionValue);

    File outFile = new File(tempDir, "data-" + (fileCounter++) + ".vortex");
    EncryptedOutputFile encFile = EncryptedFiles.plainAsEncryptedOutput(Files.localOutput(outFile));

    DataWriter<Record> writer =
        FormatModelRegistry.<Record, Void>dataWriteBuilder(FileFormat.VORTEX, Record.class, encFile)
            .schema(schema)
            .spec(spec)
            .partition(partition)
            .build();

    for (Record record : records) {
      writer.write(record);
    }
    writer.close();

    return writer.toDataFile();
  }

  private static Record makeRecord(Schema schema, long id, LocalDateTime ts) {
    GenericRecord record = GenericRecord.create(schema);
    record.set(0, id);
    record.set(1, ts);
    return record;
  }

  private static Record makeRecordTz(Schema schema, long id, OffsetDateTime ts) {
    GenericRecord record = GenericRecord.create(schema);
    record.set(0, id);
    record.set(1, ts);
    return record;
  }
}
