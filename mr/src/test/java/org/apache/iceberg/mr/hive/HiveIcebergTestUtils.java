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
package org.apache.iceberg.mr.hive;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

public class HiveIcebergTestUtils {
  // TODO: Can this be a constant all around the Iceberg tests?
  public static final Schema FULL_SCHEMA =
      new Schema(
          // TODO: Create tests for field case insensitivity.
          optional(1, "boolean_type", Types.BooleanType.get()),
          optional(2, "integer_type", Types.IntegerType.get()),
          optional(3, "long_type", Types.LongType.get()),
          optional(4, "float_type", Types.FloatType.get()),
          optional(5, "double_type", Types.DoubleType.get()),
          optional(6, "date_type", Types.DateType.get()),
          optional(7, "tstz", Types.TimestampType.withZone()),
          optional(8, "ts", Types.TimestampType.withoutZone()),
          optional(9, "string_type", Types.StringType.get()),
          optional(10, "fixed_type", Types.FixedType.ofLength(3)),
          optional(11, "binary_type", Types.BinaryType.get()),
          optional(12, "decimal_type", Types.DecimalType.of(38, 10)),
          optional(13, "time_type", Types.TimeType.get()),
          optional(14, "uuid_type", Types.UUIDType.get()));

  public static final StandardStructObjectInspector FULL_SCHEMA_OBJECT_INSPECTOR =
      ObjectInspectorFactory.getStandardStructObjectInspector(
          Arrays.asList(
              "boolean_type",
              "integer_type",
              "long_type",
              "float_type",
              "double_type",
              "date_type",
              "tstz",
              "ts",
              "string_type",
              "fixed_type",
              "binary_type",
              "decimal_type",
              "time_type",
              "uuid_type"),
          Arrays.asList(
              PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
              PrimitiveObjectInspectorFactory.writableIntObjectInspector,
              PrimitiveObjectInspectorFactory.writableLongObjectInspector,
              PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
              PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
              PrimitiveObjectInspectorFactory.writableDateObjectInspector,
              PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
              PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
              PrimitiveObjectInspectorFactory.writableStringObjectInspector,
              PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
              PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
              PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector,
              PrimitiveObjectInspectorFactory.writableStringObjectInspector,
              PrimitiveObjectInspectorFactory.writableStringObjectInspector));

  private HiveIcebergTestUtils() {
    // Empty constructor for the utility class
  }

  /**
   * Generates a test record where every field has a value.
   *
   * @return Record with every field set
   */
  public static Record getTestRecord() {
    Record record = GenericRecord.create(HiveIcebergTestUtils.FULL_SCHEMA);
    record.set(0, true);
    record.set(1, 1);
    record.set(2, 2L);
    record.set(3, 3.1f);
    record.set(4, 4.2d);
    record.set(5, LocalDate.of(2020, 1, 21));
    // Nano is not supported ?
    record.set(6, OffsetDateTime.of(2017, 11, 22, 11, 30, 7, 0, ZoneOffset.ofHours(2)));
    record.set(7, LocalDateTime.of(2019, 2, 22, 9, 44, 54));
    record.set(8, "kilenc");
    record.set(9, new byte[] {0, 1, 2});
    record.set(10, ByteBuffer.wrap(new byte[] {0, 1, 2, 3}));
    record.set(11, new BigDecimal("0.0000000013"));
    record.set(12, LocalTime.of(11, 33));
    record.set(13, UUID.fromString("73689599-d7fc-4dfb-b94e-106ff20284a5"));

    return record;
  }

  /**
   * Record with every field set to null.
   *
   * @return Empty record
   */
  public static Record getNullTestRecord() {
    Record record = GenericRecord.create(HiveIcebergTestUtils.FULL_SCHEMA);

    for (int i = 0; i < HiveIcebergTestUtils.FULL_SCHEMA.columns().size(); i++) {
      record.set(i, null);
    }

    return record;
  }

  /**
   * Hive values for the test record.
   *
   * @param record The original Iceberg record
   * @return The Hive 'record' containing the same values
   */
  public static List<Object> valuesForTestRecord(Record record) {
    return Arrays.asList(
        new BooleanWritable(Boolean.TRUE),
        new IntWritable(record.get(1, Integer.class)),
        new LongWritable(record.get(2, Long.class)),
        new FloatWritable(record.get(3, Float.class)),
        new DoubleWritable(record.get(4, Double.class)),
        new DateWritable((int) record.get(5, LocalDate.class).toEpochDay()),
        new TimestampWritable(Timestamp.from(record.get(6, OffsetDateTime.class).toInstant())),
        new TimestampWritable(Timestamp.valueOf(record.get(7, LocalDateTime.class))),
        new Text(record.get(8, String.class)),
        new BytesWritable(record.get(9, byte[].class)),
        new BytesWritable(ByteBuffers.toByteArray(record.get(10, ByteBuffer.class))),
        new HiveDecimalWritable(HiveDecimal.create(record.get(11, BigDecimal.class))),
        new Text(record.get(12, LocalTime.class).toString()),
        new Text(record.get(13, UUID.class).toString()));
  }

  /**
   * Converts a list of Object arrays to a list of Iceberg records.
   *
   * @param schema The schema of the Iceberg record
   * @param rows The data of the records
   * @return The list of the converted records
   */
  public static List<Record> valueForRow(Schema schema, List<Object[]> rows) {
    return rows.stream()
        .map(
            row -> {
              Record record = GenericRecord.create(schema);
              for (int i = 0; i < row.length; ++i) {
                record.set(i, row[i]);
              }
              return record;
            })
        .collect(Collectors.toList());
  }

  /**
   * Check if 2 Iceberg records are the same or not. Compares OffsetDateTimes only by the Intant
   * they represent.
   *
   * @param expected The expected record
   * @param actual The actual record
   */
  public static void assertEquals(Record expected, Record actual) {
    for (int i = 0; i < expected.size(); ++i) {
      if (expected.get(i) instanceof OffsetDateTime) {
        // For OffsetDateTime we just compare the actual instant
        assertThat(((OffsetDateTime) actual.get(i)).toInstant())
            .isEqualTo(((OffsetDateTime) expected.get(i)).toInstant());
      } else {
        assertThat(actual.get(i)).isEqualTo(expected.get(i));
      }
    }
  }

  /**
   * Validates whether the table contains the expected records. The results should be sorted by a
   * unique key so we do not end up with flaky tests.
   *
   * @param table The table we should read the records from
   * @param expected The expected list of Records
   * @param sortBy The column position by which we will sort
   * @throws IOException Exceptions when reading the table data
   */
  public static void validateData(Table table, List<Record> expected, int sortBy)
      throws IOException {
    // Refresh the table, so we get the new data as well
    table.refresh();
    List<Record> records = Lists.newArrayListWithExpectedSize(expected.size());
    try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
      iterable.forEach(records::add);
    }

    validateData(expected, records, sortBy);
  }

  /**
   * Validates whether the 2 sets of records are the same. The results should be sorted by a unique
   * key so we do not end up with flaky tests.
   *
   * @param expected The expected list of Records
   * @param actual The actual list of Records
   * @param sortBy The column position by which we will sort
   */
  public static void validateData(List<Record> expected, List<Record> actual, int sortBy) {
    List<Record> sortedExpected = Lists.newArrayList(expected);
    List<Record> sortedActual = Lists.newArrayList(actual);
    // Sort based on the specified column
    sortedExpected.sort(Comparator.comparingLong(record -> (Long) record.get(sortBy)));
    sortedActual.sort(Comparator.comparingLong(record -> (Long) record.get(sortBy)));

    assertThat(sortedActual).hasSize(sortedExpected.size());
    for (int i = 0; i < sortedExpected.size(); ++i) {
      assertEquals(sortedExpected.get(i), sortedActual.get(i));
    }
  }

  /**
   * Validates the number of files under a {@link Table} generated by a specific queryId and jobId.
   * Validates that the commit files are removed.
   *
   * @param table The table we are checking
   * @param conf The configuration used for generating the job location
   * @param jobId The jobId which generated the files
   * @param dataFileNum The expected number of data files (TABLE_LOCATION/data/*)
   */
  public static void validateFiles(Table table, Configuration conf, JobID jobId, int dataFileNum)
      throws IOException {
    List<Path> dataFiles =
        Files.walk(Paths.get(table.location() + "/data"))
            .filter(Files::isRegularFile)
            .filter(path -> !path.getFileName().toString().startsWith("."))
            .collect(Collectors.toList());

    assertThat(dataFiles).hasSize(dataFileNum);
    assertThat(new File(HiveIcebergOutputCommitter.generateJobLocation(table.location(), conf, jobId)))
        .doesNotExist();
  }
}
