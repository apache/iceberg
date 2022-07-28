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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.avro.AvroSchemaUtil.convert;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.UUIDType;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestBloomRowGroupFilter {

  private static final Types.StructType structFieldType =
      Types.StructType.of(Types.NestedField.required(16, "int_field", IntegerType.get()));
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", IntegerType.get()),
          required(2, "long", LongType.get()),
          required(3, "double", DoubleType.get()),
          required(4, "float", FloatType.get()),
          required(5, "string", StringType.get()),
          required(6, "uuid", UUIDType.get()),
          required(7, "required", StringType.get()),
          optional(8, "non_bloom", StringType.get()),
          optional(9, "all_nulls", LongType.get()),
          optional(10, "some_nulls", StringType.get()),
          optional(11, "no_nulls", StringType.get()),
          optional(12, "all_nans", DoubleType.get()),
          optional(13, "some_nans", FloatType.get()),
          optional(14, "no_nans", DoubleType.get()),
          optional(15, "struct_not_null", structFieldType),
          optional(17, "not_in_file", FloatType.get()),
          optional(18, "no_stats", StringType.get()),
          optional(19, "boolean", Types.BooleanType.get()),
          optional(20, "time", Types.TimeType.get()),
          optional(21, "date", Types.DateType.get()),
          optional(22, "timestamp", Types.TimestampType.withoutZone()),
          optional(23, "timestamptz", Types.TimestampType.withZone()),
          optional(24, "binary", Types.BinaryType.get()),
          optional(25, "int_decimal", Types.DecimalType.of(8, 2)),
          optional(26, "long_decimal", Types.DecimalType.of(14, 2)),
          optional(27, "fixed_decimal", Types.DecimalType.of(31, 2)));

  private static final Types.StructType _structFieldType =
      Types.StructType.of(Types.NestedField.required(16, "_int_field", IntegerType.get()));

  private static final Schema FILE_SCHEMA =
      new Schema(
          required(1, "_id", IntegerType.get()),
          required(2, "_long", LongType.get()),
          required(3, "_double", DoubleType.get()),
          required(4, "_float", FloatType.get()),
          required(5, "_string", StringType.get()),
          required(6, "_uuid", UUIDType.get()),
          required(7, "_required", StringType.get()),
          required(8, "_non_bloom", StringType.get()),
          optional(9, "_all_nulls", LongType.get()),
          optional(10, "_some_nulls", StringType.get()),
          optional(11, "_no_nulls", StringType.get()),
          optional(12, "_all_nans", DoubleType.get()),
          optional(13, "_some_nans", FloatType.get()),
          optional(14, "_no_nans", DoubleType.get()),
          optional(15, "_struct_not_null", _structFieldType),
          optional(18, "_no_stats", StringType.get()),
          optional(19, "_boolean", Types.BooleanType.get()),
          optional(20, "_time", Types.TimeType.get()),
          optional(21, "_date", Types.DateType.get()),
          optional(22, "_timestamp", Types.TimestampType.withoutZone()),
          optional(23, "_timestamptz", Types.TimestampType.withZone()),
          optional(24, "_binary", Types.BinaryType.get()),
          optional(25, "_int_decimal", Types.DecimalType.of(8, 2)),
          optional(26, "_long_decimal", Types.DecimalType.of(14, 2)),
          optional(27, "_fixed_decimal", Types.DecimalType.of(31, 2)));

  private static final String TOO_LONG_FOR_STATS;

  static {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i += 1) {
      sb.append(UUID.randomUUID().toString());
    }
    TOO_LONG_FOR_STATS = sb.toString();
  }

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 79;
  private static final int INT_VALUE_COUNT = INT_MAX_VALUE - INT_MIN_VALUE + 1;
  private static final long LONG_BASE = 100L;
  private static final double DOUBLE_BASE = 1000D;
  private static final float FLOAT_BASE = 10000F;
  private static final String BINARY_PREFIX = "BINARY测试_";
  private static final Instant instant = Instant.parse("2018-10-10T00:00:00.000Z");
  private static final List<UUID> RANDOM_UUIDS;
  private static final List<byte[]> RANDOM_BYTES;

  static {
    RANDOM_UUIDS = Lists.newArrayList();
    for (int i = 0; i < INT_VALUE_COUNT; i += 1) {
      RANDOM_UUIDS.add(UUID.randomUUID());
    }

    RANDOM_BYTES = Lists.newArrayList();
    Random rd = new Random();
    for (int i = 1; i <= INT_VALUE_COUNT; i += 1) {
      byte[] byteArray = new byte[i];
      rd.nextBytes(byteArray);
      RANDOM_BYTES.add(byteArray);
    }
  }

  private MessageType parquetSchema = null;
  private BlockMetaData rowGroupMetadata = null;
  private BloomFilterReader bloomStore = null;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void createInputFile() throws IOException {
    File parquetFile = temp.newFile();
    Assert.assertTrue(parquetFile.delete());

    // build struct field schema
    org.apache.avro.Schema structSchema = AvroSchemaUtil.convert(_structFieldType);

    OutputFile outFile = Files.localOutput(parquetFile);
    try (FileAppender<Record> appender =
        Parquet.write(outFile)
            .schema(FILE_SCHEMA)
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_id", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_long", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_double", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_float", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_string", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_uuid", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_required", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_all_nulls", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_some_nulls", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_no_nulls", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_all_nans", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_some_nans", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_no_nans", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_struct_not_null._int_field", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_not_in_file", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_no_stats", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_boolean", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_time", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_date", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_timestamp", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_timestamptz", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_binary", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_int_decimal", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_long_decimal", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "_fixed_decimal", "true")
            .build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(convert(FILE_SCHEMA, "table"));
      // create 50 records
      for (int i = 0; i < INT_VALUE_COUNT; i += 1) {
        builder.set("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
        builder.set("_long", LONG_BASE + INT_MIN_VALUE + i); // min=130L, max=179L, num-nulls=0
        builder.set(
            "_double", DOUBLE_BASE + INT_MIN_VALUE + i); // min=1030D, max=1079D, num-nulls=0
        builder.set(
            "_float", FLOAT_BASE + INT_MIN_VALUE + i); // min=10030F, max=10079F, num-nulls=0
        builder.set(
            "_string",
            BINARY_PREFIX + (INT_MIN_VALUE + i)); // min=BINARY测试_30, max=BINARY测试_79, num-nulls=0
        builder.set("_uuid", RANDOM_UUIDS.get(i)); // required, random uuid, always non-null
        builder.set("_required", "req"); // required, always non-null
        builder.set("_non_bloom", RANDOM_UUIDS.get(i)); // bloom filter not enabled
        builder.set("_all_nulls", null); // never non-null
        builder.set("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        builder.set("_no_nulls", ""); // optional, but always non-null
        builder.set("_all_nans", Double.NaN); // never non-nan
        builder.set("_some_nans", (i % 10 == 0) ? Float.NaN : 2F); // includes some nan values
        builder.set("_no_nans", 3D); // optional, but always non-nan
        Record structNotNull = new Record(structSchema);
        structNotNull.put("_int_field", INT_MIN_VALUE + i);
        builder.set("_struct_not_null", structNotNull); // struct with int
        builder.set("_no_stats", TOO_LONG_FOR_STATS); // value longer than 4k will produce no stats
        builder.set("_boolean", (i % 2 == 0) ? true : false);
        builder.set("_time", instant.plusSeconds(i * 86400).toEpochMilli());
        builder.set("_date", instant.plusSeconds(i * 86400).getEpochSecond());
        builder.set("_timestamp", instant.plusSeconds(i * 86400).toEpochMilli());
        builder.set("_timestamptz", instant.plusSeconds(i * 86400).toEpochMilli());
        builder.set("_binary", RANDOM_BYTES.get(i));
        builder.set("_int_decimal", new BigDecimal(String.valueOf(77.77 + i)));
        builder.set("_long_decimal", new BigDecimal(String.valueOf(88.88 + i)));
        builder.set("_fixed_decimal", new BigDecimal(String.valueOf(99.99 + i)));

        appender.add(builder.build());
      }
    }

    InputFile inFile = Files.localInput(parquetFile);

    ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inFile));

    Assert.assertEquals("Should create only one row group", 1, reader.getRowGroups().size());
    rowGroupMetadata = reader.getRowGroups().get(0);
    parquetSchema = reader.getFileMetaData().getSchema();
    bloomStore = reader.getBloomFilterDataReader(rowGroupMetadata);
  }

  @Test
  public void testNotNull() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notNull("all_nulls"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notNull("some_nulls"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notNull("no_nulls"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notNull("struct_not_null.int_field"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: this field is required and are always not-null", shouldRead);
  }

  @Test
  public void testIsNull() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, isNull("all_nulls"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, isNull("some_nulls"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, isNull("no_nulls"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, isNull("struct_not_null.int_field"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: this field is required and are always not-null", shouldRead);
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notNull("required"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: required columns are always non-null", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, isNull("required"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: required columns are always non-null", shouldRead);
  }

  @Test
  public void testIsNaNs() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, isNaN("all_nans"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, isNaN("some_nans"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, isNaN("no_nans"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testNotNaNs() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notNaN("all_nans"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notNaN("some_nans"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notNaN("no_nans"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testStartsWith() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, startsWith("non_bloom", "re"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: no bloom", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, startsWith("required", "re"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, startsWith("required", "req"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, startsWith("some_nulls", "so"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, startsWith("required", "reqs"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, startsWith("some_nulls", "somex"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, startsWith("no_nulls", "xxx"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testMissingColumn() {
    TestHelpers.assertThrows(
        "Should complain about missing column in expression",
        ValidationException.class,
        "Cannot find field 'missing'",
        () ->
            new ParquetBloomRowGroupFilter(SCHEMA, lessThan("missing", 5))
                .shouldRead(parquetSchema, rowGroupMetadata, bloomStore));
  }

  @Test
  public void testColumnNotInFile() {
    Expression[] exprs =
        new Expression[] {
          lessThan("not_in_file", 1.0f),
          lessThanOrEqual("not_in_file", 1.0f),
          equal("not_in_file", 1.0f),
          greaterThan("not_in_file", 1.0f),
          greaterThanOrEqual("not_in_file", 1.0f),
          notNull("not_in_file"),
          isNull("not_in_file"),
          notEqual("not_in_file", 1.0f),
          in("not_in_file", 1.0f, 2.0f)
        };

    for (Expression expr : exprs) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, expr)
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter cannot be found: " + expr, shouldRead);
    }
  }

  @Test
  public void testColumnNotBloomFilterEnabled() {
    Expression[] exprs =
        new Expression[] {
          lessThan("non_bloom", "a"), lessThanOrEqual("non_bloom", "a"), equal("non_bloom", "a"),
          greaterThan("non_bloom", "a"), greaterThanOrEqual("non_bloom", "a"), notNull("non_bloom"),
          isNull("non_bloom"), notEqual("non_bloom", "a"), in("non_bloom", "a", "test1", "test2")
        };

    for (Expression expr : exprs) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, expr)
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter cannot be found: " + expr, shouldRead);
    }
  }

  @Test
  public void testMissingStats() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("no_stats", "a"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: stats are missing but bloom filter is present", shouldRead);
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), otherwise binding will simplify
    // it out
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", i)))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), otherwise binding will simplify
    // it out
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA, and(equal("id", INT_MIN_VALUE - 25), equal("id", INT_MIN_VALUE + 30)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: and(false, true)", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA, and(equal("id", INT_MIN_VALUE - 25), equal("id", INT_MAX_VALUE + 1)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA, and(equal("id", INT_MIN_VALUE + 25), equal("id", INT_MIN_VALUE)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: and(true, true)", shouldRead);

    // AND filters that refer different columns ("id", "long", "binary")
    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                and(
                    equal("id", INT_MIN_VALUE + 25),
                    equal("long", LONG_BASE + 30),
                    equal("binary", RANDOM_BYTES.get(30))))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: and(true, true, true)", shouldRead);

    // AND filters that refer different columns ("id", "long", "binary")
    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                and(
                    equal("id", INT_MIN_VALUE - 25),
                    equal("long", LONG_BASE + 30),
                    equal("binary", RANDOM_BYTES.get(30))))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: and(false, true, true)", shouldRead);

    // In And, one of the filter's column doesn't have bloom filter
    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                and(
                    equal("id", INT_MIN_VALUE + 25),
                    equal("long", LONG_BASE + 30),
                    equal("binary", RANDOM_BYTES.get(30)),
                    equal("non_bloom", "a")))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: and(true, true, true, true)", shouldRead);

    // In And, one of the filter's column doesn't have bloom filter
    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                and(
                    equal("id", INT_MIN_VALUE - 25),
                    equal("long", LONG_BASE + 30),
                    equal("binary", RANDOM_BYTES.get(30)),
                    equal("non_bloom", "a")))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: and(false, true, true, true)", shouldRead);

    // In And, one of the filter's column is not in the file
    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                and(
                    equal("id", INT_MIN_VALUE + 25),
                    equal("long", LONG_BASE + 30),
                    equal("binary", RANDOM_BYTES.get(30)),
                    equal("not_in_file", 1.0f)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: and(true, true, true, true)", shouldRead);

    // In And, one of the filter's column is not in the file
    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                and(
                    equal("id", INT_MIN_VALUE - 25),
                    equal("long", LONG_BASE + 30),
                    equal("binary", RANDOM_BYTES.get(30)),
                    equal("not_in_file", 1.0f)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: and(false, true, true, true)", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), otherwise binding will simplify
    // it out
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA, or(equal("id", INT_MIN_VALUE - 25), equal("id", INT_MAX_VALUE + 1)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA, or(equal("id", INT_MIN_VALUE - 25), equal("id", INT_MAX_VALUE - 19)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, lessThan("id", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testIntegerLtEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, lessThanOrEqual("id", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testIntegerGt() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, greaterThan("id", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testIntegerGtEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, greaterThanOrEqual("id", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testIntegerEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("id", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      if (i >= INT_MIN_VALUE && i <= INT_MAX_VALUE) {
        Assert.assertTrue("Should read: integer within range", shouldRead);
      } else {
        Assert.assertFalse("Should not read: integer outside range", shouldRead);
      }
    }
  }

  @Test
  public void testLongEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("long", LONG_BASE + i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      if (i >= INT_MIN_VALUE && i <= INT_MAX_VALUE) {
        Assert.assertTrue("Should read: long within range", shouldRead);
      } else {
        Assert.assertFalse("Should not read: long outside range", shouldRead);
      }
    }
  }

  @Test
  public void testBytesEq() {
    for (int i = 0; i < INT_VALUE_COUNT; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("binary", RANDOM_BYTES.get(i)))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: binary within range", shouldRead);
    }

    Random rd = new Random();
    for (int i = 1; i <= 10; i += 1) {
      byte[] byteArray = new byte[i];
      rd.nextBytes(byteArray);
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("binary", byteArray))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertFalse("Should not read: cannot match a new generated binary", shouldRead);
    }
  }

  @Test
  public void testIntDeciamlEq() {
    for (int i = 0; i < INT_VALUE_COUNT; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(
                  SCHEMA, equal("int_decimal", new BigDecimal(String.valueOf(77.77 + i))))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: decimal within range", shouldRead);
    }

    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("int_decimal", new BigDecimal("1234.56")))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: decimal outside range", shouldRead);
  }

  @Test
  public void testLongDeciamlEq() {
    for (int i = 0; i < INT_VALUE_COUNT; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(
                  SCHEMA, equal("long_decimal", new BigDecimal(String.valueOf(88.88 + i))))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: decimal within range", shouldRead);
    }

    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("long_decimal", new BigDecimal("1234.56")))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: decimal outside range", shouldRead);
  }

  @Test
  public void testFixedDeciamlEq() {
    for (int i = 0; i < INT_VALUE_COUNT; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(
                  SCHEMA, equal("fixed_decimal", new BigDecimal(String.valueOf(99.99 + i))))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: decimal within range", shouldRead);
    }

    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("fixed_decimal", new BigDecimal("1234.56")))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: decimal outside range", shouldRead);
  }

  @Test
  public void testDoubleEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("double", DOUBLE_BASE + i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      if (i >= INT_MIN_VALUE && i <= INT_MAX_VALUE) {
        Assert.assertTrue("Should read: double within range", shouldRead);
      } else {
        Assert.assertFalse("Should not read: double outside range", shouldRead);
      }
    }
  }

  @Test
  public void testFloatEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("float", FLOAT_BASE + i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      if (i >= INT_MIN_VALUE && i <= INT_MAX_VALUE) {
        Assert.assertTrue("Should read: float within range", shouldRead);
      } else {
        Assert.assertFalse("Should not read: float outside range", shouldRead);
      }
    }
  }

  @Test
  public void testStringEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("string", BINARY_PREFIX + i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      if (i >= INT_MIN_VALUE && i <= INT_MAX_VALUE) {
        Assert.assertTrue("Should read: string within range", shouldRead);
      } else {
        Assert.assertFalse("Should not read: string outside range", shouldRead);
      }
    }
  }

  @Test
  public void testRandomBinaryEq() {
    for (int i = 0; i < INT_VALUE_COUNT; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("uuid", RANDOM_UUIDS.get(i)))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: uuid within range", shouldRead);
    }

    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("uuid", UUID.randomUUID().toString()))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: cannot match a new generated random uuid", shouldRead);
  }

  @Test
  public void testBooleanEq() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("boolean", true))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter is not supported for Boolean", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("boolean", false))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter is not supported for Boolean", shouldRead);
  }

  @Test
  public void testTimeEq() {
    for (int i = -20; i < INT_VALUE_COUNT + 20; i++) {
      Instant ins = instant.plusSeconds(i * 86400);
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("time", ins.toEpochMilli()))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      if (i >= 0 && i < INT_VALUE_COUNT) {
        Assert.assertTrue("Should read: time within range", shouldRead);
      } else {
        Assert.assertFalse("Should not read: time outside range", shouldRead);
      }
    }
  }

  @Test
  public void testDateEq() {
    for (int i = -20; i < INT_VALUE_COUNT + 20; i++) {
      Instant ins = instant.plusSeconds(i * 86400);
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("date", ins.getEpochSecond()))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      if (i >= 0 && i < INT_VALUE_COUNT) {
        Assert.assertTrue("Should read: date within range", shouldRead);
      } else {
        Assert.assertFalse("Should not read: date outside range", shouldRead);
      }
    }
  }

  @Test
  public void testTimestampEq() {
    for (int i = -20; i < INT_VALUE_COUNT + 20; i++) {
      Instant ins = instant.plusSeconds(i * 86400);
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("timestamp", ins.toEpochMilli()))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      if (i >= 0 && i < INT_VALUE_COUNT) {
        Assert.assertTrue("Should read: timestamp within range", shouldRead);
      } else {
        Assert.assertFalse("Should not read: timestamp outside range", shouldRead);
      }
    }
  }

  @Test
  public void testTimestamptzEq() {
    for (int i = -20; i < INT_VALUE_COUNT + 20; i++) {
      Instant ins = instant.plusSeconds(i * 86400);
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("timestamptz", ins.toEpochMilli()))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      if (i >= 0 && i < INT_VALUE_COUNT) {
        Assert.assertTrue("Should read: timestamptz within range", shouldRead);
      } else {
        Assert.assertFalse("Should not read: timestamptz outside range", shouldRead);
      }
    }
  }

  @Test
  public void testIntegerNotEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, notEqual("id", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testIntegerNotEqRewritten() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", i)))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testStringNotEq() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notEqual("some_nulls", "some"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notEqual("no_nulls", ""))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testStructFieldLt() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testStructFieldLtEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, lessThanOrEqual("struct_not_null.int_field", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testStructFieldGt() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, greaterThan("struct_not_null.int_field", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testStructFieldGtEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, greaterThanOrEqual("struct_not_null.int_field", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testStructFieldEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      if (i >= INT_MIN_VALUE && i <= INT_MAX_VALUE) {
        Assert.assertTrue("Should read: value within range", shouldRead);
      } else {
        Assert.assertFalse("Should not read: value outside range", shouldRead);
      }
    }
  }

  @Test
  public void testStructFieldNotEq() {
    for (int i = INT_MIN_VALUE - 20; i < INT_MAX_VALUE + 20; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }
  }

  @Test
  public void testCaseInsensitive() {
    // the column name is required. If setting caseSentitive to true, ValidationException: Cannot
    // find field 'Required'
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("Required", "Req"), false)
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: contains only 'req'", shouldRead);
  }

  @Test
  public void testMissingBloomFilterForColumn() {
    TestHelpers.assertThrows(
        "Should complain about missing bloom filter",
        IllegalStateException.class,
        "Failed to read required bloom filter for id: 10",
        () ->
            new ParquetBloomRowGroupFilter(SCHEMA, equal("some_nulls", "some"))
                .shouldRead(
                    parquetSchema,
                    rowGroupMetadata,
                    new DummyBloomFilterReader(null, rowGroupMetadata)));
  }

  private static class DummyBloomFilterReader extends BloomFilterReader {
    DummyBloomFilterReader(ParquetFileReader fileReader, BlockMetaData block) {
      super(fileReader, block);
    }

    @Override
    public BloomFilter readBloomFilter(ColumnChunkMetaData meta) {
      return null;
    }
  }

  @Test
  public void testIntegerIn() {
    // only one value is present
    for (int i = 0; i < INT_VALUE_COUNT; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(
                  SCHEMA, in("id", INT_MIN_VALUE - 3 * i, INT_MIN_VALUE + i, INT_MAX_VALUE + 3 * i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: integer within range", shouldRead);
    }

    // all values are present
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                in(
                    "id",
                    IntStream.range(INT_MIN_VALUE - 10, INT_MAX_VALUE + 10)
                        .boxed()
                        .collect(Collectors.toList())))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: the bloom is a subset of the in set", shouldRead);

    // all values are present
    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                in(
                    "id",
                    IntStream.range(INT_MIN_VALUE, INT_MAX_VALUE)
                        .boxed()
                        .collect(Collectors.toList())))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: the bloom is equal to the in set", shouldRead);

    // no values are present
    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                in(
                    "id",
                    IntStream.range(INT_MIN_VALUE - 10, INT_MIN_VALUE - 1)
                        .boxed()
                        .collect(Collectors.toList())))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: value outside range", shouldRead);
  }

  @Test
  public void testOtherTypesIn() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, in("all_nulls", 1, 2))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: in on all nulls column (bloom is empty) ", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, in("some_nulls", "aaa", "some"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: in on some nulls column", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, in("some_nulls", "aaa", "bbb"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: some_nulls values are not within the set", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, in("no_nulls", "aaa", "bbb"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse(
        "Should not read: in on no nulls column (empty string is not within the set)", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, in("no_nulls", "aaa", ""))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue(
        "Should read: in on no nulls column (empty string is within the set)", shouldRead);
  }

  @Test
  public void testIntegerNotIn() {
    // only one value is present
    for (int i = 0; i < INT_VALUE_COUNT; i++) {
      boolean shouldRead =
          new ParquetBloomRowGroupFilter(
                  SCHEMA,
                  notIn("id", INT_MIN_VALUE - 3 * i, INT_MIN_VALUE + i, INT_MAX_VALUE + 3 * i))
              .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
    }

    // all values are present
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                notIn(
                    "id",
                    IntStream.range(INT_MIN_VALUE - 10, INT_MAX_VALUE + 10)
                        .boxed()
                        .collect(Collectors.toList())))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    // all values are present
    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                notIn(
                    "id",
                    IntStream.range(INT_MIN_VALUE, INT_MAX_VALUE)
                        .boxed()
                        .collect(Collectors.toList())))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    // no values are present
    shouldRead =
        new ParquetBloomRowGroupFilter(
                SCHEMA,
                notIn(
                    "id",
                    IntStream.range(INT_MIN_VALUE - 10, INT_MIN_VALUE - 1)
                        .boxed()
                        .collect(Collectors.toList())))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testOtherTypesNotIn() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notIn("all_nulls", 1, 2))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notIn("some_nulls", "aaa", "bbb"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notIn("no_nulls", "aaa", "bbb"))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, notIn("no_nulls", "aaa", ""))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testTypeConversions() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("long", LONG_BASE + INT_MIN_VALUE + 1), true)
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: Integer value promoted", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("long", LONG_BASE + INT_MIN_VALUE - 1), true)
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: Integer value promoted", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("id", (long) (INT_MIN_VALUE + 1)), true)
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: Long value truncated", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("id", (long) (INT_MIN_VALUE - 1)), true)
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: Long value truncated", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("id", ((long) Integer.MAX_VALUE) + 1), true)
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: Long value outside Integer range", shouldRead);
  }
}
