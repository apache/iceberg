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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.types.Conversions.fromByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
/**
 * Tests for Metrics.
 */
public abstract class TestMetrics {

  private static final StructType LEAF_STRUCT_TYPE = StructType.of(
      optional(5, "leafLongCol", LongType.get()),
      optional(6, "leafBinaryCol", BinaryType.get())
  );

  private static final StructType NESTED_STRUCT_TYPE = StructType.of(
      required(3, "longCol", LongType.get()),
      required(4, "leafStructCol", LEAF_STRUCT_TYPE)
  );

  private static final Schema NESTED_SCHEMA = new Schema(
      required(1, "intCol", IntegerType.get()),
      required(2, "nestedStructCol", NESTED_STRUCT_TYPE)
  );

  private static final Schema SIMPLE_SCHEMA = new Schema(
      optional(1, "booleanCol", BooleanType.get()),
      required(2, "intCol", IntegerType.get()),
      optional(3, "longCol", LongType.get()),
      required(4, "floatCol", FloatType.get()),
      optional(5, "doubleCol", DoubleType.get()),
      optional(6, "decimalCol", DecimalType.of(10, 2)),
      required(7, "stringCol", StringType.get()),
      optional(8, "dateCol", DateType.get()),
      required(9, "timeCol", TimeType.get()),
      required(10, "timestampColAboveEpoch", TimestampType.withoutZone()),
      required(11, "fixedCol", FixedType.ofLength(4)),
      required(12, "binaryCol", BinaryType.get()),
      required(13, "timestampColBelowEpoch", TimestampType.withoutZone())
  );

  private final byte[] fixed = "abcd".getBytes(StandardCharsets.UTF_8);

  public abstract FileFormat fileFormat();

  public abstract Metrics getMetrics(InputFile file);

  public abstract InputFile writeRecords(Schema schema, Record... records) throws IOException;

  public abstract InputFile writeRecordsWithSmallRowGroups(Schema schema, Record... records)
      throws IOException;

  public abstract int splitCount(InputFile inputFile) throws IOException;

  public boolean supportsSmallRowGroups() {
    return false;
  }

  @Test
  public void testMetricsForRepeatedValues() throws IOException {
    Record firstRecord = GenericRecord.create(SIMPLE_SCHEMA);
    firstRecord.setField("booleanCol", true);
    firstRecord.setField("intCol", 3);
    firstRecord.setField("longCol", null);
    firstRecord.setField("floatCol", 2.0F);
    firstRecord.setField("doubleCol", 2.0D);
    firstRecord.setField("decimalCol", new BigDecimal("3.50"));
    firstRecord.setField("stringCol", "AAA");
    firstRecord.setField("dateCol", DateTimeUtil.dateFromDays(1500));
    firstRecord.setField("timeCol", DateTimeUtil.timeFromMicros(2000L));
    firstRecord.setField("timestampColAboveEpoch", DateTimeUtil.timestampFromMicros(0L));
    firstRecord.setField("fixedCol", fixed);
    firstRecord.setField("binaryCol", ByteBuffer.wrap("S".getBytes()));
    firstRecord.setField("timestampColBelowEpoch", DateTimeUtil.timestampFromMicros(0L));
    Record secondRecord = GenericRecord.create(SIMPLE_SCHEMA);
    secondRecord.setField("booleanCol", true);
    secondRecord.setField("intCol", 3);
    secondRecord.setField("longCol", null);
    secondRecord.setField("floatCol", 2.0F);
    secondRecord.setField("doubleCol", 2.0D);
    secondRecord.setField("decimalCol", new BigDecimal("3.50"));
    secondRecord.setField("stringCol", "AAA");
    secondRecord.setField("dateCol", DateTimeUtil.dateFromDays(1500));
    secondRecord.setField("timeCol", DateTimeUtil.timeFromMicros(2000L));
    secondRecord.setField("timestampColAboveEpoch", DateTimeUtil.timestampFromMicros(0L));
    secondRecord.setField("fixedCol", fixed);
    secondRecord.setField("binaryCol", ByteBuffer.wrap("S".getBytes()));
    secondRecord.setField("timestampColBelowEpoch", DateTimeUtil.timestampFromMicros(0L));

    InputFile recordsFile = writeRecords(SIMPLE_SCHEMA, firstRecord, secondRecord);

    Metrics metrics = getMetrics(recordsFile);
    Assert.assertEquals(2L, (long) metrics.recordCount());
    assertCounts(1, 2L, 0L, metrics);
    assertCounts(2, 2L, 0L, metrics);
    assertCounts(3, 2L, 2L, metrics);
    assertCounts(4, 2L, 0L, metrics);
    assertCounts(5, 2L, 0L, metrics);
    assertCounts(6, 2L, 0L, metrics);
    assertCounts(7, 2L, 0L, metrics);
    assertCounts(8, 2L, 0L, metrics);
    assertCounts(9, 2L, 0L, metrics);
    assertCounts(10, 2L, 0L, metrics);
    assertCounts(11, 2L, 0L, metrics);
    assertCounts(12, 2L, 0L, metrics);
    assertCounts(13, 2L, 0L, metrics);
  }

  @Test
  public void testMetricsForTopLevelFields() throws IOException {
    Record firstRecord = GenericRecord.create(SIMPLE_SCHEMA);
    firstRecord.setField("booleanCol", true);
    firstRecord.setField("intCol", 3);
    firstRecord.setField("longCol", 5L);
    firstRecord.setField("floatCol", 2.0F);
    firstRecord.setField("doubleCol", 2.0D);
    firstRecord.setField("decimalCol", new BigDecimal("3.50"));
    firstRecord.setField("stringCol", "AAA");
    firstRecord.setField("dateCol", DateTimeUtil.dateFromDays(1500));
    firstRecord.setField("timeCol", DateTimeUtil.timeFromMicros(2000L));
    firstRecord.setField("timestampColAboveEpoch", DateTimeUtil.timestampFromMicros(0L));
    firstRecord.setField("fixedCol", fixed);
    firstRecord.setField("binaryCol", ByteBuffer.wrap("S".getBytes()));
    firstRecord.setField("timestampColBelowEpoch", DateTimeUtil.timestampFromMicros(-1_900_300L));
    Record secondRecord = GenericRecord.create(SIMPLE_SCHEMA);
    secondRecord.setField("booleanCol", false);
    secondRecord.setField("intCol", Integer.MIN_VALUE);
    secondRecord.setField("longCol", null);
    secondRecord.setField("floatCol", 1.0F);
    secondRecord.setField("doubleCol", null);
    secondRecord.setField("decimalCol", null);
    secondRecord.setField("stringCol", "ZZZ");
    secondRecord.setField("dateCol", null);
    secondRecord.setField("timeCol", DateTimeUtil.timeFromMicros(3000L));
    secondRecord.setField("timestampColAboveEpoch", DateTimeUtil.timestampFromMicros(900L));
    secondRecord.setField("fixedCol", fixed);
    secondRecord.setField("binaryCol", ByteBuffer.wrap("W".getBytes()));
    secondRecord.setField("timestampColBelowEpoch", DateTimeUtil.timestampFromMicros(0L));

    InputFile recordsFile = writeRecords(SIMPLE_SCHEMA, firstRecord, secondRecord);

    Metrics metrics = getMetrics(recordsFile);
    Assert.assertEquals(2L, (long) metrics.recordCount());
    assertCounts(1, 2L, 0L, metrics);
    assertBounds(1, BooleanType.get(), false, true, metrics);
    assertCounts(2, 2L, 0L, metrics);
    assertBounds(2, IntegerType.get(), Integer.MIN_VALUE, 3, metrics);
    assertCounts(3, 2L, 1L, metrics);
    assertBounds(3, LongType.get(), 5L, 5L, metrics);
    assertCounts(4, 2L, 0L, metrics);
    assertBounds(4, FloatType.get(), 1.0F, 2.0F, metrics);
    assertCounts(5, 2L, 1L, metrics);
    assertBounds(5, DoubleType.get(), 2.0D, 2.0D, metrics);
    assertCounts(6, 2L, 1L, metrics);
    assertBounds(6, DecimalType.of(10, 2), new BigDecimal("3.50"), new BigDecimal("3.50"), metrics);
    assertCounts(7, 2L, 0L, metrics);
    assertBounds(7, StringType.get(), CharBuffer.wrap("AAA"), CharBuffer.wrap("ZZZ"), metrics);
    assertCounts(8, 2L, 1L, metrics);
    assertBounds(8, DateType.get(), 1500, 1500, metrics);
    assertCounts(9, 2L, 0L, metrics);
    assertBounds(9, TimeType.get(), 2000L, 3000L, metrics);
    assertCounts(10, 2L, 0L, metrics);
    if (fileFormat() == FileFormat.ORC) {
      // ORC-611: ORC only supports millisecond precision, so we adjust by 1 millisecond
      assertBounds(10, TimestampType.withoutZone(), 0L, 1000L, metrics);
    } else {
      assertBounds(10, TimestampType.withoutZone(), 0L, 900L, metrics);
    }
    assertCounts(11, 2L, 0L, metrics);
    assertBounds(11, FixedType.ofLength(4),
        ByteBuffer.wrap(fixed), ByteBuffer.wrap(fixed), metrics);
    assertCounts(12, 2L, 0L, metrics);
    assertBounds(12, BinaryType.get(),
        ByteBuffer.wrap("S".getBytes()), ByteBuffer.wrap("W".getBytes()), metrics);
    if (fileFormat() == FileFormat.ORC) {
      // TODO: enable when ORC-342 is fixed - ORC-342: creates inaccurate timestamp/stats below epoch
      // ORC-611: ORC only supports millisecond precision, so we adjust by 1 millisecond, e.g.
      // assertBounds(13, TimestampType.withoutZone(), -1000L, 1000L, metrics); would fail for a value
      // in the range `[1970-01-01 00:00:00.000,1970-01-01 00:00:00.999]`
      assertBounds(13, TimestampType.withoutZone(), -1_901_000L, 1000L, metrics);
    } else {
      assertBounds(13, TimestampType.withoutZone(), -1_900_300L, 0L, metrics);
    }
  }

  @Test
  public void testMetricsForDecimals() throws IOException {
    Schema schema = new Schema(
        required(1, "decimalAsInt32", DecimalType.of(4, 2)),
        required(2, "decimalAsInt64", DecimalType.of(14, 2)),
        required(3, "decimalAsFixed", DecimalType.of(22, 2))
    );

    Record record = GenericRecord.create(schema);
    record.setField("decimalAsInt32", new BigDecimal("2.55"));
    record.setField("decimalAsInt64", new BigDecimal("4.75"));
    record.setField("decimalAsFixed", new BigDecimal("5.80"));

    InputFile recordsFile = writeRecords(schema, record);

    Metrics metrics = getMetrics(recordsFile);
    Assert.assertEquals(1L, (long) metrics.recordCount());
    assertCounts(1, 1L, 0L, metrics);
    assertBounds(1, DecimalType.of(4, 2), new BigDecimal("2.55"), new BigDecimal("2.55"), metrics);
    assertCounts(2, 1L, 0L, metrics);
    assertBounds(2, DecimalType.of(14, 2), new BigDecimal("4.75"), new BigDecimal("4.75"), metrics);
    assertCounts(3, 1L, 0L, metrics);
    assertBounds(3, DecimalType.of(22, 2), new BigDecimal("5.80"), new BigDecimal("5.80"), metrics);
  }

  @Test
  public void testMetricsForNestedStructFields() throws IOException {

    Record leafStruct = GenericRecord.create(LEAF_STRUCT_TYPE);
    leafStruct.setField("leafLongCol", 20L);
    leafStruct.setField("leafBinaryCol", ByteBuffer.wrap("A".getBytes()));
    Record nestedStruct = GenericRecord.create(NESTED_STRUCT_TYPE);
    nestedStruct.setField("longCol", 100L);
    nestedStruct.setField("leafStructCol", leafStruct);
    Record record = GenericRecord.create(NESTED_SCHEMA);
    record.setField("intCol", Integer.MAX_VALUE);
    record.setField("nestedStructCol", nestedStruct);

    InputFile recordsFile = writeRecords(NESTED_SCHEMA, record);

    Metrics metrics = getMetrics(recordsFile);
    Assert.assertEquals(1L, (long) metrics.recordCount());
    assertCounts(1, 1L, 0L, metrics);
    assertBounds(1, IntegerType.get(), Integer.MAX_VALUE, Integer.MAX_VALUE, metrics);
    assertCounts(3, 1L, 0L, metrics);
    assertBounds(3, LongType.get(), 100L, 100L, metrics);
    assertCounts(5, 1L, 0L, metrics);
    assertBounds(5, LongType.get(), 20L, 20L, metrics);
    assertCounts(6, 1L, 0L, metrics);
    assertBounds(6, BinaryType.get(),
        ByteBuffer.wrap("A".getBytes()), ByteBuffer.wrap("A".getBytes()), metrics);
  }

  @Test
  public void testMetricsForListAndMapElements() throws IOException {
    StructType structType = StructType.of(
        required(1, "leafIntCol", IntegerType.get()),
        optional(2, "leafStringCol", StringType.get())
    );
    Schema schema = new Schema(
        optional(3, "intListCol", ListType.ofRequired(4, IntegerType.get())),
        optional(5, "mapCol", MapType.ofRequired(6, 7, StringType.get(), structType))
    );

    Record record = GenericRecord.create(schema);
    record.setField("intListCol", Lists.newArrayList(10, 11, 12));
    Record struct = GenericRecord.create(structType);
    struct.setField("leafIntCol", 1);
    struct.setField("leafStringCol", "BBB");
    Map<String, Record> map = Maps.newHashMap();
    map.put("4", struct);
    record.set(1, map);

    InputFile recordsFile = writeRecords(schema, record);

    Metrics metrics = getMetrics(recordsFile);
    Assert.assertEquals(1L, (long) metrics.recordCount());
    if (fileFormat() != FileFormat.ORC) {
      assertCounts(1, 1L, 0L, metrics);
      assertCounts(2, 1L, 0L, metrics);
      assertCounts(4, 3L, 0L, metrics);
      assertCounts(6, 1L, 0L, metrics);
    } else {
      assertCounts(1, null, null, metrics);
      assertCounts(2, null, null, metrics);
      assertCounts(4, null, null, metrics);
      assertCounts(6, null, null, metrics);
    }
    assertBounds(1, IntegerType.get(), null, null, metrics);
    assertBounds(2, StringType.get(), null, null, metrics);
    assertBounds(4, IntegerType.get(), null, null, metrics);
    assertBounds(6, StringType.get(), null, null, metrics);
    assertBounds(7, structType, null, null, metrics);
  }

  @Test
  public void testMetricsForNullColumns() throws IOException {
    Schema schema = new Schema(
        optional(1, "intCol", IntegerType.get())
    );
    Record firstRecord = GenericRecord.create(schema);
    firstRecord.setField("intCol", null);
    Record secondRecord = GenericRecord.create(schema);
    secondRecord.setField("intCol", null);

    InputFile recordsFile = writeRecords(schema, firstRecord, secondRecord);

    Metrics metrics = getMetrics(recordsFile);
    Assert.assertEquals(2L, (long) metrics.recordCount());
    assertCounts(1, 2L, 2L, metrics);
    assertBounds(1, IntegerType.get(), null, null, metrics);
  }

  @Test
  public void testMetricsForTopLevelWithMultipleRowGroup() throws Exception {
    Assume.assumeTrue("Skip test for formats that do not support small row groups", supportsSmallRowGroups());

    int recordCount = 201;
    List<Record> records = new ArrayList<>(recordCount);

    for (int i = 0; i < recordCount; i++) {
      Record newRecord = GenericRecord.create(SIMPLE_SCHEMA);
      newRecord.setField("booleanCol", i == 0 ? false : true);
      newRecord.setField("intCol", i + 1);
      newRecord.setField("longCol", i == 0 ? null : i + 1L);
      newRecord.setField("floatCol", i + 1.0F);
      newRecord.setField("doubleCol", i == 0 ? null : i + 1.0D);
      newRecord.setField("decimalCol", i == 0 ? null : new BigDecimal(i + "").add(new BigDecimal("1.00")));
      newRecord.setField("stringCol", "AAA");
      newRecord.setField("dateCol", DateTimeUtil.dateFromDays(i + 1));
      newRecord.setField("timeCol", DateTimeUtil.timeFromMicros(i + 1L));
      newRecord.setField("timestampColAboveEpoch", DateTimeUtil.timestampFromMicros(i + 1L));
      newRecord.setField("fixedCol", fixed);
      newRecord.setField("binaryCol", ByteBuffer.wrap("S".getBytes()));
      newRecord.setField("timestampColBelowEpoch", DateTimeUtil.timestampFromMicros((i + 1L) * -1L));
      records.add(newRecord);
    }

    // create file with multiple row groups. by using smaller number of bytes
    InputFile recordsFile = writeRecordsWithSmallRowGroups(SIMPLE_SCHEMA, records.toArray(new Record[0]));

    Assert.assertNotNull(recordsFile);
    // rowgroup size should be > 1
    Assert.assertEquals(3, splitCount(recordsFile));

    Metrics metrics = getMetrics(recordsFile);
    Assert.assertEquals(201L, (long) metrics.recordCount());
    assertCounts(1, 201L, 0L, metrics);
    assertBounds(1, Types.BooleanType.get(), false, true, metrics);
    assertBounds(2, Types.IntegerType.get(), 1, 201, metrics);
    assertCounts(3, 201L, 1L, metrics);
    assertBounds(3, Types.LongType.get(), 2L, 201L, metrics);
    assertCounts(4, 201L, 0L, metrics);
    assertBounds(4, Types.FloatType.get(), 1.0F, 201.0F, metrics);
    assertCounts(5, 201L, 1L, metrics);
    assertBounds(5, Types.DoubleType.get(), 2.0D, 201.0D, metrics);
    assertCounts(6, 201L, 1L, metrics);
    assertBounds(6, Types.DecimalType.of(10, 2), new BigDecimal("2.00"),
        new BigDecimal("201.00"), metrics);
  }

  @Test
  public void testMetricsForNestedStructFieldsWithMultipleRowGroup() throws IOException {
    Assume.assumeTrue("Skip test for formats that do not support small row groups", supportsSmallRowGroups());

    int recordCount = 201;
    List<Record> records = Lists.newArrayListWithExpectedSize(recordCount);

    for (int i = 0; i < recordCount; i++) {
      Record newLeafStruct = GenericRecord.create(LEAF_STRUCT_TYPE);
      newLeafStruct.setField("leafLongCol", i + 1L);
      newLeafStruct.setField("leafBinaryCol", ByteBuffer.wrap("A".getBytes()));
      Record newNestedStruct = GenericRecord.create(NESTED_STRUCT_TYPE);
      newNestedStruct.setField("longCol", i + 1L);
      newNestedStruct.setField("leafStructCol", newLeafStruct);
      Record newRecord = GenericRecord.create(NESTED_SCHEMA);
      newRecord.setField("intCol", i + 1);
      newRecord.setField("nestedStructCol", newNestedStruct);
      records.add(newRecord);
    }

    // create file with multiple row groups. by using smaller number of bytes
    InputFile recordsFile = writeRecordsWithSmallRowGroups(NESTED_SCHEMA, records.toArray(new Record[0]));

    Assert.assertNotNull(recordsFile);
    // rowgroup size should be > 1
    Assert.assertEquals(3, splitCount(recordsFile));

    Metrics metrics = getMetrics(recordsFile);
    Assert.assertEquals(201L, (long) metrics.recordCount());
    assertCounts(1, 201L, 0L, metrics);
    assertBounds(1, IntegerType.get(), 1, 201, metrics);
    assertCounts(3, 201L, 0L, metrics);
    assertBounds(3, LongType.get(), 1L, 201L, metrics);
    assertCounts(5, 201L, 0L, metrics);
    assertBounds(5, LongType.get(), 1L, 201L, metrics);
    assertCounts(6, 201L, 0L, metrics);
    assertBounds(6, BinaryType.get(),
        ByteBuffer.wrap("A".getBytes()), ByteBuffer.wrap("A".getBytes()), metrics);
  }

  private void assertCounts(int fieldId, Long valueCount, Long nullValueCount, Metrics metrics) {
    Map<Integer, Long> valueCounts = metrics.valueCounts();
    Map<Integer, Long> nullValueCounts = metrics.nullValueCounts();
    Assert.assertEquals(valueCount, valueCounts.get(fieldId));
    Assert.assertEquals(nullValueCount, nullValueCounts.get(fieldId));
  }

  protected <T> void assertBounds(int fieldId, Type type, T lowerBound, T upperBound, Metrics metrics) {
    Map<Integer, ByteBuffer> lowerBounds = metrics.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = metrics.upperBounds();

    Assert.assertEquals(
        lowerBound,
        lowerBounds.containsKey(fieldId) ? fromByteBuffer(type, lowerBounds.get(fieldId)) : null);
    Assert.assertEquals(
        upperBound,
        upperBounds.containsKey(fieldId) ? fromByteBuffer(type, upperBounds.get(fieldId)) : null);
  }

}
