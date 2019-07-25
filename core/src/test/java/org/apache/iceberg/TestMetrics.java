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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Tests for Metrics.
 */
public abstract class TestMetrics {

  private final UUID uuid = UUID.randomUUID();
  private final GenericFixed fixed = new GenericData.Fixed(
      org.apache.avro.Schema.createFixed("fixedCol", null, null, 4),
      "abcd".getBytes(StandardCharsets.UTF_8));

  public abstract Metrics getMetrics(InputFile file);

  public abstract File writeRecords(Schema schema, GenericData.Record... records) throws IOException;

  @Test
  public void testMetricsForTopLevelFields() throws IOException {
    Schema schema = new Schema(
        optional(1, "booleanCol", Types.BooleanType.get()),
        required(2, "intCol", Types.IntegerType.get()),
        optional(3, "longCol", Types.LongType.get()),
        required(4, "floatCol", Types.FloatType.get()),
        optional(5, "doubleCol", Types.DoubleType.get()),
        optional(6, "decimalCol", Types.DecimalType.of(10, 2)),
        required(7, "stringCol", Types.StringType.get()),
        optional(8, "dateCol", Types.DateType.get()),
        required(9, "timeCol", Types.TimeType.get()),
        required(10, "timestampCol", Types.TimestampType.withoutZone()),
        optional(11, "uuidCol", Types.UUIDType.get()),
        required(12, "fixedCol", Types.FixedType.ofLength(4)),
        required(13, "binaryCol", Types.BinaryType.get())
    );

    GenericData.Record firstRecord = new GenericData.Record(AvroSchemaUtil.convert(schema.asStruct()));
    firstRecord.put("booleanCol", true);
    firstRecord.put("intCol", 3);
    firstRecord.put("longCol", 5L);
    firstRecord.put("floatCol", 2.0F);
    firstRecord.put("doubleCol", 2.0D);
    firstRecord.put("decimalCol", new BigDecimal("3.50"));
    firstRecord.put("stringCol", "AAA");
    firstRecord.put("dateCol", 1500);
    firstRecord.put("timeCol", 2000L);
    firstRecord.put("timestampCol", 0L);
    firstRecord.put("uuidCol", uuid);
    firstRecord.put("fixedCol", fixed);
    firstRecord.put("binaryCol", "S".getBytes());

    GenericData.Record secondRecord = new GenericData.Record(AvroSchemaUtil.convert(schema.asStruct()));
    secondRecord.put("booleanCol", false);
    secondRecord.put("intCol", Integer.MIN_VALUE);
    secondRecord.put("longCol", null);
    secondRecord.put("floatCol", 1.0F);
    secondRecord.put("doubleCol", null);
    secondRecord.put("decimalCol", null);
    secondRecord.put("stringCol", "ZZZ");
    secondRecord.put("dateCol", null);
    secondRecord.put("timeCol", 3000L);
    secondRecord.put("timestampCol", 1000L);
    secondRecord.put("uuidCol", null);
    secondRecord.put("fixedCol", fixed);
    secondRecord.put("binaryCol", "W".getBytes());

    File recordsFile = writeRecords(schema, firstRecord, secondRecord);

    Metrics metrics = getMetrics(Files.localInput(recordsFile));
    Assert.assertEquals(2L, (long) metrics.recordCount());
    assertCounts(1, 2L, 0L, metrics);
    assertBounds(1, Types.BooleanType.get(), false, true, metrics);
    assertCounts(2, 2L, 0L, metrics);
    assertBounds(2, Types.IntegerType.get(), Integer.MIN_VALUE, 3, metrics);
    assertCounts(3, 2L, 1L, metrics);
    assertBounds(3, Types.LongType.get(), 5L, 5L, metrics);
    assertCounts(4, 2L, 0L, metrics);
    assertBounds(4, Types.FloatType.get(), 1.0F, 2.0F, metrics);
    assertCounts(5, 2L, 1L, metrics);
    assertBounds(5, Types.DoubleType.get(), 2.0D, 2.0D, metrics);
    assertCounts(6, 2L, 1L, metrics);
    assertBounds(6, Types.DecimalType.of(10, 2), new BigDecimal("3.50"), new BigDecimal("3.50"), metrics);
    assertCounts(7, 2L, 0L, metrics);
    assertBounds(7, Types.StringType.get(), CharBuffer.wrap("AAA"), CharBuffer.wrap("ZZZ"), metrics);
    assertCounts(8, 2L, 1L, metrics);
    assertBounds(8, Types.DateType.get(), 1500, 1500, metrics);
    assertCounts(9, 2L, 0L, metrics);
    assertBounds(9, Types.TimeType.get(), 2000L, 3000L, metrics);
    assertCounts(10, 2L, 0L, metrics);
    assertBounds(10, Types.TimestampType.withoutZone(), 0L, 1000L, metrics);
    assertCounts(11, 2L, 1L, metrics);
    assertBounds(11, Types.UUIDType.get(), uuid, uuid, metrics);
    assertCounts(12, 2L, 0L, metrics);
    assertBounds(12, Types.FixedType.ofLength(4),
        ByteBuffer.wrap(fixed.bytes()), ByteBuffer.wrap(fixed.bytes()), metrics);
    assertCounts(13, 2L, 0L, metrics);
    assertBounds(13, Types.BinaryType.get(),
        ByteBuffer.wrap("S".getBytes()), ByteBuffer.wrap("W".getBytes()), metrics);
  }

  @Test
  public void testMetricsForDecimals() throws IOException {
    Schema schema = new Schema(
        required(1, "decimalAsInt32", Types.DecimalType.of(4, 2)),
        required(2, "decimalAsInt64", Types.DecimalType.of(14, 2)),
        required(3, "decimalAsFixed", Types.DecimalType.of(22, 2))
    );

    GenericData.Record record = new GenericData.Record(AvroSchemaUtil.convert(schema.asStruct()));
    record.put("decimalAsInt32", new BigDecimal("2.55"));
    record.put("decimalAsInt64", new BigDecimal("4.75"));
    record.put("decimalAsFixed", new BigDecimal("5.80"));

    File recordsFile = writeRecords(schema, record);

    Metrics metrics = getMetrics(Files.localInput(recordsFile));
    Assert.assertEquals(1L, (long) metrics.recordCount());
    assertCounts(1, 1L, 0L, metrics);
    assertBounds(1, Types.DecimalType.of(4, 2), new BigDecimal("2.55"), new BigDecimal("2.55"), metrics);
    assertCounts(2, 1L, 0L, metrics);
    assertBounds(2, Types.DecimalType.of(14, 2), new BigDecimal("4.75"), new BigDecimal("4.75"), metrics);
    assertCounts(3, 1L, 0L, metrics);
    assertBounds(3, Types.DecimalType.of(22, 2), new BigDecimal("5.80"), new BigDecimal("5.80"), metrics);
  }

  @Test
  public void testMetricsForNestedStructFields() throws IOException {
    Types.StructType leafStructType = Types.StructType.of(
        optional(5, "leafLongCol", Types.LongType.get()),
        optional(6, "leafBinaryCol", Types.BinaryType.get())
    );
    Types.StructType nestedStructType = Types.StructType.of(
        required(3, "longCol", Types.LongType.get()),
        required(4, "leafStructCol", leafStructType)
    );
    Schema schema = new Schema(
        required(1, "intCol", Types.IntegerType.get()),
        required(2, "nestedStructCol", nestedStructType)
    );

    GenericData.Record leafStruct = new GenericData.Record(AvroSchemaUtil.convert(leafStructType));
    leafStruct.put("leafLongCol", 20L);
    leafStruct.put("leafBinaryCol", "A".getBytes());
    GenericData.Record nestedStruct = new GenericData.Record(AvroSchemaUtil.convert(nestedStructType));
    nestedStruct.put("longCol", 100L);
    nestedStruct.put("leafStructCol", leafStruct);
    GenericData.Record record = new GenericData.Record(AvroSchemaUtil.convert(schema.asStruct()));
    record.put("intCol", Integer.MAX_VALUE);
    record.put("nestedStructCol", nestedStruct);

    File recordsFile = writeRecords(schema, record);

    Metrics metrics = getMetrics(Files.localInput(recordsFile));
    Assert.assertEquals(1L, (long) metrics.recordCount());
    assertCounts(1, 1L, 0L, metrics);
    assertBounds(1, Types.IntegerType.get(), Integer.MAX_VALUE, Integer.MAX_VALUE, metrics);
    assertCounts(3, 1L, 0L, metrics);
    assertBounds(3, Types.LongType.get(), 100L, 100L, metrics);
    assertCounts(5, 1L, 0L, metrics);
    assertBounds(5, Types.LongType.get(), 20L, 20L, metrics);
    assertCounts(6, 1L, 0L, metrics);
    assertBounds(6, Types.BinaryType.get(),
        ByteBuffer.wrap("A".getBytes()), ByteBuffer.wrap("A".getBytes()), metrics);
  }

  @Test
  public void testMetricsForListAndMapElements() throws IOException {
    Types.StructType structType = Types.StructType.of(
        required(1, "leafIntCol", Types.IntegerType.get()),
        optional(2, "leafStringCol", Types.StringType.get())
    );
    Schema schema = new Schema(
        optional(3, "intListCol", Types.ListType.ofRequired(4, Types.IntegerType.get())),
        optional(5, "mapCol", Types.MapType.ofRequired(6, 7, Types.StringType.get(), structType))
    );

    GenericData.Record record = new GenericData.Record(AvroSchemaUtil.convert(schema.asStruct()));
    record.put("intListCol", Lists.newArrayList(10, 11, 12));
    GenericData.Record struct = new GenericData.Record(AvroSchemaUtil.convert(structType));
    struct.put("leafIntCol", 1);
    struct.put("leafStringCol", "BBB");
    Map<String, GenericData.Record> map = Maps.newHashMap();
    map.put("4", struct);
    record.put(1, map);

    File recordsFile = writeRecords(schema, record);

    Metrics metrics = getMetrics(Files.localInput(recordsFile));
    Assert.assertEquals(1L, (long) metrics.recordCount());
    assertCounts(1, 1, 0, metrics);
    assertBounds(1, Types.IntegerType.get(), null, null, metrics);
    assertCounts(2, 1, 0, metrics);
    assertBounds(2, Types.StringType.get(), null, null, metrics);
    assertCounts(4, 3, 0, metrics);
    assertBounds(4, Types.IntegerType.get(), null, null, metrics);
    assertCounts(6, 1, 0, metrics);
    assertBounds(6, Types.StringType.get(), null, null, metrics);
  }

  @Test
  public void testMetricsForNullColumns() throws IOException {
    Schema schema = new Schema(
        optional(1, "intCol", Types.IntegerType.get())
    );
    GenericData.Record firstRecord = new GenericData.Record(AvroSchemaUtil.convert(schema.asStruct()));
    firstRecord.put("intCol", null);
    GenericData.Record secondRecord = new GenericData.Record(AvroSchemaUtil.convert(schema.asStruct()));
    secondRecord.put("intCol", null);

    File recordsFile = writeRecords(schema, firstRecord, secondRecord);

    Metrics metrics = getMetrics(Files.localInput(recordsFile));
    Assert.assertEquals(2L, (long) metrics.recordCount());
    assertCounts(1, 2, 2, metrics);
    assertBounds(1, Types.IntegerType.get(), null, null, metrics);
  }

  private void assertCounts(int fieldId, long valueCount, long nullValueCount, Metrics metrics) {
    Map<Integer, Long> valueCounts = metrics.valueCounts();
    Map<Integer, Long> nullValueCounts = metrics.nullValueCounts();
    Assert.assertEquals(valueCount, (long) valueCounts.get(fieldId));
    Assert.assertEquals(nullValueCount, (long) nullValueCounts.get(fieldId));
  }

  private <T> void assertBounds(int fieldId, Type type, T lowerBound, T upperBound, Metrics metrics) {
    Map<Integer, ByteBuffer> lowerBounds = metrics.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = metrics.upperBounds();

    Assert.assertEquals(
        lowerBound,
        lowerBounds.containsKey(fieldId) ?
            Conversions.fromByteBuffer(type, lowerBounds.get(fieldId)) : null);
    Assert.assertEquals(
        upperBound,
        upperBounds.containsKey(fieldId) ?
            Conversions.fromByteBuffer(type, upperBounds.get(fieldId)) : null);
  }
}
