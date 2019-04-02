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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericFixed;
import org.apache.commons.io.Charsets;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Type;
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
import org.apache.iceberg.types.Types.UUIDType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.types.Conversions.fromByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestParquetMetrics {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private final UUID uuid = UUID.randomUUID();
  private final GenericFixed fixed = new GenericData.Fixed(
      org.apache.avro.Schema.createFixed("fixedCol", null, null, 4),
      "abcd".getBytes(Charsets.UTF_8));

  @Test
  public void testMetricsForTopLevelFields() throws IOException {
    Schema schema = new Schema(
        optional(1, "booleanCol", BooleanType.get()),
        required(2, "intCol", IntegerType.get()),
        optional(3, "longCol", LongType.get()),
        required(4, "floatCol", FloatType.get()),
        optional(5, "doubleCol", DoubleType.get()),
        optional(6, "decimalCol", DecimalType.of(10, 2)),
        required(7, "stringCol", StringType.get()),
        optional(8, "dateCol", DateType.get()),
        required(9, "timeCol", TimeType.get()),
        required(10, "timestampCol", TimestampType.withoutZone()),
        optional(11, "uuidCol", UUIDType.get()),
        required(12, "fixedCol", FixedType.ofLength(4)),
        required(13, "binaryCol", BinaryType.get())
    );

    Record firstRecord = new Record(AvroSchemaUtil.convert(schema.asStruct()));
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
    Record secondRecord = new Record(AvroSchemaUtil.convert(schema.asStruct()));
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

    File parquetFile = writeRecords(schema, firstRecord, secondRecord);

    Metrics metrics = ParquetMetrics.fromInputFile(localInput(parquetFile));
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
    assertBounds(10, TimestampType.withoutZone(), 0L, 1000L, metrics);
    assertCounts(11, 2L, 1L, metrics);
    assertBounds(11, UUIDType.get(), uuid, uuid, metrics);
    assertCounts(12, 2L, 0L, metrics);
    assertBounds(12, FixedType.ofLength(4),
        ByteBuffer.wrap(fixed.bytes()), ByteBuffer.wrap(fixed.bytes()), metrics);
    assertCounts(13, 2L, 0L, metrics);
    assertBounds(13, BinaryType.get(),
        ByteBuffer.wrap("S".getBytes()), ByteBuffer.wrap("W".getBytes()), metrics);
  }

  @Test
  public void testMetricsForDecimals() throws IOException {
    Schema schema = new Schema(
        required(1, "decimalAsInt32", DecimalType.of(4, 2)),
        required(2, "decimalAsInt64", DecimalType.of(14, 2)),
        required(3, "decimalAsFixed", DecimalType.of(22, 2))
    );

    Record record = new Record(AvroSchemaUtil.convert(schema.asStruct()));
    record.put("decimalAsInt32", new BigDecimal("2.55"));
    record.put("decimalAsInt64", new BigDecimal("4.75"));
    record.put("decimalAsFixed", new BigDecimal("5.80"));

    File parquetFile = writeRecords(schema, record);

    Metrics metrics = ParquetMetrics.fromInputFile(localInput(parquetFile));
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
    StructType leafStructType = StructType.of(
        optional(5, "leafLongCol", LongType.get()),
        optional(6, "leafBinaryCol", BinaryType.get())
    );
    StructType nestedStructType = StructType.of(
        required(3, "longCol", LongType.get()),
        required(4, "leafStructCol", leafStructType)
    );
    Schema schema = new Schema(
        required(1, "intCol", IntegerType.get()),
        required(2, "nestedStructCol", nestedStructType)
    );

    Record leafStruct = new Record(AvroSchemaUtil.convert(leafStructType));
    leafStruct.put("leafLongCol", 20L);
    leafStruct.put("leafBinaryCol", "A".getBytes());
    Record nestedStruct = new Record(AvroSchemaUtil.convert(nestedStructType));
    nestedStruct.put("longCol", 100L);
    nestedStruct.put("leafStructCol", leafStruct);
    Record record = new Record(AvroSchemaUtil.convert(schema.asStruct()));
    record.put("intCol", Integer.MAX_VALUE);
    record.put("nestedStructCol", nestedStruct);

    File parquetFile = writeRecords(schema, record);

    Metrics metrics = ParquetMetrics.fromInputFile(localInput(parquetFile));
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

    Record record = new Record(AvroSchemaUtil.convert(schema.asStruct()));
    record.put("intListCol", Lists.newArrayList(10, 11, 12));
    Record struct = new Record(AvroSchemaUtil.convert(structType));
    struct.put("leafIntCol", 1);
    struct.put("leafStringCol", "BBB");
    Map<String, Record> map = Maps.newHashMap();
    map.put("4", struct);
    record.put(1, map);

    File parquetFile = writeRecords(schema, record);

    Metrics metrics = ParquetMetrics.fromInputFile(localInput(parquetFile));
    Assert.assertEquals(1L, (long) metrics.recordCount());
    assertCounts(1, 1, 0, metrics);
    assertBounds(1, IntegerType.get(), null, null, metrics);
    assertCounts(2, 1, 0, metrics);
    assertBounds(2, StringType.get(), null, null, metrics);
    assertCounts(4, 3, 0, metrics);
    assertBounds(4, IntegerType.get(), null, null, metrics);
    assertCounts(6, 1, 0, metrics);
    assertBounds(6, StringType.get(), null, null, metrics);
  }

  @Test
  public void testMetricsForNullColumns() throws IOException {
    Schema schema = new Schema(
        optional(1, "intCol", IntegerType.get())
    );
    Record firstRecord = new Record(AvroSchemaUtil.convert(schema.asStruct()));
    firstRecord.put("intCol", null);
    Record secondRecord = new Record(AvroSchemaUtil.convert(schema.asStruct()));
    secondRecord.put("intCol", null);

    File parquetFile = writeRecords(schema, firstRecord, secondRecord);

    Metrics metrics = ParquetMetrics.fromInputFile(localInput(parquetFile));
    Assert.assertEquals(2L, (long) metrics.recordCount());
    assertCounts(1, 2, 2, metrics);
    assertBounds(1, IntegerType.get(), null, null, metrics);
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
        lowerBounds.containsKey(fieldId) ? fromByteBuffer(type, lowerBounds.get(fieldId)) : null);
    Assert.assertEquals(
        upperBound,
        upperBounds.containsKey(fieldId) ? fromByteBuffer(type, upperBounds.get(fieldId)) : null);
  }

  private File writeRecords(Schema schema, Record... records) throws IOException {
    File tmpFolder = temp.newFolder("parquet");
    String filename = UUID.randomUUID().toString();
    File file = new File(tmpFolder, FileFormat.PARQUET.addExtension(filename));
    try (FileAppender<Record> writer = Parquet.write(localOutput(file))
        .schema(schema)
        .build()) {
      writer.addAll(Lists.newArrayList(records));
    }
    return file;
  }
}
