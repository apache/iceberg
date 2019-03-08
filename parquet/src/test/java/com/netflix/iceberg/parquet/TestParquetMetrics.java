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

package com.netflix.iceberg.parquet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import com.netflix.iceberg.io.FileAppender;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericFixed;
import org.apache.commons.io.Charsets;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Map;
import java.util.UUID;

import static com.netflix.iceberg.Files.localInput;
import static com.netflix.iceberg.Files.localOutput;
import static com.netflix.iceberg.types.Conversions.fromByteBuffer;
import static com.netflix.iceberg.types.Types.*;
import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;

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
    checkFieldMetrics(1, schema, metrics, 2L, 0L, false, true);
    checkFieldMetrics(2, schema, metrics, 2L, 0L, Integer.MIN_VALUE, 3);
    checkFieldMetrics(3, schema, metrics, 2L, 1L, 5L, 5L);
    checkFieldMetrics(4, schema, metrics, 2L, 0L, 1.0F, 2.0F);
    checkFieldMetrics(5, schema, metrics, 2L, 1L, 2.0D, 2.0D);
    checkFieldMetrics(6, schema, metrics, 2L, 1L, new BigDecimal("3.50"), new BigDecimal("3.50"));
    checkFieldMetrics(7, schema, metrics, 2L, 0L, CharBuffer.wrap("AAA"), CharBuffer.wrap("ZZZ"));
    checkFieldMetrics(8, schema, metrics, 2L, 1L, 1500, 1500);
    checkFieldMetrics(9, schema, metrics, 2L, 0L, 2000L, 3000L);
    checkFieldMetrics(10, schema, metrics, 2L, 0L, 0L, 1000L);
    // TODO: enable once issue#126 is resolved
    // checkFieldMetrics(11, schema, metrics, 2L, 1L, uuid, uuid);
    checkFieldMetrics(12, schema, metrics, 2L,
      0L, ByteBuffer.wrap(fixed.bytes()), ByteBuffer.wrap(fixed.bytes()));
    checkFieldMetrics(13, schema, metrics,
      2L, 0L, ByteBuffer.wrap("S".getBytes()), ByteBuffer.wrap("W".getBytes()));
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
    checkFieldMetrics(1, schema, metrics, 1, 0, null, null);
    checkFieldMetrics(2, schema, metrics, 1, 0, null, null);
    checkFieldMetrics(4, schema, metrics, 3, 0, null, null);
    checkFieldMetrics(6, schema, metrics, 1, 0, null, null);
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
    checkFieldMetrics(1, schema, metrics, 2, 2, null, null);
  }

  private <T> void checkFieldMetrics(int fieldId, Schema schema, Metrics metrics,
                                     long expectedValueCount, long expectedNullValueCount,
                                     T expectedLowerBound, T expectedUpperBound) {
    NestedField field = schema.findField(fieldId);

    Map<Integer, Long> valueCounts = metrics.valueCounts();
    Map<Integer, Long> nullValueCounts = metrics.nullValueCounts();
    Map<Integer, ByteBuffer> lowerBounds = metrics.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = metrics.upperBounds();

    Assert.assertEquals(expectedValueCount, (long) valueCounts.get(field.fieldId()));
    Assert.assertEquals(expectedNullValueCount, (long) nullValueCounts.get(field.fieldId()));

    checkBound(field, expectedLowerBound, lowerBounds);
    checkBound(field, expectedUpperBound, upperBounds);
  }

  private <T> void checkBound(NestedField field, T expectedBound, Map<Integer, ByteBuffer> bounds) {
    ByteBuffer actualBound = bounds.get(field.fieldId());
    if (expectedBound == null) {
      Assert.assertNull(actualBound);
    } else {
      Assert.assertEquals(expectedBound, fromByteBuffer(field.type(), actualBound));
    }
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
