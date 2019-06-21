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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.types.Types.UUIDType;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.avro.AvroSchemaUtil.convert;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestMetricsRowGroupFilterTypes {
  private static final Schema SCHEMA = new Schema(
      optional(1, "boolean", BooleanType.get()),
      optional(2, "int", IntegerType.get()),
      optional(3, "long", LongType.get()),
      optional(4, "float", FloatType.get()),
      optional(5, "double", DoubleType.get()),
      optional(6, "date", DateType.get()),
      optional(7, "time", TimeType.get()),
      optional(8, "timestamp", TimestampType.withoutZone()),
      optional(9, "timestamptz", TimestampType.withZone()),
      optional(10, "string", StringType.get()),
      optional(11, "uuid", UUIDType.get()),
      optional(12, "fixed", FixedType.ofLength(4)),
      optional(13, "binary", BinaryType.get()),
      optional(14, "int_decimal", Types.DecimalType.of(8, 2)),
      optional(15, "long_decimal", Types.DecimalType.of(14, 2)),
      optional(16, "fixed_decimal", Types.DecimalType.of(31, 2))
  );

  private static final Schema FILE_SCHEMA = new Schema(
      optional(1, "_boolean", BooleanType.get()),
      optional(2, "_int", IntegerType.get()),
      optional(3, "_long", LongType.get()),
      optional(4, "_float", FloatType.get()),
      optional(5, "_double", DoubleType.get()),
      optional(6, "_date", DateType.get()),
      optional(7, "_time", TimeType.get()),
      optional(8, "_timestamp", TimestampType.withoutZone()),
      optional(9, "_timestamptz", TimestampType.withZone()),
      optional(10, "_string", StringType.get()),
      optional(11, "_uuid", UUIDType.get()),
      optional(12, "_fixed", FixedType.ofLength(4)),
      optional(13, "_binary", BinaryType.get()),
      optional(14, "_int_decimal", Types.DecimalType.of(8, 2)),
      optional(15, "_long_decimal", Types.DecimalType.of(14, 2)),
      optional(16, "_fixed_decimal", Types.DecimalType.of(31, 2))
  );

  private static final File PARQUET_FILE = new File("/tmp/stats-row-group-filter-types-test.parquet");
  private static MessageType PARQUET_SCHEMA = null;
  private static BlockMetaData ROW_GROUP_METADATA = null;

  private static final UUID uuid = UUID.randomUUID();
  private static final Integer date = (Integer) Literal.of("2018-06-29").to(DateType.get()).value();
  private static final Long time = (Long) Literal.of("10:02:34.000000").to(TimeType.get()).value();
  private static final Long timestamp = (Long) Literal.of("2018-06-29T10:02:34.000000")
      .to(TimestampType.withoutZone()).value();
  private static final GenericFixed fixed = new GenericData.Fixed(
      org.apache.avro.Schema.createFixed("_fixed", null, null, 4),
      "abcd".getBytes(StandardCharsets.UTF_8));

  @BeforeClass
  public static void createInputFile() throws IOException {
    if (PARQUET_FILE.exists()) {
      Assert.assertTrue(PARQUET_FILE.delete());
    }

    OutputFile outFile = Files.localOutput(PARQUET_FILE);
    try (FileAppender<GenericData.Record> appender = Parquet.write(outFile)
        .schema(FILE_SCHEMA)
        .build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(convert(FILE_SCHEMA, "table"));
      // create 50 records
      for (int i = 0; i < 50; i += 1) {
        builder.set("_boolean", false);
        builder.set("_int", i);
        builder.set("_long", 5_000_000_000L + i);
        builder.set("_float", ((float) (100 - i)) / 100F + 1.0F); // 2.0f, 1.99f, 1.98f, ...
        builder.set("_double", ((double) i) / 100.0D + 2.0D); // 2.0d, 2.01d, 2.02d, ...
        builder.set("_date", date);
        builder.set("_time", time);
        builder.set("_timestamp", timestamp);
        builder.set("_timestamptz", timestamp);
        builder.set("_string", "tapir");
        builder.set("_uuid", uuid);
        builder.set("_fixed", fixed);
        builder.set("_binary", ByteBuffer.wrap("xyz".getBytes(StandardCharsets.UTF_8)));
        builder.set("_int_decimal", new BigDecimal("77.77"));
        builder.set("_long_decimal", new BigDecimal("88.88"));
        builder.set("_fixed_decimal", new BigDecimal("99.99"));
        appender.add(builder.build());
      }
    }

    InputFile inFile = Files.localInput(PARQUET_FILE);
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inFile))) {
      Assert.assertEquals("Should create only one row group", 1, reader.getRowGroups().size());
      ROW_GROUP_METADATA = reader.getRowGroups().get(0);
      PARQUET_SCHEMA = reader.getFileMetaData().getSchema();
    }

    PARQUET_FILE.deleteOnExit();
  }

  private final String column;
  private final Object readValue;
  private final Object skipValue;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "boolean", false, true },
        new Object[] { "int", 5, 55 },
        new Object[] { "long", 5_000_000_049L, 5_000L },
        new Object[] { "float", 1.97f, 2.11f },
        new Object[] { "double", 2.11d, 1.97d },
        new Object[] { "date", "2018-06-29", "2018-05-03" },
        new Object[] { "time", "10:02:34.000000", "10:02:34.000001" },
        new Object[] { "timestamp",
            "2018-06-29T10:02:34.000000",
            "2018-06-29T15:02:34.000000" },
        new Object[] { "timestamptz",
            "2018-06-29T10:02:34.000000+00:00",
            "2018-06-29T10:02:34.000000-07:00" },
        new Object[] { "string", "tapir", "monthly" },
        // new Object[] { "uuid", uuid, UUID.randomUUID() }, // not supported yet
        new Object[] { "fixed", "abcd".getBytes(StandardCharsets.UTF_8), new byte[] { 0, 1, 2, 3 } },
        new Object[] { "binary", "xyz".getBytes(StandardCharsets.UTF_8), new byte[] { 0, 1, 2, 3, 4, 5 } },
        new Object[] { "int_decimal", "77.77", "12.34" },
        new Object[] { "long_decimal", "88.88", "12.34" },
        new Object[] { "fixed_decimal", "99.99", "12.34" },
    };
  }

  public TestMetricsRowGroupFilterTypes(String column, Object readValue, Object skipValue) {
    this.column = column;
    this.readValue = readValue;
    this.skipValue = skipValue;
  }

  @Test
  public void testEq() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal(column, readValue))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA);
    Assert.assertTrue("Should read: value is in the row group: " + readValue, shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal(column, skipValue))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA);
    Assert.assertFalse("Should skip: value is not in the row group: " + skipValue, shouldRead);
  }
}
