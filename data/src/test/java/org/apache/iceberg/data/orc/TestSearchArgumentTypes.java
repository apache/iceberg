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

package org.apache.iceberg.data.orc;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
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
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestSearchArgumentTypes {
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

  private static final File ORC_FILE = new File("/tmp/stats-search-argument-filter-types-test.orc");

  private static final UUID uuid = UUID.randomUUID();
  private static final LocalDate date =  LocalDate.parse("2018-06-29", DateTimeFormatter.ISO_LOCAL_DATE);
  private static final LocalTime time = LocalTime.parse("10:02:34.000000", DateTimeFormatter.ISO_LOCAL_TIME);
  private static final OffsetDateTime timestamptz = OffsetDateTime.parse("2018-06-29T10:02:34.000000+00:00",
      DateTimeFormatter.ISO_DATE_TIME);
  private static final LocalDateTime timestamp = LocalDateTime.parse("2018-06-29T10:02:34.000000",
      DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  private static final byte[] fixed = "abcd".getBytes(StandardCharsets.UTF_8);

  @BeforeClass
  public static void createInputFile() throws IOException {
    if (ORC_FILE.exists()) {
      Assert.assertTrue(ORC_FILE.delete());
    }

    OutputFile outFile = Files.localOutput(ORC_FILE);
    try (FileAppender<GenericRecord> appender = ORC.write(outFile)
        .schema(FILE_SCHEMA)
        .createWriterFunc(GenericOrcWriter::buildWriter)
        .build()) {
      GenericRecord record = GenericRecord.create(FILE_SCHEMA);
      // create 50 records
      for (int i = 0; i < 50; i += 1) {
        record.setField("_boolean", false);
        record.setField("_int", i);
        record.setField("_long", 5_000_000_000L + i);
        record.setField("_float", ((float) (100 - i)) / 100F + 1.0F); // 2.0f, 1.99f, 1.98f, ...
        record.setField("_double", ((double) i) / 100.0D + 2.0D); // 2.0d, 2.01d, 2.02d, ...
        record.setField("_date", date);
        record.setField("_time", time);
        record.setField("_timestamp", timestamp);
        record.setField("_timestamptz", timestamptz);
        record.setField("_string", "tapir");
        record.setField("_uuid", uuid);
        record.setField("_fixed", fixed);
        record.setField("_binary", ByteBuffer.wrap("xyz".getBytes(StandardCharsets.UTF_8)));
        record.setField("_int_decimal", new BigDecimal("77.77"));
        record.setField("_long_decimal", new BigDecimal("88.88"));
        record.setField("_fixed_decimal", new BigDecimal("99.99"));
        appender.add(record);
      }
    }

    InputFile inFile = Files.localInput(ORC_FILE);
    try (Reader reader = OrcFile.createReader(new Path(inFile.location()),
        OrcFile.readerOptions(new Configuration()))) {
      Assert.assertEquals("Should create only one stripe", 1, reader.getStripes().size());
    }

    ORC_FILE.deleteOnExit();
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
        new Object[] { "int_decimal", "77.77", "12.34" },
        new Object[] { "long_decimal", "88.88", "12.34" },
        new Object[] { "fixed_decimal", "99.99", "12.34" },
        // uuid, fixed and binary types not yet supported
        // new Object[] { "uuid", uuid, UUID.randomUUID() },
        // new Object[] { "fixed", "abcd".getBytes(StandardCharsets.UTF_8), new byte[] { 0, 1, 2, 3 } },
        // new Object[] { "binary", "xyz".getBytes(StandardCharsets.UTF_8), new byte[] { 0, 1, 2, 3, 4, 5 } },
    };
  }

  public TestSearchArgumentTypes(String column, Object readValue, Object skipValue) {
    this.column = column;
    this.readValue = readValue;
    this.skipValue = skipValue;
  }

  @Test
  public void testEq() throws IOException {
    boolean shouldRead = shouldRead(readValue);
    Assert.assertTrue("Should read: value is in the row group: " + readValue, shouldRead);

    shouldRead = shouldRead(skipValue);
    Assert.assertFalse("Should skip: value is not in the row group: " + skipValue, shouldRead);
  }

  private boolean shouldRead(Object value) throws IOException {
    try (CloseableIterable<Record> reader = ORC.read(Files.localInput(ORC_FILE))
        .project(SCHEMA)
        .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
        .filter(Expressions.equal(column, value))
        .build()) {
      return Lists.newArrayList(reader).size() > 0;
    }
  }
}
