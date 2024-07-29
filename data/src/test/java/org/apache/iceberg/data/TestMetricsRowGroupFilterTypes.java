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

import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMetricsRowGroupFilterTypes {
  private static final Schema SCHEMA =
      new Schema(
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
          optional(16, "fixed_decimal", Types.DecimalType.of(31, 2)));

  private static final Schema FILE_SCHEMA =
      new Schema(
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
          optional(16, "_fixed_decimal", Types.DecimalType.of(31, 2)));

  private static final File ORC_FILE = new File("/tmp/stats-row-group-filter-types-test.orc");

  private static final File PARQUET_FILE =
      new File("/tmp/stats-row-group-filter-types-test.parquet");
  private static MessageType parquetSchema = null;
  private static BlockMetaData rowGroupMetadata = null;

  private static final UUID UUID_VALUE = UUID.randomUUID();
  private static final LocalDate DATE =
      LocalDate.parse("2018-06-29", DateTimeFormatter.ISO_LOCAL_DATE);
  private static final LocalTime TIME =
      LocalTime.parse("10:02:34.000000", DateTimeFormatter.ISO_LOCAL_TIME);
  private static final OffsetDateTime TIMESTAMPTZ =
      OffsetDateTime.parse("2018-06-29T10:02:34.000000+00:00", DateTimeFormatter.ISO_DATE_TIME);
  private static final LocalDateTime TIMESTAMP =
      LocalDateTime.parse("2018-06-29T10:02:34.000000", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  private static final byte[] FIXED = "abcd".getBytes(StandardCharsets.UTF_8);

  @BeforeEach
  public void createInputFile() throws IOException {
    List<Record> records = Lists.newArrayList();
    // create 50 records
    for (int i = 0; i < 50; i += 1) {
      Record record = GenericRecord.create(FILE_SCHEMA);
      record.setField("_boolean", false);
      record.setField("_int", i);
      record.setField("_long", 5_000_000_000L + i);
      record.setField("_float", ((float) (100 - i)) / 100F + 1.0F); // 2.0f, 1.99f, 1.98f, ...
      record.setField("_double", ((double) i) / 100.0D + 2.0D); // 2.0d, 2.01d, 2.02d, ...
      record.setField("_date", DATE);
      record.setField("_time", TIME);
      record.setField("_timestamp", TIMESTAMP);
      record.setField("_timestamptz", TIMESTAMPTZ);
      record.setField("_string", "tapir");
      // record.setField("_uuid", UUID_VALUE); // Disable writing UUID value as GenericParquetWriter
      // does
      // not handle UUID type
      // correctly; Also UUID tests are disabled for both ORC and Parquet anyway
      record.setField("_fixed", FIXED);
      record.setField("_binary", ByteBuffer.wrap("xyz".getBytes(StandardCharsets.UTF_8)));
      record.setField("_int_decimal", new BigDecimal("77.77"));
      record.setField("_long_decimal", new BigDecimal("88.88"));
      record.setField("_fixed_decimal", new BigDecimal("99.99"));
      records.add(record);
    }
    switch (format) {
      case ORC:
        createOrcInputFile(records);
        break;
      case PARQUET:
        createParquetInputFile(records);
        break;
      default:
        throw new UnsupportedOperationException(
            "Row group filter types tests not supported for " + format);
    }
  }

  public void createOrcInputFile(List<Record> records) throws IOException {
    if (ORC_FILE.exists()) {
      assertThat(ORC_FILE.delete()).isTrue();
    }

    OutputFile outFile = Files.localOutput(ORC_FILE);
    try (FileAppender<Record> appender =
        ORC.write(outFile)
            .schema(FILE_SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      appender.addAll(records);
    }

    InputFile inFile = Files.localInput(ORC_FILE);
    try (Reader reader =
        OrcFile.createReader(
            new Path(inFile.location()), OrcFile.readerOptions(new Configuration()))) {
      assertThat(reader.getStripes()).as("Should create only one stripe").hasSize(1);
    }

    ORC_FILE.deleteOnExit();
  }

  public void createParquetInputFile(List<Record> records) throws IOException {
    if (PARQUET_FILE.exists()) {
      assertThat(PARQUET_FILE.delete()).isTrue();
    }

    OutputFile outFile = Files.localOutput(PARQUET_FILE);
    try (FileAppender<Record> appender =
        Parquet.write(outFile)
            .schema(FILE_SCHEMA)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build()) {
      appender.addAll(records);
    }

    InputFile inFile = Files.localInput(PARQUET_FILE);
    try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile(inFile))) {
      assertThat(reader.getRowGroups()).as("Should create only one row group").hasSize(1);
      rowGroupMetadata = reader.getRowGroups().get(0);
      parquetSchema = reader.getFileMetaData().getSchema();
    }

    PARQUET_FILE.deleteOnExit();
  }

  @Parameter(index = 0)
  private FileFormat format;

  @Parameter(index = 1)
  private String column;

  @Parameter(index = 2)
  private Object readValue;

  @Parameter(index = 3)
  private Object skipValue;

  @Parameters(name = "format = {0}, column = {1}, readValue = {2}, skipValue = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {FileFormat.PARQUET, "boolean", false, true},
      {FileFormat.PARQUET, "int", 5, 55},
      {FileFormat.PARQUET, "long", 5_000_000_049L, 5_000L},
      {FileFormat.PARQUET, "float", 1.97f, 2.11f},
      {FileFormat.PARQUET, "double", 2.11d, 1.97d},
      {FileFormat.PARQUET, "date", "2018-06-29", "2018-05-03"},
      {FileFormat.PARQUET, "time", "10:02:34.000000", "10:02:34.000001"},
      {FileFormat.PARQUET, "timestamp", "2018-06-29T10:02:34.000000", "2018-06-29T15:02:34.000000"},
      {
        FileFormat.PARQUET,
        "timestamptz",
        "2018-06-29T10:02:34.000000+00:00",
        "2018-06-29T10:02:34.000000-07:00"
      },
      {FileFormat.PARQUET, "string", "tapir", "monthly"},
      // { FileFormat.PARQUET, "uuid", UUID_VALUE, UUID.randomUUID() }, // not supported yet
      {
        FileFormat.PARQUET,
        "fixed",
        "abcd".getBytes(StandardCharsets.UTF_8),
        new byte[] {0, 1, 2, 3}
      },
      {
        FileFormat.PARQUET,
        "binary",
        "xyz".getBytes(StandardCharsets.UTF_8),
        new byte[] {0, 1, 2, 3, 4, 5}
      },
      {FileFormat.PARQUET, "int_decimal", "77.77", "12.34"},
      {FileFormat.PARQUET, "long_decimal", "88.88", "12.34"},
      {FileFormat.PARQUET, "fixed_decimal", "99.99", "12.34"},
      {FileFormat.ORC, "boolean", false, true},
      {FileFormat.ORC, "int", 5, 55},
      {FileFormat.ORC, "long", 5_000_000_049L, 5_000L},
      {FileFormat.ORC, "float", 1.97f, 2.11f},
      {FileFormat.ORC, "double", 2.11d, 1.97d},
      {FileFormat.ORC, "date", "2018-06-29", "2018-05-03"},
      {FileFormat.ORC, "time", "10:02:34.000000", "10:02:34.000001"},
      {FileFormat.ORC, "timestamp", "2018-06-29T10:02:34.000000", "2018-06-29T15:02:34.000000"},
      {
        FileFormat.ORC,
        "timestamptz",
        "2018-06-29T10:02:34.000000+00:00",
        "2018-06-29T10:02:34.000000-07:00"
      },
      {FileFormat.ORC, "string", "tapir", "monthly"},
      // uuid, fixed and binary types not supported yet
      // { FileFormat.ORC, "uuid", UUID_VALUE, UUID.randomUUID() },
      // { FileFormat.ORC, "fixed", "abcd".getBytes(StandardCharsets.UTF_8), new byte[] { 0, 1,
      // 2, 3 } },
      // { FileFormat.ORC, "binary", "xyz".getBytes(StandardCharsets.UTF_8), new byte[] { 0, 1,
      // 2, 3, 4, 5 }
      // },
      {FileFormat.ORC, "int_decimal", "77.77", "12.34"},
      {FileFormat.ORC, "long_decimal", "88.88", "12.34"},
      {FileFormat.ORC, "fixed_decimal", "99.99", "12.34"},
    };
  }

  @TestTemplate
  public void testEq() {
    boolean shouldRead = shouldRead(readValue);
    assertThat(shouldRead).as("Should read: value is in the row group: " + readValue).isTrue();

    shouldRead = shouldRead(skipValue);
    assertThat(shouldRead).as("Should skip: value is not in the row group: " + skipValue).isFalse();
  }

  private boolean shouldRead(Object value) {
    switch (format) {
      case ORC:
        return shouldReadOrc(value);
      case PARQUET:
        return shouldReadParquet(value);
      default:
        throw new UnsupportedOperationException(
            "Row group filter types tests not supported for " + format);
    }
  }

  private boolean shouldReadOrc(Object value) {
    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(ORC_FILE))
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .filter(Expressions.equal(column, value))
            .build()) {
      return !Lists.newArrayList(reader).isEmpty();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean shouldReadParquet(Object value) {
    return new ParquetMetricsRowGroupFilter(SCHEMA, equal(column, value))
        .shouldRead(parquetSchema, rowGroupMetadata);
  }

  private org.apache.parquet.io.InputFile parquetInputFile(InputFile inFile) {
    return new org.apache.parquet.io.InputFile() {
      @Override
      public long getLength() throws IOException {
        return inFile.getLength();
      }

      @Override
      public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
        SeekableInputStream stream = inFile.newStream();
        return new DelegatingSeekableInputStream(stream) {
          @Override
          public long getPos() throws IOException {
            return stream.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            stream.seek(newPos);
          }
        };
      }
    };
  }
}
