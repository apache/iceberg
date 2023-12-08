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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.expressions.Expression;
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
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.avro.AvroSchemaUtil.convert;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public abstract class MetricsRowGroupFilterBase {

  @Parameterized.Parameters(name = "format = {0}")
  public static Object[] parameters() {
    return new Object[] {"parquet", "orc"};
  }

  protected final FileFormat format;

  public MetricsRowGroupFilterBase(String format) {
    this.format = FileFormat.fromString(format);
  }

  protected static final Types.StructType structFieldType =
      Types.StructType.of(Types.NestedField.required(8, "int_field", Types.IntegerType.get()));

  protected static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "no_stats_parquet", Types.StringType.get()),
          required(3, "required", Types.StringType.get()),
          optional(4, "all_nulls", Types.DoubleType.get()),
          optional(5, "some_nulls", Types.StringType.get()),
          optional(6, "no_nulls", Types.StringType.get()),
          optional(7, "struct_not_null", structFieldType),
          optional(9, "not_in_file", Types.FloatType.get()),
          optional(10, "str", Types.StringType.get()),
          optional(
              11,
              "map_not_null",
              Types.MapType.ofRequired(12, 13, Types.StringType.get(), Types.IntegerType.get())),
          optional(14, "all_nans", Types.DoubleType.get()),
          optional(15, "some_nans", Types.FloatType.get()),
          optional(16, "no_nans", Types.DoubleType.get()));

  protected static final Types.StructType _structFieldType =
      Types.StructType.of(Types.NestedField.required(8, "_int_field", Types.IntegerType.get()));

  protected static final Schema FILE_SCHEMA =
      new Schema(
          required(1, "_id", Types.IntegerType.get()),
          optional(2, "_no_stats_parquet", Types.StringType.get()),
          required(3, "_required", Types.StringType.get()),
          optional(4, "_all_nulls", Types.DoubleType.get()),
          optional(5, "_some_nulls", Types.StringType.get()),
          optional(6, "_no_nulls", Types.StringType.get()),
          optional(7, "_struct_not_null", _structFieldType),
          optional(10, "_str", Types.StringType.get()),
          optional(14, "_all_nans", Types.DoubleType.get()),
          optional(15, "_some_nans", Types.FloatType.get()),
          optional(16, "_no_nans", Types.DoubleType.get()));

  protected static final String TOO_LONG_FOR_STATS_PARQUET;

  static {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i += 1) {
      sb.append(UUID.randomUUID().toString());
    }
    TOO_LONG_FOR_STATS_PARQUET = sb.toString();
  }

  protected static final int INT_MIN_VALUE = 30;
  protected static final int INT_MAX_VALUE = 79;

  protected File orcFile = null;
  protected MessageType parquetSchema = null;
  protected BlockMetaData rowGroupMetadata = null;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void createInputFile() throws IOException {
    switch (format) {
      case ORC:
        createOrcInputFile();
        break;
      case PARQUET:
        createParquetInputFile();
        break;
      default:
        throw new UnsupportedOperationException(
            "Row group filter tests not supported for " + format);
    }
  }

  public void createOrcInputFile() throws IOException {
    this.orcFile = temp.newFile();
    Assert.assertTrue(orcFile.delete());

    OutputFile outFile = Files.localOutput(orcFile);
    try (FileAppender<GenericRecord> appender =
        ORC.write(outFile)
            .schema(FILE_SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      GenericRecord record = GenericRecord.create(FILE_SCHEMA);
      // create 50 records
      for (int i = 0; i < INT_MAX_VALUE - INT_MIN_VALUE + 1; i += 1) {
        record.setField("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
        record.setField(
            "_no_stats_parquet",
            TOO_LONG_FOR_STATS_PARQUET); // value longer than 4k will produce no stats
        // in Parquet, but will produce stats for ORC
        record.setField("_required", "req"); // required, always non-null
        record.setField("_all_nulls", null); // never non-null
        record.setField("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        record.setField("_no_nulls", ""); // optional, but always non-null
        record.setField("_str", i + "str" + i);
        record.setField("_all_nans", Double.NaN); // never non-nan
        record.setField("_some_nans", (i % 10 == 0) ? Float.NaN : 2F); // includes some nan values
        record.setField("_no_nans", 3D); // optional, but always non-nan

        GenericRecord structNotNull = GenericRecord.create(_structFieldType);
        structNotNull.setField("_int_field", INT_MIN_VALUE + i);
        record.setField("_struct_not_null", structNotNull); // struct with int

        appender.add(record);
      }
    }

    InputFile inFile = Files.localInput(orcFile);
    try (Reader reader =
        OrcFile.createReader(
            new Path(inFile.location()), OrcFile.readerOptions(new Configuration()))) {
      Assert.assertEquals("Should create only one stripe", 1, reader.getStripes().size());
    }

    orcFile.deleteOnExit();
  }

  protected void createParquetInputFile() throws IOException {
    File parquetFile = temp.newFile();
    Assert.assertTrue(parquetFile.delete());

    // build struct field schema
    org.apache.avro.Schema structSchema = AvroSchemaUtil.convert(_structFieldType);

    OutputFile outFile = Files.localOutput(parquetFile);
    try (FileAppender<GenericData.Record> appender =
        Parquet.write(outFile).schema(FILE_SCHEMA).build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(convert(FILE_SCHEMA, "table"));
      // create 50 records
      for (int i = 0; i < INT_MAX_VALUE - INT_MIN_VALUE + 1; i += 1) {
        builder.set("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
        builder.set(
            "_no_stats_parquet",
            TOO_LONG_FOR_STATS_PARQUET); // value longer than 4k will produce no stats
        // in Parquet
        builder.set("_required", "req"); // required, always non-null
        builder.set("_all_nulls", null); // never non-null
        builder.set("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        builder.set("_no_nulls", ""); // optional, but always non-null
        builder.set("_all_nans", Double.NaN); // never non-nan
        builder.set("_some_nans", (i % 10 == 0) ? Float.NaN : 2F); // includes some nan values
        builder.set("_no_nans", 3D); // optional, but always non-nan
        builder.set("_str", i + "str" + i);

        GenericData.Record structNotNull = new GenericData.Record(structSchema);
        structNotNull.put("_int_field", INT_MIN_VALUE + i);
        builder.set("_struct_not_null", structNotNull); // struct with int

        appender.add(builder.build());
      }
    }

    InputFile inFile = Files.localInput(parquetFile);
    try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile(inFile))) {
      Assert.assertEquals("Should create only one row group", 1, reader.getRowGroups().size());
      rowGroupMetadata = reader.getRowGroups().get(0);
      parquetSchema = reader.getFileMetaData().getSchema();
    }

    parquetFile.deleteOnExit();
  }

  protected boolean shouldRead(Expression expression) {
    return shouldRead(expression, true);
  }

  protected boolean shouldRead(Expression expression, boolean caseSensitive) {
    switch (format) {
      case ORC:
        return shouldReadOrc(expression, caseSensitive);
      case PARQUET:
        return shouldReadParquet(expression, caseSensitive, parquetSchema, rowGroupMetadata);
      default:
        throw new UnsupportedOperationException(
            "Row group filter tests not supported for " + format);
    }
  }

  protected boolean shouldReadOrc(Expression expression, boolean caseSensitive) {
    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(orcFile))
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .filter(expression)
            .caseSensitive(caseSensitive)
            .build()) {
      return Lists.newArrayList(reader).size() > 0;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected boolean shouldReadParquet(
      Expression expression,
      boolean caseSensitive,
      MessageType messageType,
      BlockMetaData blockMetaData) {
    return new ParquetMetricsRowGroupFilter(SCHEMA, expression, caseSensitive)
        .shouldRead(messageType, blockMetaData);
  }

  protected org.apache.parquet.io.InputFile parquetInputFile(InputFile inFile) {
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
