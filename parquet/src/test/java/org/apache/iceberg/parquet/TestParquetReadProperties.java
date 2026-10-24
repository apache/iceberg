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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestParquetReadProperties {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

  private static final List<Record> RECORDS =
      ImmutableList.of(
          GenericRecord.create(SCHEMA).copy(ImmutableMap.of("id", 1L, "data", "a")),
          GenericRecord.create(SCHEMA).copy(ImmutableMap.of("id", 2L, "data", "b")));

  enum ReadPath {
    NON_HADOOP,
    HADOOP
  }

  private static InputFile writeFile(ReadPath readPath, File tempDir) throws IOException {
    OutputFile outputFile =
        readPath == ReadPath.HADOOP
            ? HadoopOutputFile.fromPath(
                new Path(new File(tempDir, "test.parquet").getAbsolutePath()), new Configuration())
            : new InMemoryOutputFile();
    try (DataWriter<Record> writer =
        Parquet.writeData(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      for (Record record : RECORDS) {
        writer.write(record);
      }
    }

    return outputFile.toInputFile();
  }

  private static Parquet.ReadBuilder readBuilder(InputFile file) {
    return Parquet.read(file)
        .project(SCHEMA)
        .createReaderFunc(fileSchema -> InternalReader.create(SCHEMA, fileSchema));
  }

  @ParameterizedTest
  @EnumSource(ReadPath.class)
  void readPropertiesAreAppliedToReadOptions(ReadPath readPath, @TempDir File tempDir)
      throws IOException {
    InputFile file = writeFile(readPath, tempDir);

    // parquet-java parses this property when the read options are built, so an unparseable value
    // fails the read only if the property was applied
    assertThatThrownBy(
            () -> readBuilder(file).set("parquet.read.allocation.size", "not-a-number").build())
        .isInstanceOf(NumberFormatException.class)
        .hasMessageContaining("not-a-number");
  }

  @ParameterizedTest
  @EnumSource(ReadPath.class)
  void nullPropertyValuesAreIgnored(ReadPath readPath, @TempDir File tempDir) throws IOException {
    InputFile file = writeFile(readPath, tempDir);

    try (CloseableIterable<Record> reader =
        readBuilder(file).set("parquet.custom.property", null).build()) {
      assertThat(reader).hasSameSizeAs(RECORDS);
    }
  }

  @ParameterizedTest
  @EnumSource(ReadPath.class)
  void removedReadPropertiesAreNotApplied(ReadPath readPath, @TempDir File tempDir)
      throws IOException {
    InputFile file = writeFile(readPath, tempDir);

    // parquet-java would fail to load this record filter class if the property was applied
    try (CloseableIterable<Record> reader =
        readBuilder(file).set("parquet.read.filter", "org.example.MissingFilter").build()) {
      assertThat(reader).hasSameSizeAs(RECORDS);
    }
  }
}
