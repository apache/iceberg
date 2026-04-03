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

import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestParquetWriteAdapter {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private Path temp;

  @Test
  void postCloseAccessors() throws IOException {
    OutputFile file = Files.localOutput(createTempFile(temp));

    // Parquet.write() without .createWriterFunc() produces a ParquetWriteAdapter
    FileAppender<GenericData.Record> appender =
        Parquet.write(file).schema(SCHEMA).overwrite().build();

    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(SCHEMA, "table");
    GenericData.Record record = new GenericData.Record(avroSchema);
    record.put("id", 1L);
    record.put("data", "a");
    appender.add(record);

    // close the appender — this nulls writer in ParquetWriteAdapter
    appender.close();

    // all three accessors must work after close without NPE
    assertThatNoException().isThrownBy(appender::length);
    assertThat(appender.length()).isGreaterThan(0L);

    assertThatNoException().isThrownBy(appender::metrics);
    assertThat(appender.metrics()).isNotNull();
    assertThat(appender.metrics().recordCount()).isEqualTo(1L);

    assertThatNoException().isThrownBy(appender::splitOffsets);
  }

  @Test
  void dataWriterCloseWithDeprecatedAdapter() throws IOException {
    OutputFile file = Files.localOutput(createTempFile(temp));

    // Parquet.writeData() without .createWriterFunc() uses ParquetWriteAdapter
    DataWriter<GenericData.Record> dataWriter =
        Parquet.writeData(file)
            .schema(SCHEMA)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(SCHEMA, "table");
    GenericData.Record record = new GenericData.Record(avroSchema);
    record.put("id", 1L);
    record.put("data", "a");
    dataWriter.write(record);

    // DataWriter.close() calls appender.close() then appender.length()/splitOffsets()/metrics()
    // This previously threw NPE with ParquetWriteAdapter
    dataWriter.close();

    DataFile dataFile = dataWriter.toDataFile();
    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(1L);
    assertThat(dataFile.fileSizeInBytes()).isGreaterThan(0L);
    assertThat(dataFile.format()).isEqualTo(FileFormat.PARQUET);
  }
}
