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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestNativeParquetWriteRoundTrip {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tempDir;
  private String previousClient;

  @BeforeEach
  void useNativeClient() {
    this.previousClient = System.getProperty(Parquet.PARQUET_CLIENT);
    System.setProperty(Parquet.PARQUET_CLIENT, "native");
  }

  @AfterEach
  void restoreClient() {
    if (previousClient == null) {
      System.clearProperty(Parquet.PARQUET_CLIENT);
    } else {
      System.setProperty(Parquet.PARQUET_CLIENT, previousClient);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"uncompressed", "gzip", "snappy", "zstd"})
  void writtenRecordsAreReadBack(String codec) throws IOException {
    File file = new File(tempDir, "data.parquet");
    List<Record> expected = Lists.newArrayList();
    for (long i = 0; i < 1000; i++) {
      GenericRecord record = GenericRecord.create(SCHEMA);
      record.setField("id", i);
      record.setField("data", i % 3 == 0 ? null : "value-" + i);
      expected.add(record);
    }

    FileAppender<Record> appender =
        Parquet.write(Files.localOutput(file))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .set("write.parquet.compression-codec", codec)
            .build();
    try (FileAppender<Record> closeable = appender) {
      closeable.addAll(expected);
    }

    Metrics metrics = appender.metrics();
    assertThat(metrics.recordCount()).isEqualTo(1000L);
    assertThat(metrics.nullValueCounts()).containsEntry(2, 334L);
    assertThat(appender.splitOffsets()).isNotEmpty();
    assertThat(appender.length()).isEqualTo(file.length());

    System.setProperty(Parquet.PARQUET_CLIENT, "hadoop");
    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(file))
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
            .build()) {
      assertThat(Lists.newArrayList(reader)).isEqualTo(expected);
    }
  }
}
