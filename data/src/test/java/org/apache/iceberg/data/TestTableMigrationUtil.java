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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.RandomAvroData;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetAvroWriter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestTableMigrationUtil {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("id").build();
  private static final Map<String, String> PARTITION = Map.of("id", "1");
  private static final String FORMAT = "parquet";
  private static final Configuration CONF = new Configuration();

  @TempDir protected Path tempTableLocation;

  @Test
  void testListPartition() throws IOException {
    Path partitionPath = tempTableLocation.resolve("id=1");
    String partitionUri = partitionPath.toUri().toString();
    java.nio.file.Files.createDirectories(partitionPath);
    writePartitionFile(partitionPath.toFile());

    List<DataFile> dataFiles =
        TableMigrationUtil.listPartition(
            PARTITION, partitionUri, FORMAT, SPEC, CONF, MetricsConfig.getDefault(), null);
    assertThat(dataFiles)
        .as("List partition with 1 Parquet file should return 1 DataFile")
        .hasSize(1);
  }

  @Test
  void testListEmptyPartition() throws IOException {
    Path partitionPath = tempTableLocation.resolve("id=1");
    String partitionUri = partitionPath.toUri().toString();
    java.nio.file.Files.createDirectories(partitionPath);

    List<DataFile> dataFiles =
        TableMigrationUtil.listPartition(
            PARTITION, partitionUri, FORMAT, SPEC, CONF, MetricsConfig.getDefault(), null);
    assertThat(dataFiles).as("List partition with 0 file should return 0 DataFile").isEmpty();
  }

  @Test
  void testListPartitionMissingFilesFailure() {
    String partitionUri = tempTableLocation.resolve("id=1").toUri().toString();

    assertThatThrownBy(
            () ->
                TableMigrationUtil.listPartition(
                    PARTITION, partitionUri, FORMAT, SPEC, CONF, MetricsConfig.getDefault(), null))
        .hasMessageContaining("Unable to list files in partition: " + partitionUri)
        .isInstanceOf(RuntimeException.class)
        .hasRootCauseInstanceOf(FileNotFoundException.class);
  }

  private static void writePartitionFile(File outputDir) throws IOException {
    Iterable<GenericData.Record> records = RandomAvroData.generate(SCHEMA, 1000, 54310);
    File testFile = new File(outputDir, "junit" + System.nanoTime() + ".parquet");

    try (FileAppender<GenericData.Record> writer =
        Parquet.write(Files.localOutput(testFile))
            .schema(SCHEMA)
            .createWriterFunc(ParquetAvroWriter::buildWriter)
            .build()) {
      writer.addAll(records);
    }
  }
}
