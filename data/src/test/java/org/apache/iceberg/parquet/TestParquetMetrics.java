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
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestMetrics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * Test Metrics for Parquet.
 */
public class TestParquetMetrics extends TestMetrics {
  private static final Map<String, String> SMALL_ROW_GROUP_CONFIG = ImmutableMap.of(
      TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1600");

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Override
  public FileFormat fileFormat() {
    return FileFormat.PARQUET;
  }

  @Override
  public Metrics getMetrics(InputFile file, MetricsConfig metricsConfig) {
    return ParquetUtil.fileMetrics(file, metricsConfig);
  }

  @Override
  public InputFile writeRecordsWithSmallRowGroups(Schema schema, Record... records) throws IOException {
    return writeRecords(schema, SMALL_ROW_GROUP_CONFIG, records);
  }

  @Override
  public InputFile writeRecords(Schema schema, Record... records) throws IOException {
    return writeRecords(schema, ImmutableMap.of(), records);
  }

  private InputFile writeRecords(Schema schema, Map<String, String> properties, Record... records) throws IOException {
    File tmpFolder = temp.newFolder("parquet");
    String filename = UUID.randomUUID().toString();
    OutputFile file = Files.localOutput(new File(tmpFolder, FileFormat.PARQUET.addExtension(filename)));
    try (FileAppender<Record> writer = Parquet.write(file)
        .schema(schema)
        .setAll(properties)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build()) {
      writer.addAll(Lists.newArrayList(records));
    }
    return file.toInputFile();
  }

  @Override
  public int splitCount(InputFile inputFile) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
      return reader.getRowGroups().size();
    }
  }

  @Override
  public boolean supportsSmallRowGroups() {
    return true;
  }
}
