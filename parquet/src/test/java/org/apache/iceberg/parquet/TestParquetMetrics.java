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
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestMetrics;
import org.apache.iceberg.io.InputFile;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;

/**
 * Test Metrics for Parquet.
 */
public class TestParquetMetrics extends TestMetrics {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Override
  public Metrics getMetrics(InputFile file) {
    return ParquetUtil.fileMetrics(file);
  }

  @Override
  public File writeRecords(Schema schema, GenericData.Record... records) throws IOException {
    return ParquetWritingTestUtils.writeRecords(temp, schema, records);
  }

  @Override
  public File writeRecords(Schema schema, Map<String, String> properties, GenericData.Record... records) throws IOException {
    return ParquetWritingTestUtils.writeRecords(temp, schema, properties, records);
  }

  @Override
  public int splitCount(File parquetFile) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
      return reader.getRowGroups().size();
    }
  }
}
