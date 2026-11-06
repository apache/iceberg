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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.avro.AvroFormat;
import org.apache.iceberg.data.orc.OrcFormat;
import org.apache.iceberg.data.parquet.ParquetFormat;
import org.apache.iceberg.data.vortex.VortexFormat;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public interface FileFormatTestSupport {

  FileFormatTestSupport[] ALL = all();

  /**
   * The formats this TCK runs against.
   *
   * <p>Vortex is only included when {@code iceberg-vortex} is on the classpath: the Flink modules
   * inherit these tests without it, and Vortex has no Flink integration yet.
   */
  static FileFormatTestSupport[] all() {
    List<FileFormatTestSupport> formats =
        Lists.newArrayList(new AvroFormat(), new OrcFormat(), new ParquetFormat());
    try {
      Class.forName("org.apache.iceberg.vortex.VortexFormatModels");
      formats.add(new VortexFormat());
    } catch (ClassNotFoundException e) {
      // iceberg-vortex is not on the classpath; skip the format
    }

    return formats.toArray(new FileFormatTestSupport[0]);
  }

  static FileFormat[] formats() {
    return Arrays.stream(ALL).map(FileFormatTestSupport::format).toArray(FileFormat[]::new);
  }

  static FileFormatTestSupport forFormat(FileFormat format) {
    return Arrays.stream(ALL)
        .filter(testSupportFormat -> testSupportFormat.format() == format)
        .findFirst()
        .orElseThrow(() -> new UnsupportedOperationException("Unsupported file format: " + format));
  }

  FileFormat format();

  void writeRecordsWithoutFieldIds(OutputFile outputFile, Schema schema, List<Record> records)
      throws IOException;

  Map<String, String> testPropertiesToSet();

  boolean checkTestProperties(InputFile inputFile) throws IOException;

  String metadataValue(InputFile inputFile, String key) throws IOException;

  String splitSizeProperty();
}
