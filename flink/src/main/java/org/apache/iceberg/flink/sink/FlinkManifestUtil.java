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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class FlinkManifestUtil {
  private static final int ICEBERG_FORMAT_VERSION = 2;
  private static final Long DUMMY_SNAPSHOT_ID = 0L;

  private FlinkManifestUtil() {
  }

  static ManifestFile writeDataFiles(OutputFile outputFile, PartitionSpec spec, List<DataFile> dataFiles)
      throws IOException {
    ManifestWriter<DataFile> writer = ManifestFiles.write(ICEBERG_FORMAT_VERSION, spec, outputFile, DUMMY_SNAPSHOT_ID);

    try (ManifestWriter<DataFile> closeableWriter = writer) {
      closeableWriter.addAll(dataFiles);
    }

    return writer.toManifestFile();
  }

  static List<DataFile> readDataFiles(ManifestFile manifestFile, FileIO io) throws IOException {
    try (CloseableIterable<DataFile> dataFiles = ManifestFiles.read(manifestFile, io)) {
      return Lists.newArrayList(dataFiles);
    }
  }

  static ManifestOutputFileFactory createOutputFileFactory(Table table, String flinkJobId, int subTaskId,
                                                           long attemptNumber) {
    TableOperations ops = ((HasTableOperations) table).operations();
    return new ManifestOutputFileFactory(ops, table.io(), table.properties(), flinkJobId, subTaskId, attemptNumber);
  }
}
