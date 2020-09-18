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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class FlinkManifest {
  private static final int ICEBERG_FORMAT_VERSION = 2;
  private static final Long DUMMY_SNAPSHOT_ID = 0L;

  private final OutputFile outputFile;
  private final PartitionSpec spec;

  private FlinkManifest(OutputFile outputFile, PartitionSpec spec) {
    this.outputFile = outputFile;
    this.spec = spec;
  }

  ManifestFile write(List<DataFile> dataFiles) throws IOException {
    ManifestWriter<DataFile> writer = ManifestFiles.write(ICEBERG_FORMAT_VERSION, spec, outputFile, DUMMY_SNAPSHOT_ID);
    try (ManifestWriter<DataFile> closeableWriter = writer) {
      closeableWriter.addAll(dataFiles);
    }
    return writer.toManifestFile();
  }

  static List<DataFile> read(ManifestFile manifestFile, FileIO io) throws IOException {
    try (CloseableIterable<DataFile> dataFiles = ManifestFiles.read(manifestFile, io).project(ManifestFile.schema())) {
      return Lists.newArrayList(dataFiles);
    }
  }

  static FlinkManifestFactory createFactory(Table table, String flinkJobId, int partitionId, long taskId) {
    return new FlinkManifestFactory(table.location(), table.spec(), FileFormat.AVRO, table.locationProvider(),
        table.io(), table.encryption(), flinkJobId, partitionId, taskId);
  }

  static class FlinkManifestFactory {
    private final String tableLocation;
    private final PartitionSpec spec;
    private final FileFormat format;
    private final OutputFileFactory outputFileFactory;
    private final int partitionId;
    private final long taskId;
    private final String flinkJobId;
    private final AtomicInteger fileCount = new AtomicInteger(0);

    FlinkManifestFactory(String tableLocation, PartitionSpec spec, FileFormat format, LocationProvider locations,
                         FileIO io, EncryptionManager encryptionManager, String flinkJobId, int partitionId,
                         long taskId) {
      this.tableLocation = tableLocation;
      this.spec = spec;
      this.format = format;
      this.flinkJobId = flinkJobId;
      this.partitionId = partitionId;
      this.taskId = taskId;
      this.outputFileFactory = new OutputFileFactory(spec, format, locations, io,
          encryptionManager, partitionId, taskId);
    }

    private String generateRelativeFilePath(long checkpointId) {
      return format.addExtension(
          String.format("%s/flink-manifest/%s-%05d-%d-%d-%05d", tableLocation, flinkJobId, partitionId, taskId,
              checkpointId, fileCount.incrementAndGet()));
    }

    FlinkManifest create(long checkpointId) {
      String relativeFilePath = generateRelativeFilePath(checkpointId);
      return new FlinkManifest(outputFileFactory.newOutputFile(relativeFilePath).encryptingOutputFile(), spec);
    }
  }
}
