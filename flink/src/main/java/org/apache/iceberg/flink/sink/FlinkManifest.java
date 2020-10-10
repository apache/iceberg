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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
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
    try (CloseableIterable<DataFile> dataFiles = ManifestFiles.read(manifestFile, io)) {
      return Lists.newArrayList(dataFiles);
    }
  }

  static FlinkManifestFactory createFactory(Table table, String flinkJobId, int subTaskId, long attemptNumber) {
    TableOperations ops = ((HasTableOperations) table).operations();
    return new FlinkManifestFactory(ops, table.spec(), table.io(), table.properties(), flinkJobId, subTaskId,
        attemptNumber);
  }

  static class FlinkManifestFactory {
    // Users could define their own flink manifests directory by setting this value in table properties.
    private static final String FLINK_MANIFEST_LOCATION = "flink.manifests.location";

    private final TableOperations ops;
    private final PartitionSpec spec;
    private final FileIO io;
    private final Map<String, String> props;
    private final String flinkJobId;
    private final int subTaskId;
    private final long attemptNumber;
    private final AtomicInteger fileCount = new AtomicInteger(0);

    FlinkManifestFactory(TableOperations ops, PartitionSpec spec, FileIO io, Map<String, String> props,
                         String flinkJobId, int subTaskId, long attemptNumber) {
      this.ops = ops;
      this.spec = spec;
      this.io = io;
      this.props = props;
      this.flinkJobId = flinkJobId;
      this.subTaskId = subTaskId;
      this.attemptNumber = attemptNumber;
    }

    private String generatePath(long checkpointId) {
      return FileFormat.AVRO.addExtension(String.format("%s-%05d-%d-%d-%05d", flinkJobId, subTaskId,
          attemptNumber, checkpointId, fileCount.incrementAndGet()));
    }

    FlinkManifest create(long checkpointId) {
      String flinkManifestDir = props.get(FLINK_MANIFEST_LOCATION);

      String newManifestFullPath;
      if (flinkManifestDir == null) {
        // User don't specify any flink manifest directory, so just use the default metadata path.
        newManifestFullPath = ops.metadataFileLocation(generatePath(checkpointId));
      } else {
        newManifestFullPath = String.format("%s/%s", stripTrailingSlash(flinkManifestDir), generatePath(checkpointId));
      }

      return new FlinkManifest(io.newOutputFile(newManifestFullPath), spec);
    }

    String stripTrailingSlash(String path) {
      String result = path;
      while (result.endsWith("/")) {
        result = result.substring(0, result.length() - 1);
      }
      return result;
    }
  }
}
