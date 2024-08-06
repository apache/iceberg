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
import java.util.function.Supplier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkManifestUtil {
  private static final int FORMAT_V2 = 2;
  private static final Long DUMMY_SNAPSHOT_ID = 0L;
  private static final Logger LOG = LoggerFactory.getLogger(FlinkManifestUtil.class);

  private FlinkManifestUtil() {}

  public static ManifestFile writeDataFiles(
      OutputFile outputFile, PartitionSpec spec, List<DataFile> dataFiles) throws IOException {
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(FORMAT_V2, spec, outputFile, DUMMY_SNAPSHOT_ID);

    try (ManifestWriter<DataFile> closeableWriter = writer) {
      closeableWriter.addAll(dataFiles);
    }

    return writer.toManifestFile();
  }

  public static List<DataFile> readDataFiles(
      ManifestFile manifestFile, FileIO io, Map<Integer, PartitionSpec> specsById)
      throws IOException {
    try (CloseableIterable<DataFile> dataFiles = ManifestFiles.read(manifestFile, io, specsById)) {
      return Lists.newArrayList(dataFiles);
    }
  }

  public static ManifestOutputFileFactory createOutputFileFactory(
      Supplier<Table> tableSupplier,
      Map<String, String> tableProps,
      String flinkJobId,
      String operatorUniqueId,
      int subTaskId,
      long attemptNumber) {
    return new ManifestOutputFileFactory(
        tableSupplier, tableProps, flinkJobId, operatorUniqueId, subTaskId, attemptNumber);
  }

  /**
   * Write the {@link WriteResult} to temporary manifest files.
   *
   * @param result all those DataFiles/DeleteFiles in this WriteResult should be written with same
   *     partition spec
   */
  public static DeltaManifests writeCompletedFiles(
      WriteResult result, Supplier<OutputFile> outputFileSupplier, PartitionSpec spec)
      throws IOException {

    ManifestFile dataManifest = null;
    ManifestFile deleteManifest = null;

    // Write the completed data files into a newly created data manifest file.
    if (result.dataFiles() != null && result.dataFiles().length > 0) {
      dataManifest =
          writeDataFiles(outputFileSupplier.get(), spec, Lists.newArrayList(result.dataFiles()));
    }

    // Write the completed delete files into a newly created delete manifest file.
    if (result.deleteFiles() != null && result.deleteFiles().length > 0) {
      OutputFile deleteManifestFile = outputFileSupplier.get();

      ManifestWriter<DeleteFile> deleteManifestWriter =
          ManifestFiles.writeDeleteManifest(FORMAT_V2, spec, deleteManifestFile, DUMMY_SNAPSHOT_ID);
      try (ManifestWriter<DeleteFile> writer = deleteManifestWriter) {
        for (DeleteFile deleteFile : result.deleteFiles()) {
          writer.add(deleteFile);
        }
      }

      deleteManifest = deleteManifestWriter.toManifestFile();
    }

    return new DeltaManifests(dataManifest, deleteManifest, result.referencedDataFiles());
  }

  public static WriteResult readCompletedFiles(
      DeltaManifests deltaManifests, FileIO io, Map<Integer, PartitionSpec> specsById)
      throws IOException {
    WriteResult.Builder builder = WriteResult.builder();

    // Read the completed data files from persisted data manifest file.
    if (deltaManifests.dataManifest() != null) {
      builder.addDataFiles(readDataFiles(deltaManifests.dataManifest(), io, specsById));
    }

    // Read the completed delete files from persisted delete manifests file.
    if (deltaManifests.deleteManifest() != null) {
      try (CloseableIterable<DeleteFile> deleteFiles =
          ManifestFiles.readDeleteManifest(deltaManifests.deleteManifest(), io, specsById)) {
        builder.addDeleteFiles(deleteFiles);
      }
    }

    return builder.addReferencedDataFiles(deltaManifests.referencedDataFiles()).build();
  }

  public static void deleteCommittedManifests(
      Table table, List<ManifestFile> manifests, String newFlinkJobId, long checkpointId) {
    for (ManifestFile manifest : manifests) {
      try {
        table.io().deleteFile(manifest.path());
      } catch (Exception e) {
        // The flink manifests cleaning failure shouldn't abort the completed checkpoint.
        String details =
            MoreObjects.toStringHelper(FlinkManifestUtil.class)
                .add("flinkJobId", newFlinkJobId)
                .add("checkpointId", checkpointId)
                .add("manifestPath", manifest.path())
                .toString();
        LOG.warn(
            "The iceberg transaction has been committed, but we failed to clean the temporary flink manifests: {}",
            details,
            e);
      }
    }
  }
}
