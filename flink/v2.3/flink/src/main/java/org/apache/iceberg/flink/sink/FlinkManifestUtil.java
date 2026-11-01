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
import java.util.function.Function;
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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkManifestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkManifestUtil.class);
  private static final Long DUMMY_SNAPSHOT_ID = 0L;

  private FlinkManifestUtil() {}

  static ManifestFile writeDataFiles(
      OutputFile outputFile, PartitionSpec spec, List<DataFile> dataFiles, int formatVersion)
      throws IOException {
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(formatVersion, spec, outputFile, DUMMY_SNAPSHOT_ID);

    try (ManifestWriter<DataFile> closeableWriter = writer) {
      closeableWriter.addAll(dataFiles);
    }

    return writer.toManifestFile();
  }

  static List<DataFile> readDataFiles(
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
        tableSupplier, tableProps, flinkJobId, operatorUniqueId, subTaskId, attemptNumber, null);
  }

  public static ManifestOutputFileFactory createOutputFileFactory(
      Supplier<Table> tableSupplier,
      Map<String, String> tableProps,
      String flinkJobId,
      String operatorUniqueId,
      int subTaskId,
      long attemptNumber,
      String suffix) {
    return new ManifestOutputFileFactory(
        tableSupplier, tableProps, flinkJobId, operatorUniqueId, subTaskId, attemptNumber, suffix);
  }

  /**
   * Write the {@link WriteResult} to temporary manifest files.
   *
   * @param result all those DataFiles/DeleteFiles in this WriteResult should be written with same
   *     partition spec
   */
  public static DeltaManifests writeCompletedFiles(
      WriteResult result,
      Supplier<OutputFile> outputFileSupplier,
      PartitionSpec spec,
      int formatVersion)
      throws IOException {
    return writeCompletedFiles(result, outputFileSupplier, spec, specId -> spec, formatVersion);
  }

  /**
   * Write the {@link WriteResult} to temporary manifest files.
   *
   * @param result the completed files; data files must be written with {@code spec}
   * @param deleteSpecs resolves the partition spec of each delete file by its spec ID
   */
  public static DeltaManifests writeCompletedFiles(
      WriteResult result,
      Supplier<OutputFile> outputFileSupplier,
      PartitionSpec spec,
      Function<Integer, PartitionSpec> deleteSpecs,
      int formatVersion)
      throws IOException {

    ManifestFile dataManifest = null;
    List<ManifestFile> deleteManifests = Lists.newArrayList();

    // Write the completed data files into a newly created data manifest file.
    if (result.dataFiles() != null && result.dataFiles().length > 0) {
      dataManifest =
          writeDataFiles(
              outputFileSupplier.get(),
              spec,
              Lists.newArrayList(result.dataFiles()),
              formatVersion);
    }

    if (result.deleteFiles() != null && result.deleteFiles().length > 0) {
      Map<PartitionSpec, List<DeleteFile>> deleteFilesBySpec = Maps.newLinkedHashMap();
      for (DeleteFile deleteFile : result.deleteFiles()) {
        PartitionSpec deleteSpec = deleteSpecs.apply(deleteFile.specId());
        if (deleteSpec == null) {
          LOG.warn(
              "Skipping delete file {} for removed partition spec {}",
              deleteFile.location(),
              deleteFile.specId());
          continue;
        }

        deleteFilesBySpec
            .computeIfAbsent(deleteSpec, unused -> Lists.newArrayList())
            .add(deleteFile);
      }

      for (Map.Entry<PartitionSpec, List<DeleteFile>> entry : deleteFilesBySpec.entrySet()) {
        deleteManifests.add(
            writeDeleteFiles(
                outputFileSupplier.get(), entry.getKey(), entry.getValue(), formatVersion));
      }
    }

    return new DeltaManifests(dataManifest, deleteManifests, result.referencedDataFiles());
  }

  private static ManifestFile writeDeleteFiles(
      OutputFile outputFile, PartitionSpec spec, List<DeleteFile> deleteFiles, int formatVersion)
      throws IOException {
    ManifestWriter<DeleteFile> deleteManifestWriter =
        ManifestFiles.writeDeleteManifest(formatVersion, spec, outputFile, DUMMY_SNAPSHOT_ID);
    try (ManifestWriter<DeleteFile> writer = deleteManifestWriter) {
      for (DeleteFile deleteFile : deleteFiles) {
        writer.add(deleteFile);
      }
    }

    return deleteManifestWriter.toManifestFile();
  }

  public static WriteResult readCompletedFiles(
      DeltaManifests deltaManifests, FileIO io, Map<Integer, PartitionSpec> specsById)
      throws IOException {
    WriteResult.Builder builder = WriteResult.builder();

    // Read the completed data files from persisted data manifest file.
    if (deltaManifests.dataManifest() != null) {
      builder.addDataFiles(readDataFiles(deltaManifests.dataManifest(), io, specsById));
    }

    for (ManifestFile deleteManifest : deltaManifests.deleteManifests()) {
      if (!specsById.containsKey(deleteManifest.partitionSpecId())) {
        LOG.warn(
            "Skipping delete manifest {} for removed partition spec {}",
            deleteManifest.path(),
            deleteManifest.partitionSpecId());
        continue;
      }

      try (CloseableIterable<DeleteFile> deleteFiles =
          ManifestFiles.readDeleteManifest(deleteManifest, io, specsById)) {
        builder.addDeleteFiles(deleteFiles);
      }
    }

    return builder.addReferencedDataFiles(deltaManifests.referencedDataFiles()).build();
  }

  public static void deleteCommittedManifests(
      Table table, List<ManifestFile> manifests, String newFlinkJobId, long checkpointId) {
    deleteCommittedManifests(table.name(), table.io(), manifests, newFlinkJobId, checkpointId);
  }

  static void deleteCommittedManifests(
      String tableName,
      FileIO io,
      List<ManifestFile> manifestsPath,
      String newFlinkJobId,
      long checkpointId) {
    for (ManifestFile manifest : manifestsPath) {
      try {
        io.deleteFile(manifest.path());
      } catch (Exception e) {
        // The flink manifests cleaning failure shouldn't abort the completed checkpoint.
        String details =
            MoreObjects.toStringHelper(FlinkManifestUtil.class)
                .add("tableName", tableName)
                .add("flinkJobId", newFlinkJobId)
                .add("checkpointId", checkpointId)
                .add("manifestPath", manifest)
                .toString();
        LOG.warn(
            "The iceberg transaction has been committed, but we failed to clean the temporary flink manifests: {}",
            details,
            e);
      }
    }
  }
}
