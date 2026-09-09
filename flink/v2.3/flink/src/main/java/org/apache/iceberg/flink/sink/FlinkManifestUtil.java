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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
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
    ManifestFile dataManifest = writeDataFiles(result, outputFileSupplier, spec, formatVersion);

    // Write the completed delete files into a newly created delete manifest file.
    ManifestFile deleteManifest = null;
    if (result.deleteFiles() != null && result.deleteFiles().length > 0) {
      deleteManifest =
          writeDeleteFiles(
              outputFileSupplier.get(),
              spec,
              Lists.newArrayList(result.deleteFiles()),
              formatVersion);
    }

    return new DeltaManifests(dataManifest, deleteManifest, result.referencedDataFiles());
  }

  /**
   * Write the {@link WriteResult} of the DV-only write path to temporary manifest files.
   *
   * <p>Its deletion vectors reference data files of any partition spec the table ever had, and a
   * delete file has to be tracked by a manifest of its own spec: reading it through any other spec
   * would give it the wrong partition. The delete files and the ones they replace are therefore
   * written to one manifest per spec.
   *
   * @param result the files of one checkpoint; the data files all belong to {@code dataSpec}
   * @param specsById every spec the delete files belong to
   * @param baselineSnapshotId snapshot the deletes were resolved against
   */
  static DeltaManifests writeCompletedFiles(
      WriteResult result,
      Supplier<OutputFile> outputFileSupplier,
      PartitionSpec dataSpec,
      Map<Integer, PartitionSpec> specsById,
      int formatVersion,
      Long baselineSnapshotId)
      throws IOException {
    ManifestFile dataManifest = writeDataFiles(result, outputFileSupplier, dataSpec, formatVersion);
    List<ManifestFile> deleteManifests =
        writeDeleteFilesBySpec(result.deleteFiles(), outputFileSupplier, specsById, formatVersion);
    // Delete files superseded by the ones above, tracked so that the committer can drop them.
    List<ManifestFile> rewrittenDeleteManifests =
        writeDeleteFilesBySpec(
            result.rewrittenDeleteFiles(), outputFileSupplier, specsById, formatVersion);

    return new DeltaManifests(
        dataManifest,
        deleteManifests,
        rewrittenDeleteManifests,
        result.referencedDataFiles(),
        baselineSnapshotId);
  }

  private static ManifestFile writeDataFiles(
      WriteResult result,
      Supplier<OutputFile> outputFileSupplier,
      PartitionSpec spec,
      int formatVersion)
      throws IOException {
    if (result.dataFiles() == null || result.dataFiles().length == 0) {
      return null;
    }

    return writeDataFiles(
        outputFileSupplier.get(), spec, Lists.newArrayList(result.dataFiles()), formatVersion);
  }

  private static List<ManifestFile> writeDeleteFilesBySpec(
      DeleteFile[] deleteFiles,
      Supplier<OutputFile> outputFileSupplier,
      Map<Integer, PartitionSpec> specsById,
      int formatVersion)
      throws IOException {
    if (deleteFiles == null || deleteFiles.length == 0) {
      return ImmutableList.of();
    }

    Map<Integer, List<DeleteFile>> filesBySpec = Maps.newTreeMap();
    for (DeleteFile deleteFile : deleteFiles) {
      filesBySpec.computeIfAbsent(deleteFile.specId(), id -> Lists.newArrayList()).add(deleteFile);
    }

    List<ManifestFile> manifests = Lists.newArrayListWithCapacity(filesBySpec.size());
    for (Map.Entry<Integer, List<DeleteFile>> entry : filesBySpec.entrySet()) {
      PartitionSpec spec = specsById.get(entry.getKey());
      Preconditions.checkState(
          spec != null, "Cannot find partition spec %s of a delete file", entry.getKey());
      manifests.add(
          writeDeleteFiles(outputFileSupplier.get(), spec, entry.getValue(), formatVersion));
    }

    return manifests;
  }

  private static ManifestFile writeDeleteFiles(
      OutputFile outputFile, PartitionSpec spec, List<DeleteFile> deleteFiles, int formatVersion)
      throws IOException {
    ManifestWriter<DeleteFile> writer =
        ManifestFiles.writeDeleteManifest(formatVersion, spec, outputFile, DUMMY_SNAPSHOT_ID);
    try (ManifestWriter<DeleteFile> closeableWriter = writer) {
      for (DeleteFile deleteFile : deleteFiles) {
        closeableWriter.add(deleteFile);
      }
    }

    return writer.toManifestFile();
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
    for (ManifestFile manifest : deltaManifests.deleteManifests()) {
      try (CloseableIterable<DeleteFile> deleteFiles =
          ManifestFiles.readDeleteManifest(manifest, io, specsById)) {
        builder.addDeleteFiles(deleteFiles);
      }
    }

    for (ManifestFile manifest : deltaManifests.rewrittenDeleteManifests()) {
      try (CloseableIterable<DeleteFile> rewritten =
          ManifestFiles.readDeleteManifest(manifest, io, specsById)) {
        builder.addRewrittenDeleteFiles(rewritten);
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
