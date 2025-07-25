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
package org.apache.iceberg;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for Rewrite table path action. */
public class RewriteTablePathUtil {

  private static final Logger LOG = LoggerFactory.getLogger(RewriteTablePathUtil.class);
  // Use the POSIX separator instead of File.separator because File.separator is dependent on
  // the client environment and not the target filesystem. POSIX is compatible with S3, GCS, etc
  public static final String FILE_SEPARATOR = "/";

  private RewriteTablePathUtil() {}

  /**
   * Rewrite result.
   *
   * @param <T> type of file to rewrite
   */
  public static class RewriteResult<T> implements Serializable {
    private final Set<T> toRewrite = Sets.newHashSet();
    private final Set<Pair<String, String>> copyPlan = Sets.newHashSet();

    public RewriteResult() {}

    public RewriteResult<T> append(RewriteResult<T> r1) {
      toRewrite.addAll(r1.toRewrite);
      copyPlan.addAll(r1.copyPlan);
      return this;
    }

    /** Returns next list of files to rewrite (discovered by rewriting this file) */
    public Set<T> toRewrite() {
      return toRewrite;
    }

    /**
     * Returns a copy plan of files whose metadata were rewritten, for each file a source and target
     * location
     */
    public Set<Pair<String, String>> copyPlan() {
      return copyPlan;
    }
  }

  /**
   * Create a new table metadata object, replacing path references
   *
   * @param metadata source table metadata
   * @param sourcePrefix source prefix that will be replaced
   * @param targetPrefix target prefix that will replace it
   * @return copy of table metadata with paths replaced
   */
  public static TableMetadata replacePaths(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    String newLocation = metadata.location().replaceFirst(sourcePrefix, targetPrefix);
    List<Snapshot> newSnapshots = updatePathInSnapshots(metadata, sourcePrefix, targetPrefix);
    List<TableMetadata.MetadataLogEntry> metadataLogEntries =
        updatePathInMetadataLogs(metadata, sourcePrefix, targetPrefix);
    long snapshotId =
        metadata.currentSnapshot() == null ? -1 : metadata.currentSnapshot().snapshotId();
    Map<String, String> properties =
        updateProperties(metadata.properties(), sourcePrefix, targetPrefix);

    return new TableMetadata(
        null,
        metadata.formatVersion(),
        metadata.uuid(),
        newLocation,
        metadata.lastSequenceNumber(),
        metadata.lastUpdatedMillis(),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.schemas(),
        metadata.defaultSpecId(),
        metadata.specs(),
        metadata.lastAssignedPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.sortOrders(),
        properties,
        snapshotId,
        newSnapshots,
        null,
        metadata.snapshotLog(),
        metadataLogEntries,
        metadata.refs(),
        updatePathInStatisticsFiles(metadata.statisticsFiles(), sourcePrefix, targetPrefix),
        // TODO: update partition statistics file paths
        metadata.partitionStatisticsFiles(),
        metadata.nextRowId(),
        metadata.encryptionKeys(),
        metadata.changes());
  }

  private static Map<String, String> updateProperties(
      Map<String, String> tableProperties, String sourcePrefix, String targetPrefix) {
    Map<String, String> properties = Maps.newHashMap(tableProperties);
    updatePathInProperty(properties, sourcePrefix, targetPrefix, TableProperties.OBJECT_STORE_PATH);
    updatePathInProperty(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
    updatePathInProperty(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_DATA_LOCATION);
    updatePathInProperty(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_METADATA_LOCATION);

    return properties;
  }

  private static void updatePathInProperty(
      Map<String, String> properties,
      String sourcePrefix,
      String targetPrefix,
      String propertyName) {
    if (properties.containsKey(propertyName)) {
      properties.put(
          propertyName, newPath(properties.get(propertyName), sourcePrefix, targetPrefix));
    }
  }

  private static List<StatisticsFile> updatePathInStatisticsFiles(
      List<StatisticsFile> statisticsFiles, String sourcePrefix, String targetPrefix) {
    return statisticsFiles.stream()
        .map(
            existing ->
                new GenericStatisticsFile(
                    existing.snapshotId(),
                    newPath(existing.path(), sourcePrefix, targetPrefix),
                    existing.fileSizeInBytes(),
                    existing.fileFooterSizeInBytes(),
                    existing.blobMetadata()))
        .collect(Collectors.toList());
  }

  private static List<TableMetadata.MetadataLogEntry> updatePathInMetadataLogs(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    List<TableMetadata.MetadataLogEntry> metadataLogEntries =
        Lists.newArrayListWithCapacity(metadata.previousFiles().size());
    for (TableMetadata.MetadataLogEntry metadataLog : metadata.previousFiles()) {
      TableMetadata.MetadataLogEntry newMetadataLog =
          new TableMetadata.MetadataLogEntry(
              metadataLog.timestampMillis(),
              newPath(metadataLog.file(), sourcePrefix, targetPrefix));
      metadataLogEntries.add(newMetadataLog);
    }
    return metadataLogEntries;
  }

  private static List<Snapshot> updatePathInSnapshots(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    List<Snapshot> newSnapshots = Lists.newArrayListWithCapacity(metadata.snapshots().size());
    for (Snapshot snapshot : metadata.snapshots()) {
      String newManifestListLocation =
          newPath(snapshot.manifestListLocation(), sourcePrefix, targetPrefix);
      Snapshot newSnapshot =
          new BaseSnapshot(
              snapshot.sequenceNumber(),
              snapshot.snapshotId(),
              snapshot.parentId(),
              snapshot.timestampMillis(),
              snapshot.operation(),
              snapshot.summary(),
              snapshot.schemaId(),
              newManifestListLocation,
              snapshot.firstRowId(),
              snapshot.addedRows(),
              snapshot.keyId());
      newSnapshots.add(newSnapshot);
    }
    return newSnapshots;
  }

  /**
   * Rewrite a manifest list representing a snapshot, replacing path references.
   *
   * @param snapshot snapshot represented by the manifest list
   * @param io file io
   * @param tableMetadata metadata of table
   * @param manifestsToRewrite a list of manifest files to filter for rewrite
   * @param sourcePrefix source prefix that will be replaced
   * @param targetPrefix target prefix that will replace it
   * @param stagingDir staging directory
   * @param outputPath location to write the manifest list
   * @return a copy plan for manifest files whose metadata were contained in the rewritten manifest
   *     list
   */
  public static RewriteResult<ManifestFile> rewriteManifestList(
      Snapshot snapshot,
      FileIO io,
      TableMetadata tableMetadata,
      Set<String> manifestsToRewrite,
      String sourcePrefix,
      String targetPrefix,
      String stagingDir,
      String outputPath) {
    RewriteResult<ManifestFile> result = new RewriteResult<>();
    OutputFile outputFile = io.newOutputFile(outputPath);

    List<ManifestFile> manifestFiles = manifestFilesInSnapshot(io, snapshot);
    manifestFiles.forEach(
        mf ->
            Preconditions.checkArgument(
                mf.path().startsWith(sourcePrefix),
                "Encountered manifest file %s not under the source prefix %s",
                mf.path(),
                sourcePrefix));

    try (FileAppender<ManifestFile> writer =
        ManifestLists.write(
            tableMetadata.formatVersion(),
            outputFile,
            snapshot.snapshotId(),
            snapshot.parentId(),
            snapshot.sequenceNumber(),
            snapshot.firstRowId())) {

      for (ManifestFile file : manifestFiles) {
        ManifestFile newFile = file.copy();
        ((StructLike) newFile).set(0, newPath(newFile.path(), sourcePrefix, targetPrefix));
        writer.add(newFile);

        if (manifestsToRewrite.contains(file.path())) {
          result.toRewrite().add(file);
          result
              .copyPlan()
              .add(Pair.of(stagingPath(file.path(), sourcePrefix, stagingDir), newFile.path()));
        }
      }
      return result;
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to rewrite the manifest list file " + snapshot.manifestListLocation(), e);
    }
  }

  private static List<ManifestFile> manifestFilesInSnapshot(FileIO io, Snapshot snapshot) {
    String path = snapshot.manifestListLocation();
    List<ManifestFile> manifestFiles = Lists.newLinkedList();
    try {
      manifestFiles = ManifestLists.read(io.newInputFile(path));
    } catch (RuntimeIOException e) {
      LOG.warn("Failed to read manifest list {}", path, e);
    }
    return manifestFiles;
  }

  /**
   * Rewrite a data manifest, replacing path references.
   *
   * @param manifestFile source manifest file to rewrite
   * @param outputFile output file to rewrite manifest file to
   * @param io file io
   * @param format format of the manifest file
   * @param specsById map of partition specs by id
   * @param sourcePrefix source prefix that will be replaced
   * @param targetPrefix target prefix that will replace it
   * @return a copy plan of content files in the manifest that was rewritten
   * @deprecated since 1.10.0, will be removed in 1.11.0
   */
  @Deprecated
  public static RewriteResult<DataFile> rewriteDataManifest(
      ManifestFile manifestFile,
      OutputFile outputFile,
      FileIO io,
      int format,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix)
      throws IOException {
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());
    try (ManifestWriter<DataFile> writer =
            ManifestFiles.write(format, spec, outputFile, manifestFile.snapshotId());
        ManifestReader<DataFile> reader =
            ManifestFiles.read(manifestFile, io, specsById).select(Arrays.asList("*"))) {
      return StreamSupport.stream(reader.entries().spliterator(), false)
          .map(
              entry ->
                  writeDataFileEntry(entry, Set.of(), spec, sourcePrefix, targetPrefix, writer))
          .reduce(new RewriteResult<>(), RewriteResult::append);
    }
  }

  /**
   * Rewrite a data manifest, replacing path references.
   *
   * @param manifestFile source manifest file to rewrite
   * @param snapshotIds snapshot ids for filtering returned data manifest entries
   * @param outputFile output file to rewrite manifest file to
   * @param io file io
   * @param format format of the manifest file
   * @param specsById map of partition specs by id
   * @param sourcePrefix source prefix that will be replaced
   * @param targetPrefix target prefix that will replace it
   * @return a copy plan of content files in the manifest that was rewritten
   */
  public static RewriteResult<DataFile> rewriteDataManifest(
      ManifestFile manifestFile,
      Set<Long> snapshotIds,
      OutputFile outputFile,
      FileIO io,
      int format,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix)
      throws IOException {
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());
    try (ManifestWriter<DataFile> writer =
            ManifestFiles.write(format, spec, outputFile, manifestFile.snapshotId());
        ManifestReader<DataFile> reader =
            ManifestFiles.read(manifestFile, io, specsById).select(Arrays.asList("*"))) {
      return StreamSupport.stream(reader.entries().spliterator(), false)
          .map(
              entry ->
                  writeDataFileEntry(entry, snapshotIds, spec, sourcePrefix, targetPrefix, writer))
          .reduce(new RewriteResult<>(), RewriteResult::append);
    }
  }

  /**
   * Rewrite a delete manifest, replacing path references.
   *
   * @param manifestFile source delete manifest to rewrite
   * @param outputFile output file to rewrite manifest file to
   * @param io file io
   * @param format format of the manifest file
   * @param specsById map of partition specs by id
   * @param sourcePrefix source prefix that will be replaced
   * @param targetPrefix target prefix that will replace it
   * @param stagingLocation staging location for rewritten files (referred delete file will be
   *     rewritten here)
   * @return a copy plan of content files in the manifest that was rewritten
   * @deprecated since 1.10.0, will be removed in 1.11.0
   */
  @Deprecated
  public static RewriteResult<DeleteFile> rewriteDeleteManifest(
      ManifestFile manifestFile,
      OutputFile outputFile,
      FileIO io,
      int format,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix,
      String stagingLocation)
      throws IOException {
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());
    try (ManifestWriter<DeleteFile> writer =
            ManifestFiles.writeDeleteManifest(format, spec, outputFile, manifestFile.snapshotId());
        ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifestFile, io, specsById)
                .select(Arrays.asList("*"))) {
      return StreamSupport.stream(reader.entries().spliterator(), false)
          .map(
              entry ->
                  writeDeleteFileEntry(
                      entry, Set.of(), spec, sourcePrefix, targetPrefix, stagingLocation, writer))
          .reduce(new RewriteResult<>(), RewriteResult::append);
    }
  }

  /**
   * Rewrite a delete manifest, replacing path references.
   *
   * @param manifestFile source delete manifest to rewrite
   * @param snapshotIds snapshot ids for filtering returned delete manifest entries
   * @param outputFile output file to rewrite manifest file to
   * @param io file io
   * @param format format of the manifest file
   * @param specsById map of partition specs by id
   * @param sourcePrefix source prefix that will be replaced
   * @param targetPrefix target prefix that will replace it
   * @param stagingLocation staging location for rewritten files (referred delete file will be
   *     rewritten here)
   * @return a copy plan of content files in the manifest that was rewritten
   */
  public static RewriteResult<DeleteFile> rewriteDeleteManifest(
      ManifestFile manifestFile,
      Set<Long> snapshotIds,
      OutputFile outputFile,
      FileIO io,
      int format,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix,
      String stagingLocation)
      throws IOException {
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());
    try (ManifestWriter<DeleteFile> writer =
            ManifestFiles.writeDeleteManifest(format, spec, outputFile, manifestFile.snapshotId());
        ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifestFile, io, specsById)
                .select(Arrays.asList("*"))) {
      return StreamSupport.stream(reader.entries().spliterator(), false)
          .map(
              entry ->
                  writeDeleteFileEntry(
                      entry,
                      snapshotIds,
                      spec,
                      sourcePrefix,
                      targetPrefix,
                      stagingLocation,
                      writer))
          .reduce(new RewriteResult<>(), RewriteResult::append);
    }
  }

  private static RewriteResult<DataFile> writeDataFileEntry(
      ManifestEntry<DataFile> entry,
      Set<Long> snapshotIds,
      PartitionSpec spec,
      String sourcePrefix,
      String targetPrefix,
      ManifestWriter<DataFile> writer) {
    RewriteResult<DataFile> result = new RewriteResult<>();
    DataFile dataFile = entry.file();
    String sourceDataFilePath = dataFile.location();
    Preconditions.checkArgument(
        sourceDataFilePath.startsWith(sourcePrefix),
        "Encountered data file %s not under the source prefix %s",
        sourceDataFilePath,
        sourcePrefix);
    String targetDataFilePath = newPath(sourceDataFilePath, sourcePrefix, targetPrefix);
    DataFile newDataFile =
        DataFiles.builder(spec).copy(entry.file()).withPath(targetDataFilePath).build();
    appendEntryWithFile(entry, writer, newDataFile);
    // keep the following entries in metadata but exclude them from copyPlan
    // 1) deleted data files
    // 2) entries not changed by snapshotIds
    if (entry.isLive() && snapshotIds.contains(entry.snapshotId())) {
      result.copyPlan().add(Pair.of(sourceDataFilePath, newDataFile.location()));
    }
    return result;
  }

  private static RewriteResult<DeleteFile> writeDeleteFileEntry(
      ManifestEntry<DeleteFile> entry,
      Set<Long> snapshotIds,
      PartitionSpec spec,
      String sourcePrefix,
      String targetPrefix,
      String stagingLocation,
      ManifestWriter<DeleteFile> writer) {

    DeleteFile file = entry.file();
    RewriteResult<DeleteFile> result = new RewriteResult<>();

    switch (file.content()) {
      case POSITION_DELETES:
        String targetDeleteFilePath = newPath(file.location(), sourcePrefix, targetPrefix);
        Metrics metricsWithTargetPath =
            ContentFileUtil.replacePathBounds(file, sourcePrefix, targetPrefix);
        DeleteFile movedFile =
            FileMetadata.deleteFileBuilder(spec)
                .copy(file)
                .withPath(targetDeleteFilePath)
                .withMetrics(metricsWithTargetPath)
                .build();
        appendEntryWithFile(entry, writer, movedFile);
        // keep the following entries in metadata but exclude them from copyPlan
        // 1) deleted position delete files
        // 2) entries not changed by snapshotIds
        if (entry.isLive() && snapshotIds.contains(entry.snapshotId())) {
          result
              .copyPlan()
              .add(
                  Pair.of(
                      stagingPath(file.location(), sourcePrefix, stagingLocation),
                      movedFile.location()));
        }
        result.toRewrite().add(file);
        return result;
      case EQUALITY_DELETES:
        DeleteFile eqDeleteFile = newEqualityDeleteEntry(file, spec, sourcePrefix, targetPrefix);
        appendEntryWithFile(entry, writer, eqDeleteFile);
        // keep the following entries in metadata but exclude them from copyPlan
        // 1) deleted equality delete files
        // 2) entries not changed by snapshotIds
        if (entry.isLive() && snapshotIds.contains(entry.snapshotId())) {
          // No need to rewrite equality delete files as they do not contain absolute file paths.
          result.copyPlan().add(Pair.of(file.location(), eqDeleteFile.location()));
        }
        return result;

      default:
        throw new UnsupportedOperationException("Unsupported delete file type: " + file.content());
    }
  }

  private static <F extends ContentFile<F>> void appendEntryWithFile(
      ManifestEntry<F> entry, ManifestWriter<F> writer, F file) {

    switch (entry.status()) {
      case ADDED:
        writer.add(file);
        break;
      case EXISTING:
        writer.existing(
            file, entry.snapshotId(), entry.dataSequenceNumber(), entry.fileSequenceNumber());
        break;
      case DELETED:
        writer.delete(file, entry.dataSequenceNumber(), entry.fileSequenceNumber());
        break;
    }
  }

  private static DeleteFile newEqualityDeleteEntry(
      DeleteFile file, PartitionSpec spec, String sourcePrefix, String targetPrefix) {
    String path = file.location();

    if (!path.startsWith(sourcePrefix)) {
      throw new UnsupportedOperationException(
          "Expected delete file to be under the source prefix: "
              + sourcePrefix
              + " but was "
              + path);
    }
    int[] equalityFieldIds = file.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray();
    String newPath = newPath(path, sourcePrefix, targetPrefix);
    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes(equalityFieldIds)
        .copy(file)
        .withPath(newPath)
        .withSplitOffsets(file.splitOffsets())
        .build();
  }

  /** Class providing engine-specific methods to read and write position delete files. */
  public interface PositionDeleteReaderWriter extends Serializable {
    CloseableIterable<Record> reader(InputFile inputFile, FileFormat format, PartitionSpec spec);

    PositionDeleteWriter<Record> writer(
        OutputFile outputFile,
        FileFormat format,
        PartitionSpec spec,
        StructLike partition,
        Schema rowSchema)
        throws IOException;
  }

  /**
   * Rewrite a position delete file, replacing path references.
   *
   * @param deleteFile source delete file to be rewritten
   * @param outputFile output file to rewrite delete file to
   * @param io file io
   * @param spec spec of delete file
   * @param sourcePrefix source prefix that will be replaced
   * @param targetPrefix target prefix to replace it
   * @param posDeleteReaderWriter class to read and write position delete files
   */
  public static void rewritePositionDeleteFile(
      DeleteFile deleteFile,
      OutputFile outputFile,
      FileIO io,
      PartitionSpec spec,
      String sourcePrefix,
      String targetPrefix,
      PositionDeleteReaderWriter posDeleteReaderWriter)
      throws IOException {
    String path = deleteFile.location();
    if (!path.startsWith(sourcePrefix)) {
      throw new UnsupportedOperationException(
          String.format("Expected delete file %s to start with prefix: %s", path, sourcePrefix));
    }
    InputFile sourceFile = io.newInputFile(path);
    try (CloseableIterable<Record> reader =
        posDeleteReaderWriter.reader(sourceFile, deleteFile.format(), spec)) {
      Record record = null;
      Schema rowSchema = null;
      CloseableIterator<Record> recordIt = reader.iterator();

      if (recordIt.hasNext()) {
        record = recordIt.next();
        rowSchema = record.get(2) != null ? spec.schema() : null;
      }

      if (record != null) {
        try (PositionDeleteWriter<Record> writer =
            posDeleteReaderWriter.writer(
                outputFile, deleteFile.format(), spec, deleteFile.partition(), rowSchema)) {

          writer.write(newPositionDeleteRecord(record, sourcePrefix, targetPrefix));

          while (recordIt.hasNext()) {
            record = recordIt.next();
            if (record != null) {
              writer.write(newPositionDeleteRecord(record, sourcePrefix, targetPrefix));
            }
          }
        }
      }
    }
  }

  private static PositionDelete newPositionDeleteRecord(
      Record record, String sourcePrefix, String targetPrefix) {
    PositionDelete delete = PositionDelete.create();
    String oldPath = (String) record.get(0);
    if (!oldPath.startsWith(sourcePrefix)) {
      throw new UnsupportedOperationException(
          "Expected delete file to be under the source prefix: "
              + sourcePrefix
              + " but was "
              + oldPath);
    }
    String newPath = newPath(oldPath, sourcePrefix, targetPrefix);
    delete.set(newPath, (Long) record.get(1), record.get(2));
    return delete;
  }

  /**
   * Replace path reference
   *
   * @param path path reference
   * @param sourcePrefix source prefix that will be replaced
   * @param targetPrefix target prefix that will replace it
   * @return new path reference
   */
  public static String newPath(String path, String sourcePrefix, String targetPrefix) {
    return combinePaths(targetPrefix, relativize(path, sourcePrefix));
  }

  /** Combine a base and relative path. */
  public static String combinePaths(String absolutePath, String relativePath) {
    return maybeAppendFileSeparator(absolutePath) + relativePath;
  }

  /** Returns the file name of a path. */
  public static String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(FILE_SEPARATOR);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }

  /** Relativize a path. */
  public static String relativize(String path, String prefix) {
    String toRemove = maybeAppendFileSeparator(prefix);
    if (!path.startsWith(toRemove)) {
      throw new IllegalArgumentException(
          String.format("Path %s does not start with %s", path, toRemove));
    }
    return path.substring(toRemove.length());
  }

  public static String maybeAppendFileSeparator(String path) {
    return path.endsWith(FILE_SEPARATOR) ? path : path + FILE_SEPARATOR;
  }

  /**
   * Construct a staging path under a given staging directory
   *
   * @param originalPath source path
   * @param stagingDir staging directory
   * @return a staging path under the staging directory, based on the original path
   * @deprecated since 1.10.0, will be removed in 1.11.0. Use {@link #stagingPath(String, String,
   *     String)} instead to avoid filename conflicts
   */
  @Deprecated
  public static String stagingPath(String originalPath, String stagingDir) {
    return stagingDir + fileName(originalPath);
  }

  /**
   * Construct a staging path under a given staging directory, preserving relative directory
   * structure to avoid conflicts when multiple files have the same name but different paths.
   *
   * @param originalPath source path
   * @param sourcePrefix source prefix to be replaced
   * @param stagingDir staging directory
   * @return a staging path under the staging directory that preserves the relative path structure
   */
  public static String stagingPath(String originalPath, String sourcePrefix, String stagingDir) {
    String relativePath = relativize(originalPath, sourcePrefix);
    return combinePaths(stagingDir, relativePath);
  }
}
