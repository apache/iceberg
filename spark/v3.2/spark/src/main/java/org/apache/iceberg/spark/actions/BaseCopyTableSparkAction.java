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

package org.apache.iceberg.spark.actions;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestLists;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableMetadataUtil;
import org.apache.iceberg.actions.BaseCopyTableActionResult;
import org.apache.iceberg.actions.CopyTable;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseCopyTableSparkAction
    extends BaseSparkAction<CopyTable, CopyTable.Result> implements CopyTable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseCopyTableSparkAction.class);
  private static final String DATA_FILE_LIST_DIR = "data-file-list-to-move";
  private static final String METADATA_FILE_LIST_DIR = "metadata-file-list-to-move";

  private final Table table;
  private final Set<String> metadataFilesToMove = Collections.synchronizedSet(Sets.newHashSet());
  private final Set<String> manifestFilePaths = Collections.synchronizedSet(Sets.newHashSet());
  private final Set<ManifestFile> manifestFilesToRewrite = Collections.synchronizedSet(Sets.newHashSet());
  private String dataFileListPath = null;
  private String metadataFileListPath = null;
  private final boolean enabledPME;

  private String sourcePrefix = "";
  private String targetPrefix = "";
  private String startVersion = "";
  private String endVersion = "";
  private String stagingDir = "";
  private Table targetTable = null;

  private Table startStaticTable = null;
  private Table endStaticTable = null;

  public BaseCopyTableSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    enabledPME = table.properties().containsKey("parquet.encryption.footer.key");
  }

  @Override
  public CopyTable rewriteLocationPrefix(String sPrefix, String tPrefix) {
    Preconditions.checkArgument(sPrefix != null && !sPrefix.isEmpty(), "Source prefix('%s') cannot be empty.", sPrefix);
    this.sourcePrefix = sPrefix;

    if (tPrefix != null) {
      this.targetPrefix = tPrefix;
    }
    return this;
  }

  @Override
  public CopyTable lastCopiedVersion(String sVersion) {
    Preconditions.checkArgument(sVersion != null && !sVersion.trim().isEmpty(),
        "Last copied version('%s') cannot be empty.", sVersion);
    this.startVersion = sVersion;
    return this;
  }

  @Override
  public CopyTable endVersion(String eVersion) {
    Preconditions.checkArgument(eVersion != null && !eVersion.trim().isEmpty(),
        "End version('%s') cannot be empty.", eVersion);
    this.endVersion = eVersion;
    return this;
  }

  @Override
  public CopyTable stagingLocation(String stagingLocation) {
    Preconditions.checkArgument(stagingLocation != null && !stagingLocation.isEmpty(),
        "Staging location('%s') cannot be empty.", stagingLocation);
    this.stagingDir = stagingLocation;
    return this;
  }

  @Override
  public CopyTable targetTable(Table tgtTable) {
    this.targetTable = tgtTable;
    return this;
  }

  @Override
  protected CopyTable self() {
    return this;
  }

  @Override
  public Result execute() {
    validateInputs();
    JobGroupInfo info = newJobGroupInfo("COPY-TABLE", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private CopyTable.Result doExecute() {
    rebuildMetaData();
    return new BaseCopyTableActionResult(dataFileListPath, metadataFileListPath, fileName(endVersion));
  }

  private void validateInputs() {
    Preconditions.checkArgument(sourcePrefix != null && !sourcePrefix.isEmpty(),
        "Source prefix('%s') cannot be empty.", sourcePrefix);

    validateAndSetEndVersion();

    endStaticTable = newStaticTable(endVersion, table.io());

    TableMetadata tableMetadata = ((HasTableOperations) endStaticTable).operations().current();
    Preconditions.checkArgument(tableMetadata.formatVersion() == 1, "Support Iceberg format version 1 only.");

    validateAndSetStartVersion(tableMetadata);

    if (fileExist(startVersion)) {
      startStaticTable = newStaticTable(startVersion, table.io());
    }

    if (stagingDir.isEmpty()) {
      stagingDir = getMetadataLocation(table) + "copy-table-staging-" + UUID.randomUUID() + "/";
    } else if (!stagingDir.endsWith("/")) {
      stagingDir = stagingDir + "/";
    }
  }

  private void validateAndSetEndVersion() {
    if (endVersion.isEmpty()) {
      endVersion = currentMetadataPath(table);
    } else {
      TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();
      if (versionInFilePath(tableMetadata.metadataFileLocation(), endVersion)) {
        endVersion = tableMetadata.metadataFileLocation();
      }
      for (MetadataLogEntry metadataLogEntry : tableMetadata.previousFiles()) {
        if (versionInFilePath(metadataLogEntry.file(), endVersion)) {
          endVersion = metadataLogEntry.file();
          break;
        }
      }

      Preconditions.checkArgument(fileExist(endVersion), "Cannot find the end version('%s') in the current version " +
          "files", endVersion);
    }
  }

  private void validateAndSetStartVersion(TableMetadata tableMetadata) {
    if (startVersion.isEmpty()) {
      if (targetTable == null) {
        LOG.warn("No input of the start version. Will do a full copy.");
      } else {
        String tgtTableCurrentVersion = fileName(currentMetadataPath(targetTable));

        for (MetadataLogEntry metadataLogEntry : tableMetadata.previousFiles()) {
          if (metadataLogEntry.file().endsWith(tgtTableCurrentVersion)) {
            startVersion = metadataLogEntry.file();
            break;
          }
        }

        if (fileNotExist(startVersion)) {
          throw new IllegalArgumentException("Cannot find the current version of target table in the source table. " +
              "Please make sure the target table is a subset of source table.");
        }
      }
    } else {
      for (MetadataLogEntry metadataLogEntry : tableMetadata.previousFiles()) {
        if (versionInFilePath(metadataLogEntry.file(), startVersion)) {
          startVersion = metadataLogEntry.file();
          break;
        }
      }

      Preconditions.checkArgument(fileExist(startVersion), "Start version('%s') is NOT valid.", startVersion);

      if (targetTable != null && !fileName(startVersion).equals(fileName(currentMetadataPath(targetTable)))) {
        throw new IllegalArgumentException("The start version isn't the current version of the target table. " +
            "Please make sure the target table is a subset of source table.");
      }
    }
  }

  private boolean versionInFilePath(String path, String version) {
    return fileName(path).equals(version);
  }

  private String jobDesc() {
    if (startVersion.isEmpty()) {
      return String.format("Replacing path prefixes '%s' with '%s' in the metadata files of table %s," +
          "up to version '%s'.", sourcePrefix, targetPrefix, table.name(), endVersion);
    } else {
      return String.format("Replacing path prefixes '%s' with '%s' in the metadata files of table %s," +
          "from version '%s' to '%s'.", sourcePrefix, targetPrefix, table.name(), startVersion, endVersion);
    }
  }

  /**
   * Here are steps: 1. rebuild version files 2. rebuild manifest list files 3. rebuild manifest files 4. get all data
   * files need to move
   */
  private void rebuildMetaData() {
    TableMetadata tableMetadata = ((HasTableOperations) endStaticTable).operations().current();

    // rebuild version files
    Set<Long> allSnapshotIds = rewriteVersionFiles(tableMetadata);

    Set<Long> diffSnapshotIds = getDiffSnapshotIds(allSnapshotIds);

    // get all manifest file paths need to rewrite
    List<String> manifestFilePathToMove = manifestFilesToMove(diffSnapshotIds);
    manifestFilePaths.addAll(manifestFilePathToMove);

    // rebuild manifest-list files
    Set<Snapshot> validSnapshots = Sets.difference(snapshotSet(endVersion), snapshotSet(startVersion));
    validSnapshots.forEach(snapshot -> rewriteManifestList(snapshot, tableMetadata));

    // rebuild manifest files
    List<ManifestFile> newManifests = rewriteManifests(tableMetadata);
    newManifests.forEach(manifestFile -> addToRebuiltFiles(manifestFile.path()));
    saveMetadataFileList();

    Dataset<Row> dataFiles = getDiffDataFiles(diffSnapshotIds);
    saveDataFileList(dataFiles);
  }

  private void saveMetadataFileList() {
    List<String> fileList = Lists.newArrayList();
    fileList.addAll(metadataFilesToMove);
    Dataset<String> metadataFileList = spark().createDataset(fileList, Encoders.STRING());
    metadataFileListPath = stagingDir + METADATA_FILE_LIST_DIR;
    metadataFileList.repartition(1).write().mode(SaveMode.Overwrite).format("text").save(metadataFileListPath);
  }

  private void saveDataFileList(Dataset<Row> dataFiles) {
    dataFileListPath = stagingDir + DATA_FILE_LIST_DIR;

    try {
      dataFiles.repartition(1).write().mode(SaveMode.Overwrite).format("text").save(dataFileListPath);
    } catch (Exception e) {
      throw new UnsupportedOperationException("Failed to build the data files dataframe, the end version you are " +
          "trying to copy may contain invalid snapshots, please use the younger version which doesn't have invalid " +
          "snapshots", e);
    }
  }

  private Set<Long> getDiffSnapshotIds(Set<Long> allSnapshotIds) {
    Set<Long> snapshotIdsInStartVersion = Sets.newHashSet();
    if (startStaticTable != null) {
      startStaticTable.snapshots().forEach(snapshot -> snapshotIdsInStartVersion.add(snapshot.snapshotId()));
    }
    return Sets.difference(allSnapshotIds, snapshotIdsInStartVersion);
  }

  private Set<Long> rewriteVersionFiles(TableMetadata metadata) {
    Set<Long> allSnapshotIds = Sets.newHashSet();

    String stagingPath = stagingPath(endVersion, stagingDir);
    metadata.snapshots().forEach(snapshot -> allSnapshotIds.add(snapshot.snapshotId()));
    rewriteVersionFile(metadata, stagingPath);

    List<MetadataLogEntry> versions = metadata.previousFiles();
    for (int i = versions.size() - 1; i >= 0; i--) {
      String versionFilePath = versions.get(i).file();
      if (versionFilePath.equals(startVersion)) {
        break;
      }

      Preconditions.checkArgument(
          fileExist(versionFilePath),
          String.format("Version file %s doesn't exist", versionFilePath));
      String newPath = stagingPath(versionFilePath, stagingDir);
      TableMetadata tableMetadata = new StaticTableOperations(versionFilePath, table.io()).current();

      tableMetadata.snapshots().forEach(snapshot -> allSnapshotIds.add(snapshot.snapshotId()));

      rewriteVersionFile(tableMetadata, newPath);
    }

    return allSnapshotIds;
  }

  private Set<Snapshot> snapshotSet(String metadataPath) {
    Set<Snapshot> snapshots = Sets.newHashSet();
    if (!metadataPath.isEmpty()) {
      StaticTableOperations ops = new StaticTableOperations(metadataPath, table.io());
      TableMetadata metadata = ops.current();
      snapshots.addAll(metadata.snapshots());
    }
    return snapshots;
  }

  private void rewriteVersionFile(TableMetadata metadata, String stagingPath) {
    TableMetadata newTableMetadata = TableMetadataUtil.replacePaths(metadata, sourcePrefix, targetPrefix, table.io());
    TableMetadataParser.overwrite(newTableMetadata, table.io().newOutputFile(stagingPath));
    addToRebuiltFiles(stagingPath);
  }

  private void rewriteManifestList(Snapshot snapshot, TableMetadata tableMetadata) {
    List<ManifestFile> manifestFiles = manifestFilesInSnapshot(snapshot);
    String path = snapshot.manifestListLocation();
    String stagingPath = stagingPath(path, stagingDir);
    OutputFile outputFile = table.io().newOutputFile(stagingPath);
    try (FileAppender<ManifestFile> writer = ManifestLists.write(tableMetadata.formatVersion(), outputFile,
        snapshot.snapshotId(), snapshot.parentId(), snapshot.sequenceNumber())) {

      for (ManifestFile file : manifestFiles) {
        // need to get the ManifestFile object for manifest file rewriting
        if (manifestFilePaths.contains(file.path())) {
          manifestFilesToRewrite.add(file);
        }

        ManifestFile newFile = file.copy();
        if (newFile.path().startsWith(sourcePrefix)) {
          ((StructLike) newFile).set(0, newPath(newFile.path(), sourcePrefix, targetPrefix));
        }
        writer.add(newFile);
      }

      addToRebuiltFiles(stagingPath);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to rewrite the manifest list file " + path, e);
    }
  }

  private List<ManifestFile> manifestFilesInSnapshot(Snapshot snapshot) {
    String path = snapshot.manifestListLocation();
    List<ManifestFile> manifestFiles = Lists.newLinkedList();
    try {
      manifestFiles = ManifestLists.read(table.io().newInputFile(path));
    } catch (RuntimeIOException e) {
      LOG.warn("Failed to read manifest list {}", path, e);
    }
    return manifestFiles;
  }

  private List<String> manifestFilesToMove(Set<Long> diffSnapshotIds) {
    try {
      Dataset<Row> lastVersionFiles = buildManifestFileDF(endStaticTable);
      if (startStaticTable == null) {
        return lastVersionFiles.distinct()
            .as(Encoders.STRING())
            .collectAsList();
      } else {
        return lastVersionFiles.distinct()
            .filter(functions.column("added_snapshot_id").isInCollection(diffSnapshotIds))
            .as(Encoders.STRING())
            .collectAsList();
      }
    } catch (Exception e) {
      throw new UnsupportedOperationException("Failed to build the manifest files dataframe, the end version you are " +
          "trying to copy may contain invalid snapshots, please use the younger version which doesn't have invalid " +
          "snapshots", e);
    }
  }

  /**
   * Rewrite manifest files in a distributed manner.
   */
  private List<ManifestFile> rewriteManifests(TableMetadata tableMetadata) {
    if (manifestFilesToRewrite.isEmpty()) {
      return Lists.newArrayList();
    }

    Encoder<ManifestFile> manifestFileEncoder = Encoders.javaSerialization(ManifestFile.class);
    Dataset<ManifestFile> manifestDS =
        spark().createDataset(Lists.newArrayList(manifestFilesToRewrite), manifestFileEncoder);

    Broadcast<FileIO> io = sparkContext().broadcast(SparkUtil.serializableFileIO(table));
    Broadcast<Map<Integer, PartitionSpec>> specsById = sparkContext().broadcast(tableMetadata.specsById());

    return manifestDS
        .repartition(manifestFilesToRewrite.size())
        .mapPartitions(
            toManifests(io, stagingDir, tableMetadata.formatVersion(), specsById, sourcePrefix, targetPrefix),
            manifestFileEncoder
        )
        .collectAsList();
  }

  private static MapPartitionsFunction<ManifestFile, ManifestFile> toManifests(
      Broadcast<FileIO> io, String stagingLocation, int format, Broadcast<Map<Integer, PartitionSpec>> specsById,
      String sourcePrefix, String targetPrefix) {

    return rows -> {
      List<ManifestFile> manifests = Lists.newArrayList();
      while (rows.hasNext()) {
        manifests.add(writeManifest(rows.next(), io, stagingLocation, format, specsById, sourcePrefix, targetPrefix));
      }

      return manifests.iterator();
    };
  }

  private static ManifestFile writeManifest(
      ManifestFile manifestFile, Broadcast<FileIO> io, String stagingLocation, int format,
      Broadcast<Map<Integer, PartitionSpec>> specsById, String sourcePrefix, String targetPrefix) throws IOException {

    String stagingPath = stagingPath(manifestFile.path(), stagingLocation);
    OutputFile outputFile = io.value().newOutputFile(stagingPath);
    PartitionSpec spec = specsById.getValue().get(manifestFile.partitionSpecId());
    ManifestWriter<DataFile> writer = ManifestFiles.write(format, spec, outputFile, manifestFile.snapshotId());

    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifestFile, io.getValue(), specsById.getValue())
        .select(Arrays.asList("*"))) {
      reader.entries().forEach(entry -> appendEntry(entry, writer, spec, sourcePrefix, targetPrefix));
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  private static void appendEntry(
      ManifestEntry<DataFile> entry, ManifestWriter<DataFile> writer, PartitionSpec spec,
      String sourcePrefix, String targetPrefix) {
    DataFile dataFile = entry.file();
    String dataFilePath = dataFile.path().toString();
    if (dataFilePath.startsWith(sourcePrefix)) {
      dataFilePath = newPath(dataFilePath, sourcePrefix, targetPrefix);
      dataFile = DataFiles.builder(spec).copy(entry.file()).withPath(dataFilePath).build();
    }

    switch (entry.status()) {
      case ADDED:
        writer.add(dataFile);
        break;
      case EXISTING:
        writer.existing(dataFile, entry.snapshotId(), entry.sequenceNumber());
        break;
      case DELETED:
        writer.delete(dataFile);
        break;
    }
  }

  private Dataset<Row> getDiffDataFiles(Set<Long> diffSnapshotIds) {
    Dataset<Row> lastVersionFiles = buildValidDataFileDFWithSnapshotId(endStaticTable);
    if (startStaticTable == null) {
      return lastVersionFiles.distinct().select("file_path");
    } else {
      return lastVersionFiles.distinct()
          .filter(functions.column("snapshot_id").isInCollection(diffSnapshotIds))
          .select("file_path");
    }
  }

  private boolean fileNotExist(String path) {
    return !fileExist(path);
  }

  private boolean fileExist(String path) {
    if (path == null || path.trim().isEmpty()) {
      return false;
    }
    return table.io().newInputFile(path).exists();
  }

  private static String newPath(String path, String sourcePrefix, String targetPrefix) {
    return path.replaceFirst(sourcePrefix, targetPrefix);
  }

  private void addToRebuiltFiles(String path) {
    metadataFilesToMove.add(path);
  }

  private static String stagingPath(String originalPath, String stagingLocation) {
    return stagingLocation + fileName(originalPath);
  }

  private String currentMetadataPath(Table tbl) {
    return ((HasTableOperations) tbl).operations().current().metadataFileLocation();
  }

  private static String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(File.separator);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }

  private String getMetadataLocation(Table tbl) {
    String currentMetadataPath = ((HasTableOperations) tbl).operations().current().metadataFileLocation();
    int lastIndex = currentMetadataPath.lastIndexOf(File.separator);
    String metadataDir = "";
    if (lastIndex != -1) {
      metadataDir = currentMetadataPath.substring(0, lastIndex + 1);
    }

    Preconditions.checkArgument(!metadataDir.isEmpty(), "Failed to get the metadata file root directory");
    return metadataDir;
  }
}
