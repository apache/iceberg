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
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestLists;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
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

public class BaseCopyTableSparkAction extends BaseSparkAction<CopyTable> implements CopyTable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseCopyTableSparkAction.class);
  private static final String DATA_FILE_LIST_DIR = "data-file-list-to-move";
  private static final String METADATA_FILE_LIST_DIR = "metadata-file-list-to-move";

  private final Table table;
  private final Set<String> metadataFilesToMove = Collections.synchronizedSet(Sets.newHashSet());
  private final Set<String> manifestFilePaths = Collections.synchronizedSet(Sets.newHashSet());
  private final Set<ManifestFile> manifestFilesToRewrite =
      Collections.synchronizedSet(Sets.newHashSet());
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
    Preconditions.checkArgument(
        sPrefix != null && !sPrefix.isEmpty(), "Source prefix('%s') cannot be empty.", sPrefix);
    this.sourcePrefix = sPrefix;

    if (tPrefix != null) {
      this.targetPrefix = tPrefix;
    }
    return this;
  }

  @Override
  public CopyTable lastCopiedVersion(String sVersion) {
    Preconditions.checkArgument(
        sVersion != null && !sVersion.trim().isEmpty(),
        "Last copied version('%s') cannot be empty.",
        sVersion);
    this.startVersion = sVersion;
    return this;
  }

  @Override
  public CopyTable endVersion(String eVersion) {
    Preconditions.checkArgument(
        eVersion != null && !eVersion.trim().isEmpty(),
        "End version('%s') cannot be empty.",
        eVersion);
    this.endVersion = eVersion;
    return this;
  }

  @Override
  public CopyTable stagingLocation(String stagingLocation) {
    Preconditions.checkArgument(
        stagingLocation != null && !stagingLocation.isEmpty(),
        "Staging location('%s') cannot be empty.",
        stagingLocation);
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

  private Result doExecute() {
    rebuildMetaData();
    return new BaseCopyTableActionResult(
        stagingDir, dataFileListPath, metadataFileListPath, fileName(endVersion));
  }

  private void validateInputs() {
    Preconditions.checkArgument(
        sourcePrefix != null && !sourcePrefix.isEmpty(),
        "Source prefix('%s') cannot be empty.",
        sourcePrefix);

    validateAndSetEndVersion();

    endStaticTable = newStaticTable(TableMetadataParser.read(table.io(), endVersion), table.io());

    TableMetadata tableMetadata = ((HasTableOperations) endStaticTable).operations().current();
    Preconditions.checkArgument(
        tableMetadata.formatVersion() == 2, "Support Iceberg format version 2 only.");

    validateAndSetStartVersion(tableMetadata);

    if (fileExist(startVersion)) {
      startStaticTable =
          newStaticTable(TableMetadataParser.read(table.io(), startVersion), table.io());
    }

    if (stagingDir.isEmpty()) {
      String stagingDirName = "copy-table-staging-" + UUID.randomUUID();
      stagingDir = combinePaths(table.location(), stagingDirName);
    }

    if (!stagingDir.endsWith("/")) {
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

      Preconditions.checkArgument(
          fileExist(endVersion),
          "Cannot find the end version('%s') in the current version " + "files",
          endVersion);
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
          throw new IllegalArgumentException(
              "Cannot find the current version of target table in the source table. "
                  + "Please make sure the target table is a subset of source table.");
        }
      }
    } else {
      for (MetadataLogEntry metadataLogEntry : tableMetadata.previousFiles()) {
        if (versionInFilePath(metadataLogEntry.file(), startVersion)) {
          startVersion = metadataLogEntry.file();
          break;
        }
      }

      Preconditions.checkArgument(
          fileExist(startVersion), "Start version('%s') is NOT valid.", startVersion);

      if (targetTable != null
          && !fileName(startVersion).equals(fileName(currentMetadataPath(targetTable)))) {
        throw new IllegalArgumentException(
            "The start version isn't the current version of the target table. "
                + "Please make sure the target table is a subset of source table.");
      }
    }
  }

  private boolean versionInFilePath(String path, String version) {
    return fileName(path).equals(version);
  }

  private String jobDesc() {
    if (startVersion.isEmpty()) {
      return String.format(
          "Replacing path prefixes '%s' with '%s' in the metadata files of table %s,"
              + "up to version '%s'.",
          sourcePrefix, targetPrefix, table.name(), endVersion);
    } else {
      return String.format(
          "Replacing path prefixes '%s' with '%s' in the metadata files of table %s,"
              + "from version '%s' to '%s'.",
          sourcePrefix, targetPrefix, table.name(), startVersion, endVersion);
    }
  }

  /**
   * Here are steps: 1. rebuild version files 2. rebuild manifest list files 3. rebuild manifest
   * files 4. get all data files need to move
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
    Set<Snapshot> validSnapshots =
        Sets.difference(snapshotSet(endVersion), snapshotSet(startVersion));
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
    metadataFileList
        .repartition(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("text")
        .save(metadataFileListPath);
  }

  private void saveDataFileList(Dataset<Row> dataFiles) {
    dataFileListPath = stagingDir + DATA_FILE_LIST_DIR;

    try {
      dataFiles
          .repartition(1)
          .write()
          .mode(SaveMode.Overwrite)
          .format("text")
          .save(dataFileListPath);
    } catch (Exception e) {
      throw new UnsupportedOperationException(
          "Failed to build the data files dataframe, the end version you are "
              + "trying to copy may contain invalid snapshots, please use the younger version which doesn't have invalid "
              + "snapshots",
          e);
    }
  }

  private Set<Long> getDiffSnapshotIds(Set<Long> allSnapshotIds) {
    Set<Long> snapshotIdsInStartVersion = Sets.newHashSet();
    if (startStaticTable != null) {
      startStaticTable
          .snapshots()
          .forEach(snapshot -> snapshotIdsInStartVersion.add(snapshot.snapshotId()));
    }
    return Sets.difference(allSnapshotIds, snapshotIdsInStartVersion);
  }

  private Set<Long> rewriteVersionFiles(TableMetadata metadata) {
    Set<Long> allSnapshotIds = Sets.newHashSet();

    String stagingPath = combinePaths(stagingDir, relativize(endVersion, sourcePrefix));
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
      String newPath = combinePaths(stagingDir, relativize(versionFilePath, sourcePrefix));
      TableMetadata tableMetadata =
          new StaticTableOperations(versionFilePath, table.io()).current();

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
    TableMetadata newTableMetadata =
        TableMetadataUtil.replacePaths(metadata, sourcePrefix, targetPrefix);
    TableMetadataParser.overwrite(newTableMetadata, table.io().newOutputFile(stagingPath));
    addToRebuiltFiles(stagingPath);
  }

  private void rewriteManifestList(Snapshot snapshot, TableMetadata tableMetadata) {
    List<ManifestFile> manifestFiles = manifestFilesInSnapshot(snapshot);
    String path = snapshot.manifestListLocation();
    String stagingPath = combinePaths(stagingDir, relativize(path, sourcePrefix));
    OutputFile outputFile = table.io().newOutputFile(stagingPath);
    try (FileAppender<ManifestFile> writer =
        ManifestLists.write(
            tableMetadata.formatVersion(),
            outputFile,
            snapshot.snapshotId(),
            snapshot.parentId(),
            snapshot.sequenceNumber())) {

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
        return lastVersionFiles.distinct().as(Encoders.STRING()).collectAsList();
      } else {
        return lastVersionFiles
            .distinct()
            .filter(functions.column("added_snapshot_id").isInCollection(diffSnapshotIds))
            .as(Encoders.STRING())
            .collectAsList();
      }
    } catch (Exception e) {
      throw new UnsupportedOperationException(
          "Failed to build the manifest files dataframe, the end version you are "
              + "trying to copy may contain invalid snapshots, please use the younger version which doesn't have invalid "
              + "snapshots",
          e);
    }
  }

  /** Rewrite manifest files in a distributed manner. */
  private List<ManifestFile> rewriteManifests(TableMetadata tableMetadata) {
    if (manifestFilesToRewrite.isEmpty()) {
      return Lists.newArrayList();
    }

    Encoder<ManifestFile> manifestFileEncoder = Encoders.javaSerialization(ManifestFile.class);
    Dataset<ManifestFile> manifestDS =
        spark().createDataset(Lists.newArrayList(manifestFilesToRewrite), manifestFileEncoder);

    FileIO tableIO = SerializableTable.copyOf(table).io();
    Broadcast<FileIO> io = sparkContext().broadcast(tableIO);

    return manifestDS
        .repartition(manifestFilesToRewrite.size())
        .mapPartitions(
            toManifests(
                io,
                stagingDir,
                tableMetadata.formatVersion(),
                tableMetadata.specsById(),
                sourcePrefix,
                targetPrefix),
            manifestFileEncoder)
        .collectAsList();
  }

  private static MapPartitionsFunction<ManifestFile, ManifestFile> toManifests(
      Broadcast<FileIO> io,
      String stagingLocation,
      int format,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix) {

    return rows -> {
      List<ManifestFile> manifests = Lists.newArrayList();
      while (rows.hasNext()) {
        ManifestFile manifestFile = rows.next();
        switch (manifestFile.content()) {
          case DATA:
            manifests.add(
                writeDataManifest(
                    manifestFile,
                    io,
                    stagingLocation,
                    format,
                    specsById,
                    sourcePrefix,
                    targetPrefix));
            break;
          case DELETES:
            manifests.add(
                writeDeleteManifest(
                    manifestFile,
                    io,
                    stagingLocation,
                    format,
                    specsById,
                    sourcePrefix,
                    targetPrefix));
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported manifest type: " + manifestFile.content());
        }
      }

      return manifests.iterator();
    };
  }

  private static ManifestFile writeDataManifest(
      ManifestFile manifestFile,
      Broadcast<FileIO> io,
      String stagingLocation,
      int format,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix)
      throws IOException {
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());
    String stagingPath =
        combinePaths(stagingLocation, relativize(manifestFile.path(), sourcePrefix));
    OutputFile outputFile = io.value().newOutputFile(stagingPath);
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(format, spec, outputFile, manifestFile.snapshotId());

    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifestFile, io.getValue(), specsById).select(Arrays.asList("*"))) {
      reader
          .entries()
          .forEach(
              entry ->
                  appendEntryWithFile(
                      entry, writer, newDataFile(entry.file(), spec, sourcePrefix, targetPrefix)));
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }

  private static DataFile newDataFile(
      DataFile file, PartitionSpec spec, String sourcePrefix, String targetPrefix) {
    DataFile transformedFile = file;
    String filePath = file.path().toString();
    if (filePath.startsWith(sourcePrefix)) {
      filePath = newPath(filePath, sourcePrefix, targetPrefix);
      transformedFile = DataFiles.builder(spec).copy(file).withPath(filePath).build();
    }
    return transformedFile;
  }

  private static ManifestFile writeDeleteManifest(
      ManifestFile manifestFile,
      Broadcast<FileIO> io,
      String stagingLocation,
      int format,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix)
      throws IOException {
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());

    String manifestStagingPath =
        combinePaths(stagingLocation, relativize(manifestFile.path(), sourcePrefix));
    OutputFile manifestOutputFile = io.value().newOutputFile(manifestStagingPath);
    ManifestWriter<DeleteFile> writer =
        ManifestFiles.writeDeleteManifest(
            format, spec, manifestOutputFile, manifestFile.snapshotId());

    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifestFile, io.getValue(), specsById)
            .select(Arrays.asList("*"))) {

      for (ManifestEntry<DeleteFile> entry : reader.entries()) {
        DeleteFile file = entry.file();

        switch (file.content()) {
          case POSITION_DELETES:
            String filePath = file.path().toString();
            String deleteFileStagingPath =
                combinePaths(stagingLocation, relativize(filePath, sourcePrefix));
            DeleteFile newDeleteFile =
                rewritePositionDeleteFile(
                    io, file, deleteFileStagingPath, spec, sourcePrefix, targetPrefix);

            if (filePath.startsWith(sourcePrefix)) {
              filePath = newPath(filePath, sourcePrefix, targetPrefix);
              newDeleteFile =
                  FileMetadata.deleteFileBuilder(spec)
                      .ofPositionDeletes()
                      .copy(newDeleteFile)
                      .withPath(filePath)
                      .withSplitOffsets(file.splitOffsets())
                      .build();
            }
            appendEntryWithFile(entry, writer, newDeleteFile);
            break;
          case EQUALITY_DELETES:
            appendEntryWithFile(
                entry, writer, newEqualityDeleteFile(file, spec, sourcePrefix, targetPrefix));
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported delete file type: " + file.content());
        }
      }
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }

  private static DeleteFile newEqualityDeleteFile(
      DeleteFile file, PartitionSpec spec, String sourcePrefix, String targetPrefix) {
    DeleteFile transformedFile = file;
    String filePath = file.path().toString();

    if (filePath.startsWith(sourcePrefix)) {
      int[] equalityFieldIds =
          file.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray();
      filePath = newPath(filePath, sourcePrefix, targetPrefix);
      transformedFile =
          FileMetadata.deleteFileBuilder(spec)
              .ofEqualityDeletes(equalityFieldIds)
              .copy(file)
              .withPath(filePath)
              .withSplitOffsets(file.splitOffsets())
              .build();
    }
    return transformedFile;
  }

  private static PositionDelete newPositionDeleteRecord(
      Record record, String sourcePrefix, String targetPrefix) {
    PositionDelete delete = PositionDelete.create();
    delete.set(
        newPath((String) record.get(0), sourcePrefix, targetPrefix),
        (Long) record.get(1),
        record.get(2));
    return delete;
  }

  private static DeleteFile rewritePositionDeleteFile(
      Broadcast<FileIO> io,
      DeleteFile current,
      String path,
      PartitionSpec spec,
      String sourcePrefix,
      String targetPrefix)
      throws IOException {
    OutputFile targetFile = io.value().newOutputFile(path);
    InputFile sourceFile = io.value().newInputFile(current.path().toString());

    try (CloseableIterable<Record> reader =
        positionDeletesReader(sourceFile, current.format(), spec)) {
      Record record = null;
      Schema rowSchema = null;
      CloseableIterator<Record> recordIt = reader.iterator();

      if (recordIt.hasNext()) {
        record = recordIt.next();
        rowSchema = record.get(2) != null ? spec.schema() : null;
      }

      PositionDeleteWriter<Record> writer =
          positionDeletesWriter(targetFile, current.format(), spec, current.partition(), rowSchema);

      try {
        if (record != null) {
          writer.write(newPositionDeleteRecord(record, sourcePrefix, targetPrefix));
        }

        while (recordIt.hasNext()) {
          record = recordIt.next();
          writer.write(newPositionDeleteRecord(record, sourcePrefix, targetPrefix));
        }
      } finally {
        writer.close();
      }
      return writer.toDeleteFile();
    }
  }

  private static CloseableIterable<Record> positionDeletesReader(
      InputFile inputFile, FileFormat format, PartitionSpec spec) throws IOException {
    Schema deleteSchema = DeleteSchemaUtil.posDeleteSchema(spec.schema());
    switch (format) {
      case AVRO:
        return Avro.read(inputFile)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(DataReader::create)
            .build();

      case PARQUET:
        return Parquet.read(inputFile)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(deleteSchema, fileSchema))
            .build();

      case ORC:
        return ORC.read(inputFile)
            .project(deleteSchema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(deleteSchema, fileSchema))
            .build();

      default:
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }
  }

  private static PositionDeleteWriter<Record> positionDeletesWriter(
      OutputFile outputFile,
      FileFormat format,
      PartitionSpec spec,
      StructLike partition,
      Schema rowSchema)
      throws IOException {
    switch (format) {
      case AVRO:
        return Avro.writeDeletes(outputFile)
            .createWriterFunc(DataWriter::create)
            .withPartition(partition)
            .rowSchema(rowSchema)
            .withSpec(spec)
            .buildPositionWriter();
      case PARQUET:
        return Parquet.writeDeletes(outputFile)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .withPartition(partition)
            .rowSchema(rowSchema)
            .withSpec(spec)
            .buildPositionWriter();
      case ORC:
        return ORC.writeDeletes(outputFile)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .withPartition(partition)
            .rowSchema(rowSchema)
            .withSpec(spec)
            .buildPositionWriter();
      default:
        throw new UnsupportedOperationException("Unsupported file format: " + format);
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

  private Dataset<Row> getDiffDataFiles(Set<Long> diffSnapshotIds) {
    Dataset<Row> lastVersionFiles = buildValidDataFileDFWithSnapshotId(endStaticTable);
    if (startStaticTable == null) {
      return lastVersionFiles.distinct().select("file_path");
    } else {
      return lastVersionFiles
          .distinct()
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

  private static String relativize(String path, String prefix) {
    String toRemove = prefix;
    if (!toRemove.endsWith("/")) {
      toRemove += "/";
    }
    if (!path.startsWith(toRemove)) {
      throw new IllegalArgumentException(
          String.format("Path %s does not start with %s", path, toRemove));
    }
    return path.substring(toRemove.length());
  }

  private static String newPath(String path, String sourcePrefix, String targetPrefix) {
    return combinePaths(targetPrefix, relativize(path, sourcePrefix));
  }

  private void addToRebuiltFiles(String path) {
    metadataFilesToMove.add(path);
  }

  private String currentMetadataPath(Table tbl) {
    return ((HasTableOperations) tbl).operations().current().metadataFileLocation();
  }

  private static String combinePaths(String absolutePath, String relativePath) {
    String combined = absolutePath;
    if (!combined.endsWith("/")) {
      combined += "/";
    }
    combined += relativePath;
    return combined;
  }

  private static String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(File.separator);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }
}
