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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestLists;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteTablePathUtil;
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
import org.apache.iceberg.actions.ImmutableRewriteTablePath;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
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
import org.apache.iceberg.util.Pair;
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
import scala.Tuple2;

public class RewriteTablePathSparkAction extends BaseSparkAction<RewriteTablePath>
    implements RewriteTablePath {

  private static final Logger LOG = LoggerFactory.getLogger(RewriteTablePathSparkAction.class);
  private static final String RESULT_LOCATION = "file-list";

  private String sourcePrefix;
  private String targetPrefix;
  private String startVersionName;
  private String endVersionName;
  private String stagingDir;

  private final Table table;

  RewriteTablePathSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected RewriteTablePath self() {
    return this;
  }

  @Override
  public RewriteTablePath rewriteLocationPrefix(String sPrefix, String tPrefix) {
    Preconditions.checkArgument(
        sPrefix != null && !sPrefix.isEmpty(), "Source prefix('%s') cannot be empty.", sPrefix);
    this.sourcePrefix = sPrefix;
    this.targetPrefix = tPrefix;
    return this;
  }

  @Override
  public RewriteTablePath startVersion(String sVersion) {
    Preconditions.checkArgument(
        sVersion != null && !sVersion.trim().isEmpty(),
        "Start version('%s') cannot be empty.",
        sVersion);
    this.startVersionName = sVersion;
    return this;
  }

  @Override
  public RewriteTablePath endVersion(String eVersion) {
    Preconditions.checkArgument(
        eVersion != null && !eVersion.trim().isEmpty(),
        "End version('%s') cannot be empty.",
        eVersion);
    this.endVersionName = eVersion;
    return this;
  }

  @Override
  public RewriteTablePath stagingLocation(String stagingLocation) {
    Preconditions.checkArgument(
        stagingLocation != null && !stagingLocation.isEmpty(),
        "Staging location('%s') cannot be empty.",
        stagingLocation);
    this.stagingDir = stagingLocation;
    return this;
  }

  @Override
  public Result execute() {
    validateInputs();
    JobGroupInfo info = newJobGroupInfo("COPY-TABLE", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    String resultLocation = rebuildMetadata();
    return ImmutableRewriteTablePath.Result.builder()
        .stagingLocation(stagingDir)
        .fileListLocation(resultLocation)
        .latestVersion(fileName(endVersionName))
        .build();
  }

  private void validateInputs() {
    Preconditions.checkArgument(
        sourcePrefix != null && !sourcePrefix.isEmpty(),
        "Source prefix('%s') cannot be empty.",
        sourcePrefix);
    Preconditions.checkArgument(
        targetPrefix != null && !targetPrefix.isEmpty(),
        "Target prefix('%s') cannot be empty.",
        targetPrefix);
    Preconditions.checkArgument(
        !sourcePrefix.equals(targetPrefix),
        "Source prefix cannot be the same as target prefix (%s)",
        sourcePrefix);

    validateAndSetEndVersion();
    validateAndSetStartVersion();

    if (stagingDir == null) {
      stagingDir = getMetadataLocation(table) + "copy-table-staging-" + UUID.randomUUID() + "/";
    } else if (!stagingDir.endsWith("/")) {
      stagingDir = stagingDir + "/";
    }
  }

  private void validateAndSetEndVersion() {
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();

    if (endVersionName == null) {
      LOG.info("No end version specified. Will stage all files to the latest table version.");
      Preconditions.checkNotNull(
          tableMetadata.metadataFileLocation(), "Metadata file location should not be null");
      this.endVersionName = tableMetadata.metadataFileLocation();
    } else {
      this.endVersionName = validateVersion(tableMetadata, endVersionName);
    }
  }

  private void validateAndSetStartVersion() {
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();

    if (startVersionName != null) {
      this.startVersionName = validateVersion(tableMetadata, startVersionName);
    }
  }

  private String validateVersion(TableMetadata tableMetadata, String versionFileName) {
    String versionFile = versionFile(tableMetadata, versionFileName);

    Preconditions.checkNotNull(
        versionFile, "Version file %s does not exist in metadata log.", versionFile);
    Preconditions.checkArgument(
        fileExist(versionFile), "Version file %s does not exist.", versionFile);
    return versionFile;
  }

  private String versionFile(TableMetadata metadata, String versionFileName) {
    if (versionInFilePath(metadata.metadataFileLocation(), versionFileName)) {
      return metadata.metadataFileLocation();
    }

    for (MetadataLogEntry log : metadata.previousFiles()) {
      if (versionInFilePath(log.file(), versionFileName)) {
        return log.file();
      }
    }
    return null;
  }

  private boolean versionInFilePath(String path, String version) {
    return fileName(path).equals(version);
  }

  private String jobDesc() {
    if (startVersionName != null) {
      return String.format(
          "Replacing path prefixes '%s' with '%s' in the metadata files of table %s,"
              + "up to version '%s'.",
          sourcePrefix, targetPrefix, table.name(), endVersionName);
    } else {
      return String.format(
          "Replacing path prefixes '%s' with '%s' in the metadata files of table %s,"
              + "from version '%s' to '%s'.",
          sourcePrefix, targetPrefix, table.name(), startVersionName, endVersionName);
    }
  }

  /**
   *
   *
   * <ul>
   *   <li>Rebuild version files to staging
   *   <li>Rebuild manifest list files to staging
   *   <li>Rebuild manifest to staging
   *   <li>Get all files needed to move
   * </ul>
   */
  private String rebuildMetadata() {
    TableMetadata startMetadata =
        startVersionName != null
            ? ((HasTableOperations) newStaticTable(startVersionName, table.io()))
                .operations()
                .current()
            : null;
    TableMetadata endMetadata =
        ((HasTableOperations) newStaticTable(endVersionName, table.io())).operations().current();

    Preconditions.checkArgument(
        endMetadata.statisticsFiles() == null || endMetadata.statisticsFiles().isEmpty(),
        "Statistic files are not supported yet.");

    // rebuild version files
    RewriteResult<Snapshot> rewriteVersionResult = rewriteVersionFiles(endMetadata);
    Set<Snapshot> diffSnapshots =
        getDiffSnapshotIds(startMetadata, rewriteVersionResult.toRewrite());

    Set<String> manifestsToRewrite = manifestsToRewrite(diffSnapshots, startMetadata);
    Set<Snapshot> validSnapshots =
        Sets.difference(snapshotSet(endMetadata), snapshotSet(startMetadata));

    // rebuild manifest-list files
    RewriteResult<ManifestFile> rewriteManifestListResult =
        validSnapshots.stream()
            .map(snapshot -> rewriteManifestList(snapshot, endMetadata, manifestsToRewrite))
            .reduce(new RewriteResult<>(), RewriteResult::new);

    // rebuild manifest files
    Set<Pair<String, String>> contentFilesToMove =
        rewriteManifests(endMetadata, rewriteManifestListResult.toRewrite());

    Set<Pair<String, String>> movePlan = Sets.newHashSet();
    movePlan.addAll(rewriteVersionResult.copyPlan());
    movePlan.addAll(rewriteManifestListResult.copyPlan());
    movePlan.addAll(contentFilesToMove);

    return saveFileList(movePlan);
  }

  private String saveFileList(Set<Pair<String, String>> filesToMove) {
    List<Tuple2<String, String>> fileList =
        filesToMove.stream()
            .map(p -> Tuple2.apply(p.first(), p.second()))
            .collect(Collectors.toList());
    Dataset<Tuple2<String, String>> fileListDataset =
        spark().createDataset(fileList, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
    String fileListPath = stagingDir + RESULT_LOCATION;
    fileListDataset
        .repartition(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("csv")
        .save(fileListPath);
    return fileListPath;
  }

  private Set<Snapshot> getDiffSnapshotIds(
      TableMetadata startMetadata, Set<Snapshot> allSnapshots) {
    if (startMetadata == null) {
      return allSnapshots;
    } else {
      Set<Long> startSnapshotIds =
          startMetadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      return allSnapshots.stream()
          .filter(s -> !startSnapshotIds.contains(s.snapshotId()))
          .collect(Collectors.toSet());
    }
  }

  private RewriteResult<Snapshot> rewriteVersionFiles(TableMetadata endMetadata) {
    RewriteResult<Snapshot> result = new RewriteResult<>();
    result.toRewrite().addAll(endMetadata.snapshots());
    result.copyPlan().add(rewriteVersionFile(endMetadata, endVersionName));

    List<MetadataLogEntry> versions = endMetadata.previousFiles();
    for (int i = versions.size() - 1; i >= 0; i--) {
      String versionFilePath = versions.get(i).file();
      if (versionFilePath.equals(startVersionName)) {
        break;
      }

      Preconditions.checkArgument(
          fileExist(versionFilePath),
          String.format("Version file %s doesn't exist", versionFilePath));
      TableMetadata tableMetadata =
          new StaticTableOperations(versionFilePath, table.io()).current();

      result.toRewrite().addAll(tableMetadata.snapshots());
      result.copyPlan().add(rewriteVersionFile(tableMetadata, versionFilePath));
    }

    return result;
  }

  private Pair<String, String> rewriteVersionFile(TableMetadata metadata, String versionFilePath) {
    String stagingPath = stagingPath(versionFilePath, stagingDir);
    TableMetadata newTableMetadata =
        TableMetadataUtil.replacePaths(metadata, sourcePrefix, targetPrefix);
    TableMetadataParser.overwrite(newTableMetadata, table.io().newOutputFile(stagingPath));
    return Pair.of(stagingPath, newPath(versionFilePath, sourcePrefix, targetPrefix));
  }

  /**
   * Rewrite a manifest list representing a snapshot.
   *
   * @param snapshot snapshot represented by the manifest list
   * @param tableMetadata metadata of table
   */
  private RewriteResult<ManifestFile> rewriteManifestList(
      Snapshot snapshot, TableMetadata tableMetadata, Set<String> manifestsToRewrite) {
    RewriteResult<ManifestFile> result = new RewriteResult<>();
    List<ManifestFile> manifestFiles = manifestFilesInSnapshot(snapshot);
    String path = snapshot.manifestListLocation();
    String stagingPath = stagingPath(path, stagingDir);
    OutputFile outputFile = table.io().newOutputFile(stagingPath);
    try (FileAppender<ManifestFile> writer =
        ManifestLists.write(
            tableMetadata.formatVersion(),
            outputFile,
            snapshot.snapshotId(),
            snapshot.parentId(),
            snapshot.sequenceNumber())) {

      for (ManifestFile file : manifestFiles) {
        Preconditions.checkArgument(
            file.path().startsWith(sourcePrefix),
            "Encountered manifest file %s not under the source prefix %s",
            file.path(),
            sourcePrefix);

        ManifestFile newFile = file.copy();
        ((StructLike) newFile).set(0, newPath(newFile.path(), sourcePrefix, targetPrefix));
        writer.add(newFile);

        // return the ManifestFile object for subsequent rewriting
        if (manifestsToRewrite.contains(file.path())) {
          result.toRewrite().add(file);
          result.copyPlan().add(Pair.of(stagingPath(file.path(), stagingDir), newFile.path()));
        }
      }

      result.copyPlan().add(Pair.of(stagingPath, newPath(path, sourcePrefix, targetPrefix)));
      return result;
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

  private Set<String> manifestsToRewrite(Set<Snapshot> diffSnapshots, TableMetadata startMetadata) {
    try {
      Table endStaticTable = newStaticTable(endVersionName, table.io());
      Dataset<Row> lastVersionFiles = manifestDS(endStaticTable).select("path");
      if (startMetadata == null) {
        return Sets.newHashSet(lastVersionFiles.distinct().as(Encoders.STRING()).collectAsList());
      } else {
        Set<Long> diffSnapshotIds =
            diffSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
        return Sets.newHashSet(
            lastVersionFiles
                .distinct()
                .filter(functions.column("added_snapshot_id").isInCollection(diffSnapshotIds))
                .as(Encoders.STRING())
                .collectAsList());
      }
    } catch (Exception e) {
      throw new UnsupportedOperationException(
          "Failed to build the manifest files dataframe, the end version you are "
              + "trying to copy may contain invalid snapshots, please a younger version that doesn't have invalid "
              + "snapshots",
          e);
    }
  }

  /** Rewrite manifest files in a distributed manner and return rewritten data files path pairs. */
  private Set<Pair<String, String>> rewriteManifests(
      TableMetadata tableMetadata, Set<ManifestFile> toRewrite) {
    if (toRewrite.isEmpty()) {
      return Sets.newHashSet();
    }

    Encoder<ManifestFile> manifestFileEncoder = Encoders.javaSerialization(ManifestFile.class);
    Dataset<ManifestFile> manifestDS =
        spark().createDataset(Lists.newArrayList(toRewrite), manifestFileEncoder);

    Broadcast<Table> serializableTable = sparkContext().broadcast(SerializableTable.copyOf(table));
    Broadcast<Map<Integer, PartitionSpec>> specsById =
        sparkContext().broadcast(tableMetadata.specsById());

    List<Tuple2<String, String>> dataFiles =
        manifestDS
            .repartition(toRewrite.size())
            .mapPartitions(
                toManifests(
                    serializableTable,
                    stagingDir,
                    tableMetadata.formatVersion(),
                    specsById,
                    sourcePrefix,
                    targetPrefix),
                Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
            .collectAsList();

    // duplicates are expected here as the same data file can have different statuses
    // (e.g. added and deleted)
    return dataFiles.stream().map(t -> Pair.of(t._1(), t._2())).collect(Collectors.toSet());
  }

  private static MapPartitionsFunction<ManifestFile, Tuple2<String, String>> toManifests(
      Broadcast<Table> tableBroadcast,
      String stagingLocation,
      int format,
      Broadcast<Map<Integer, PartitionSpec>> specsById,
      String sourcePrefix,
      String targetPrefix) {

    return rows -> {
      List<Tuple2<String, String>> files = Lists.newArrayList();
      while (rows.hasNext()) {
        ManifestFile manifestFile = rows.next();
        switch (manifestFile.content()) {
          case DATA:
            files.addAll(
                writeDataManifest(
                    manifestFile,
                    tableBroadcast,
                    stagingLocation,
                    format,
                    specsById,
                    sourcePrefix,
                    targetPrefix));
            break;
          case DELETES:
            files.addAll(
                writeDeleteManifest(
                    manifestFile,
                    tableBroadcast,
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
      return files.iterator();
    };
  }

  private static List<Tuple2<String, String>> writeDataManifest(
      ManifestFile manifestFile,
      Broadcast<Table> tableBroadcast,
      String stagingLocation,
      int format,
      Broadcast<Map<Integer, PartitionSpec>> specsByIdBroadcast,
      String sourcePrefix,
      String targetPrefix)
      throws IOException {
    String stagingPath = stagingPath(manifestFile.path(), stagingLocation);
    FileIO io = tableBroadcast.getValue().io();
    OutputFile outputFile = io.newOutputFile(stagingPath);
    Map<Integer, PartitionSpec> specsById = specsByIdBroadcast.getValue();
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());

    return RewriteTablePathUtil.rewriteManifest(
            io, format, spec, outputFile, manifestFile, specsById, sourcePrefix, targetPrefix)
        .stream()
        .map(p -> Tuple2.apply(p.first(), p.second()))
        .collect(Collectors.toList());
  }

  private static List<Tuple2<String, String>> writeDeleteManifest(
      ManifestFile manifestFile,
      Broadcast<Table> tableBroadcast,
      String stagingLocation,
      int format,
      Broadcast<Map<Integer, PartitionSpec>> specsByIdBroadcast,
      String sourcePrefix,
      String targetPrefix)
      throws IOException {
    String stagingPath = stagingPath(manifestFile.path(), stagingLocation);
    FileIO io = tableBroadcast.getValue().io();
    OutputFile outputFile = io.newOutputFile(stagingPath);
    Map<Integer, PartitionSpec> specsById = specsByIdBroadcast.getValue();
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());
    RewriteTablePathUtil.PositionDeleteReaderWriter posDeleteReaderWriter =
        new RewriteTablePathUtil.PositionDeleteReaderWriter() {
          @Override
          public CloseableIterable<Record> reader(
              InputFile inputFile, FileFormat format, PartitionSpec spec) {
            return positionDeletesReader(inputFile, format, spec);
          }

          @Override
          public PositionDeleteWriter<Record> writer(
              OutputFile outputFile,
              FileFormat format,
              PartitionSpec spec,
              StructLike partition,
              Schema rowSchema)
              throws IOException {
            return positionDeletesWriter(outputFile, format, spec, partition, rowSchema);
          }
        };
    return RewriteTablePathUtil.rewriteDeleteManifest(
            io,
            format,
            spec,
            outputFile,
            manifestFile,
            specsById,
            sourcePrefix,
            targetPrefix,
            stagingLocation,
            posDeleteReaderWriter)
        .stream()
        .map(p -> Tuple2.apply(p.first(), p.second()))
        .collect(Collectors.toList());
  }

  private static CloseableIterable<Record> positionDeletesReader(
      InputFile inputFile, FileFormat format, PartitionSpec spec) {
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

  private Set<Snapshot> snapshotSet(TableMetadata metadata) {
    if (metadata == null) {
      return Sets.newHashSet();
    } else {
      return Sets.newHashSet(metadata.snapshots());
    }
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

  private static String stagingPath(String originalPath, String stagingLocation) {
    return stagingLocation + fileName(originalPath);
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

  private String getMetadataLocation(Table tbl) {
    String currentMetadataPath =
        ((HasTableOperations) tbl).operations().current().metadataFileLocation();
    int lastIndex = currentMetadataPath.lastIndexOf(File.separator);
    String metadataDir = "";
    if (lastIndex != -1) {
      metadataDir = currentMetadataPath.substring(0, lastIndex + 1);
    }

    Preconditions.checkArgument(
        !metadataDir.isEmpty(), "Failed to get the metadata file root directory");
    return metadataDir;
  }

  static class RewriteResult<T> {
    private final Set<T> toRewrite = Sets.newHashSet();
    private final Set<Pair<String, String>> copyPlan = Sets.newHashSet();

    RewriteResult() {}

    RewriteResult(RewriteResult<T> r1, RewriteResult<T> r2) {
      toRewrite.addAll(r1.toRewrite);
      toRewrite.addAll(r2.toRewrite);
      copyPlan.addAll(r1.copyPlan);
      copyPlan.addAll(r2.copyPlan);
    }

    private Set<T> toRewrite() {
      return toRewrite;
    }

    private Set<Pair<String, String>> copyPlan() {
      return copyPlan;
    }
  }
}
