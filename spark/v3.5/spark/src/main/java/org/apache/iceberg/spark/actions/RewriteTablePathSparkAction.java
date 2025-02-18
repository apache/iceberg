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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.RewriteTablePathUtil.PositionDeleteReaderWriter;
import org.apache.iceberg.RewriteTablePathUtil.RewriteResult;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadataParser;
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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.util.Pair;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
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
  private Broadcast<Table> tableBroadcast = null;

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
    JobGroupInfo info = newJobGroupInfo("REWRITE-TABLE-PATH", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    String resultLocation = rebuildMetadata();
    return ImmutableRewriteTablePath.Result.builder()
        .stagingLocation(stagingDir)
        .fileListLocation(resultLocation)
        .latestVersion(RewriteTablePathUtil.fileName(endVersionName))
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
      stagingDir =
          getMetadataLocation(table)
              + "copy-table-staging-"
              + UUID.randomUUID()
              + RewriteTablePathUtil.FILE_SEPARATOR;
    } else {
      stagingDir = RewriteTablePathUtil.maybeAppendFileSeparator(stagingDir);
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
    String versionFile = null;
    if (versionInFilePath(tableMetadata.metadataFileLocation(), versionFileName)) {
      versionFile = tableMetadata.metadataFileLocation();
    }

    for (MetadataLogEntry log : tableMetadata.previousFiles()) {
      if (versionInFilePath(log.file(), versionFileName)) {
        versionFile = log.file();
      }
    }

    Preconditions.checkArgument(
        versionFile != null,
        "Cannot find provided version file %s in metadata log.",
        versionFileName);
    Preconditions.checkArgument(
        fileExist(versionFile), "Version file %s does not exist.", versionFile);
    return versionFile;
  }

  private boolean versionInFilePath(String path, String version) {
    return RewriteTablePathUtil.fileName(path).equals(version);
  }

  private String jobDesc() {
    if (startVersionName == null) {
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
   * Rebuild metadata in a staging location, with paths rewritten.
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
        endMetadata.partitionStatisticsFiles() == null
            || endMetadata.partitionStatisticsFiles().isEmpty(),
        "Partition statistics files are not supported yet.");

    // rebuild version files
    RewriteResult<Snapshot> rewriteVersionResult = rewriteVersionFiles(endMetadata);
    Set<Snapshot> deltaSnapshots = deltaSnapshots(startMetadata, rewriteVersionResult.toRewrite());

    Set<String> manifestsToRewrite = manifestsToRewrite(deltaSnapshots, startMetadata);
    Set<Snapshot> validSnapshots =
        Sets.difference(snapshotSet(endMetadata), snapshotSet(startMetadata));

    // rebuild manifest-list files
    RewriteResult<ManifestFile> rewriteManifestListResult =
        validSnapshots.stream()
            .map(snapshot -> rewriteManifestList(snapshot, endMetadata, manifestsToRewrite))
            .reduce(new RewriteResult<>(), RewriteResult::append);

    // rebuild manifest files
    RewriteContentFileResult rewriteManifestResult =
        rewriteManifests(endMetadata, rewriteManifestListResult.toRewrite());

    // rebuild position delete files
    Set<DeleteFile> deleteFiles =
        rewriteManifestResult.toRewrite().stream()
            .filter(e -> e instanceof DeleteFile)
            .map(e -> (DeleteFile) e)
            .collect(Collectors.toSet());
    rewritePositionDeletes(endMetadata, deleteFiles);

    Set<Pair<String, String>> copyPlan = Sets.newHashSet();
    copyPlan.addAll(rewriteVersionResult.copyPlan());
    copyPlan.addAll(rewriteManifestListResult.copyPlan());
    copyPlan.addAll(rewriteManifestResult.copyPlan());

    return saveFileList(copyPlan);
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

  private Set<Snapshot> deltaSnapshots(TableMetadata startMetadata, Set<Snapshot> allSnapshots) {
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
    result.copyPlan().addAll(rewriteVersionFile(endMetadata, endVersionName));

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
      result.copyPlan().addAll(rewriteVersionFile(tableMetadata, versionFilePath));
    }

    return result;
  }

  private Set<Pair<String, String>> rewriteVersionFile(
      TableMetadata metadata, String versionFilePath) {
    Set<Pair<String, String>> result = Sets.newHashSet();
    String stagingPath = RewriteTablePathUtil.stagingPath(versionFilePath, stagingDir);
    TableMetadata newTableMetadata =
        RewriteTablePathUtil.replacePaths(metadata, sourcePrefix, targetPrefix);
    TableMetadataParser.overwrite(newTableMetadata, table.io().newOutputFile(stagingPath));
    result.add(
        Pair.of(
            stagingPath,
            RewriteTablePathUtil.newPath(versionFilePath, sourcePrefix, targetPrefix)));

    // include statistics files in copy plan
    result.addAll(
        statsFileCopyPlan(metadata.statisticsFiles(), newTableMetadata.statisticsFiles()));
    return result;
  }

  private Set<Pair<String, String>> statsFileCopyPlan(
      List<StatisticsFile> beforeStats, List<StatisticsFile> afterStats) {
    Set<Pair<String, String>> result = Sets.newHashSet();
    if (beforeStats.isEmpty()) {
      return result;
    }

    Preconditions.checkArgument(
        beforeStats.size() == afterStats.size(),
        "Before and after path rewrite, statistic files count should be same");
    for (int i = 0; i < beforeStats.size(); i++) {
      StatisticsFile before = beforeStats.get(i);
      StatisticsFile after = afterStats.get(i);
      Preconditions.checkArgument(
          before.fileSizeInBytes() == after.fileSizeInBytes(),
          "Before and after path rewrite, statistic file size should be same");
      result.add(
          Pair.of(RewriteTablePathUtil.stagingPath(before.path(), stagingDir), after.path()));
    }
    return result;
  }

  /**
   * Rewrite a manifest list representing a snapshot.
   *
   * @param snapshot snapshot represented by the manifest list
   * @param tableMetadata metadata of table
   * @param manifestsToRewrite filter of manifests to rewrite.
   * @return a result including a copy plan for the manifests contained in the manifest list, as
   *     well as for the manifest list itself
   */
  private RewriteResult<ManifestFile> rewriteManifestList(
      Snapshot snapshot, TableMetadata tableMetadata, Set<String> manifestsToRewrite) {
    RewriteResult<ManifestFile> result = new RewriteResult<>();

    String path = snapshot.manifestListLocation();
    String outputPath = RewriteTablePathUtil.stagingPath(path, stagingDir);
    RewriteResult<ManifestFile> rewriteResult =
        RewriteTablePathUtil.rewriteManifestList(
            snapshot,
            table.io(),
            tableMetadata,
            manifestsToRewrite,
            sourcePrefix,
            targetPrefix,
            stagingDir,
            outputPath);

    result.append(rewriteResult);
    // add the manifest list copy plan itself to the result
    result
        .copyPlan()
        .add(Pair.of(outputPath, RewriteTablePathUtil.newPath(path, sourcePrefix, targetPrefix)));
    return result;
  }

  private Set<String> manifestsToRewrite(
      Set<Snapshot> deltaSnapshots, TableMetadata startMetadata) {
    try {
      Table endStaticTable = newStaticTable(endVersionName, table.io());
      Dataset<Row> lastVersionFiles = manifestDS(endStaticTable).select("path");
      if (startMetadata == null) {
        return Sets.newHashSet(lastVersionFiles.distinct().as(Encoders.STRING()).collectAsList());
      } else {
        Set<Long> deltaSnapshotIds =
            deltaSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
        return Sets.newHashSet(
            lastVersionFiles
                .distinct()
                .filter(
                    functions
                        .column(ManifestFile.SNAPSHOT_ID.name())
                        .isInCollection(deltaSnapshotIds))
                .as(Encoders.STRING())
                .collectAsList());
      }
    } catch (Exception e) {
      throw new UnsupportedOperationException(
          "Unable to build the manifest files dataframe. The end version in use may contain invalid snapshots. "
              + "Please choose an earlier version without invalid snapshots.",
          e);
    }
  }

  public static class RewriteContentFileResult extends RewriteResult<ContentFile<?>> {
    public RewriteContentFileResult append(RewriteResult<ContentFile<?>> r1) {
      this.copyPlan().addAll(r1.copyPlan());
      this.toRewrite().addAll(r1.toRewrite());
      return this;
    }

    public RewriteContentFileResult appendDataFile(RewriteResult<DataFile> r1) {
      this.copyPlan().addAll(r1.copyPlan());
      this.toRewrite().addAll(r1.toRewrite());
      return this;
    }

    public RewriteContentFileResult appendDeleteFile(RewriteResult<DeleteFile> r1) {
      this.copyPlan().addAll(r1.copyPlan());
      this.toRewrite().addAll(r1.toRewrite());
      return this;
    }
  }

  /** Rewrite manifest files in a distributed manner and return rewritten data files path pairs. */
  private RewriteContentFileResult rewriteManifests(
      TableMetadata tableMetadata, Set<ManifestFile> toRewrite) {
    if (toRewrite.isEmpty()) {
      return new RewriteContentFileResult();
    }

    Encoder<ManifestFile> manifestFileEncoder = Encoders.javaSerialization(ManifestFile.class);
    Dataset<ManifestFile> manifestDS =
        spark().createDataset(Lists.newArrayList(toRewrite), manifestFileEncoder);

    return manifestDS
        .repartition(toRewrite.size())
        .map(
            toManifests(
                tableBroadcast(),
                stagingDir,
                tableMetadata.formatVersion(),
                sourcePrefix,
                targetPrefix),
            Encoders.bean(RewriteContentFileResult.class))
        // duplicates are expected here as the same data file can have different statuses
        // (e.g. added and deleted)
        .reduce((ReduceFunction<RewriteContentFileResult>) RewriteContentFileResult::append);
  }

  private static MapFunction<ManifestFile, RewriteContentFileResult> toManifests(
      Broadcast<Table> table,
      String stagingLocation,
      int format,
      String sourcePrefix,
      String targetPrefix) {

    return manifestFile -> {
      RewriteContentFileResult result = new RewriteContentFileResult();
      switch (manifestFile.content()) {
        case DATA:
          result.appendDataFile(
              writeDataManifest(
                  manifestFile, table, stagingLocation, format, sourcePrefix, targetPrefix));
          break;
        case DELETES:
          result.appendDeleteFile(
              writeDeleteManifest(
                  manifestFile, table, stagingLocation, format, sourcePrefix, targetPrefix));
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported manifest type: " + manifestFile.content());
      }
      return result;
    };
  }

  private static RewriteResult<DataFile> writeDataManifest(
      ManifestFile manifestFile,
      Broadcast<Table> table,
      String stagingLocation,
      int format,
      String sourcePrefix,
      String targetPrefix) {
    try {
      String stagingPath = RewriteTablePathUtil.stagingPath(manifestFile.path(), stagingLocation);
      FileIO io = table.getValue().io();
      OutputFile outputFile = io.newOutputFile(stagingPath);
      Map<Integer, PartitionSpec> specsById = table.getValue().specs();
      return RewriteTablePathUtil.rewriteDataManifest(
          manifestFile, outputFile, io, format, specsById, sourcePrefix, targetPrefix);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static RewriteResult<DeleteFile> writeDeleteManifest(
      ManifestFile manifestFile,
      Broadcast<Table> table,
      String stagingLocation,
      int format,
      String sourcePrefix,
      String targetPrefix) {
    try {
      String stagingPath = RewriteTablePathUtil.stagingPath(manifestFile.path(), stagingLocation);
      FileIO io = table.getValue().io();
      OutputFile outputFile = io.newOutputFile(stagingPath);
      Map<Integer, PartitionSpec> specsById = table.getValue().specs();
      return RewriteTablePathUtil.rewriteDeleteManifest(
          manifestFile,
          outputFile,
          io,
          format,
          specsById,
          sourcePrefix,
          targetPrefix,
          stagingLocation);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private void rewritePositionDeletes(TableMetadata metadata, Set<DeleteFile> toRewrite) {
    if (toRewrite.isEmpty()) {
      return;
    }

    Encoder<DeleteFile> deleteFileEncoder = Encoders.javaSerialization(DeleteFile.class);
    Dataset<DeleteFile> deleteFileDs =
        spark().createDataset(Lists.newArrayList(toRewrite), deleteFileEncoder);

    PositionDeleteReaderWriter posDeleteReaderWriter = new SparkPositionDeleteReaderWriter();
    deleteFileDs
        .repartition(toRewrite.size())
        .foreach(
            rewritePositionDelete(
                tableBroadcast(), sourcePrefix, targetPrefix, stagingDir, posDeleteReaderWriter));
  }

  private static class SparkPositionDeleteReaderWriter implements PositionDeleteReaderWriter {
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
  }

  private ForeachFunction<DeleteFile> rewritePositionDelete(
      Broadcast<Table> tableArg,
      String sourcePrefixArg,
      String targetPrefixArg,
      String stagingLocationArg,
      PositionDeleteReaderWriter posDeleteReaderWriter) {
    return deleteFile -> {
      FileIO io = tableArg.getValue().io();
      String newPath = RewriteTablePathUtil.stagingPath(deleteFile.location(), stagingLocationArg);
      OutputFile outputFile = io.newOutputFile(newPath);
      PartitionSpec spec = tableArg.getValue().specs().get(deleteFile.specId());
      RewriteTablePathUtil.rewritePositionDeleteFile(
          deleteFile,
          outputFile,
          io,
          spec,
          sourcePrefixArg,
          targetPrefixArg,
          posDeleteReaderWriter);
    };
  }

  private static CloseableIterable<Record> positionDeletesReader(
      InputFile inputFile, FileFormat format, PartitionSpec spec) {
    Schema deleteSchema = DeleteSchemaUtil.posDeleteReadSchema(spec.schema());
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

  private String getMetadataLocation(Table tbl) {
    String currentMetadataPath =
        ((HasTableOperations) tbl).operations().current().metadataFileLocation();
    int lastIndex = currentMetadataPath.lastIndexOf(RewriteTablePathUtil.FILE_SEPARATOR);
    String metadataDir = "";
    if (lastIndex != -1) {
      metadataDir = currentMetadataPath.substring(0, lastIndex + 1);
    }

    Preconditions.checkArgument(
        !metadataDir.isEmpty(), "Failed to get the metadata file root directory");
    return metadataDir;
  }

  @VisibleForTesting
  Broadcast<Table> tableBroadcast() {
    if (tableBroadcast == null) {
      this.tableBroadcast = sparkContext().broadcast(SerializableTableWithSize.copyOf(table));
    }

    return tableBroadcast;
  }
}
