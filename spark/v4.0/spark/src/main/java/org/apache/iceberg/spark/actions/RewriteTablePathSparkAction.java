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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
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
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.formats.FormatModelRegistry;
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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class RewriteTablePathSparkAction extends BaseSparkAction<RewriteTablePath>
    implements RewriteTablePath {

  private static final Logger LOG = LoggerFactory.getLogger(RewriteTablePathSparkAction.class);
  private static final String RESULT_LOCATION = "file-list";
  static final String NOT_APPLICABLE = "N/A";

  private String sourcePrefix;
  private String targetPrefix;
  private String startVersionName;
  private String endVersionName;
  private String stagingDir;
  private boolean createFileList = true;
  private ExecutorService executorService;

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
  public RewriteTablePath createFileList(boolean createFileListFlag) {
    this.createFileList = createFileListFlag;
    return this;
  }

  @Override
  public RewriteTablePath executeWith(ExecutorService service) {
    this.executorService = service;
    return this;
  }

  @Override
  public Result execute() {
    validateInputs();
    JobGroupInfo info = newJobGroupInfo("REWRITE-TABLE-PATH", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    return rebuildMetadata();
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
   *   <li>Rewrite referenced position delete files to staging
   *   <li>Rebuild manifests to staging
   *   <li>Get all files needed to move
   * </ul>
   */
  private Result rebuildMetadata() {
    TableMetadata startMetadata =
        startVersionName != null
            ? ((HasTableOperations) newStaticTable(startVersionName, table.io()))
                .operations()
                .current()
            : null;
    TableMetadata endMetadata =
        ((HasTableOperations) newStaticTable(endVersionName, table.io())).operations().current();

    // rebuild version files
    RewriteResult<Snapshot> rewriteVersionResult = rewriteVersionFiles(endMetadata);
    Set<Snapshot> deltaSnapshots = deltaSnapshots(startMetadata, rewriteVersionResult.toRewrite());

    Set<String> manifestsToRewrite = manifestsToRewrite(deltaSnapshots, startMetadata);
    Set<Snapshot> validSnapshots =
        Sets.difference(snapshotSet(endMetadata), snapshotSet(startMetadata));

    // rebuild manifest-list files
    Set<RewriteResult<ManifestFile>> manifestListResults = Sets.newConcurrentHashSet();
    Tasks.foreach(validSnapshots)
        .noRetry()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(
            snapshot ->
                manifestListResults.add(
                    rewriteManifestList(snapshot, endMetadata, manifestsToRewrite)));

    RewriteResult<ManifestFile> rewriteManifestListResult = new RewriteResult<>();
    manifestListResults.forEach(rewriteManifestListResult::append);

    Set<ManifestFile> manifestFiles = rewriteManifestListResult.toRewrite();

    // rebuild position delete files
    Set<ManifestFile> deleteManifests =
        manifestFiles.stream()
            .filter(manifest -> manifest.content() == ManifestContent.DELETES)
            .collect(Collectors.toSet());
    Set<DeleteFile> deleteFilesToRewrite = positionDeletesToRewrite(deleteManifests);
    Map<String, Long> rewrittenDeleteFileSizes = rewritePositionDeletes(deleteFilesToRewrite);

    // rebuild manifest files
    RewriteContentFileResult rewriteManifestResult =
        rewriteManifests(
            deltaSnapshots,
            endMetadata,
            manifestFiles,
            sparkContext().broadcast(rewrittenDeleteFileSizes));

    ImmutableRewriteTablePath.Result.Builder builder =
        ImmutableRewriteTablePath.Result.builder()
            .stagingLocation(stagingDir)
            .rewrittenDeleteFilePathsCount(deleteFilesToRewrite.size())
            .rewrittenManifestFilePathsCount(manifestFiles.size())
            .latestVersion(RewriteTablePathUtil.fileName(endVersionName));

    if (!createFileList) {
      return builder.fileListLocation(NOT_APPLICABLE).build();
    }

    Set<Pair<String, String>> copyPlan = Sets.newHashSet();
    copyPlan.addAll(rewriteVersionResult.copyPlan());
    copyPlan.addAll(rewriteManifestListResult.copyPlan());
    copyPlan.addAll(rewriteManifestResult.copyPlan());
    String fileListLocation = saveFileList(copyPlan);

    return builder.fileListLocation(fileListLocation).build();
  }

  private String saveFileList(Set<Pair<String, String>> filesToMove) {
    String fileListPath = stagingDir + RESULT_LOCATION;
    OutputFile fileList = table.io().newOutputFile(fileListPath);
    writeAsCsv(filesToMove, fileList);
    return fileListPath;
  }

  private void writeAsCsv(Set<Pair<String, String>> rows, OutputFile outputFile) {
    try (BufferedWriter writer =
        new BufferedWriter(
            new OutputStreamWriter(outputFile.createOrOverwrite(), StandardCharsets.UTF_8))) {
      for (Pair<String, String> pair : rows) {
        writer.write(String.join(",", pair.first(), pair.second()));
        writer.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
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
    List<String> versionFilePaths = Lists.newArrayList();
    for (int i = versions.size() - 1; i >= 0; i--) {
      String versionFilePath = versions.get(i).file();
      if (versionFilePath.equals(startVersionName)) {
        break;
      }

      Preconditions.checkArgument(
          fileExist(versionFilePath),
          String.format("Version file %s doesn't exist", versionFilePath));
      versionFilePaths.add(versionFilePath);
    }

    Set<Snapshot> allSnapshots = Sets.newConcurrentHashSet();
    Set<Pair<String, String>> allCopyPlan = Sets.newConcurrentHashSet();
    Tasks.foreach(versionFilePaths)
        .noRetry()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(
            versionFilePath -> {
              TableMetadata tableMetadata =
                  new StaticTableOperations(versionFilePath, table.io()).current();
              allSnapshots.addAll(tableMetadata.snapshots());
              allCopyPlan.addAll(rewriteVersionFile(tableMetadata, versionFilePath));
            });

    result.toRewrite().addAll(allSnapshots);
    result.copyPlan().addAll(allCopyPlan);

    return result;
  }

  private Set<Pair<String, String>> rewriteVersionFile(
      TableMetadata metadata, String versionFilePath) {
    Set<Pair<String, String>> result = Sets.newHashSet();
    String stagingPath =
        RewriteTablePathUtil.stagingPath(versionFilePath, sourcePrefix, stagingDir);
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
    result.addAll(
        partitionStatsFileCopyPlan(
            metadata.partitionStatisticsFiles(), newTableMetadata.partitionStatisticsFiles()));
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
      result.add(Pair.of(before.path(), after.path()));
    }
    return result;
  }

  private Set<Pair<String, String>> partitionStatsFileCopyPlan(
      List<PartitionStatisticsFile> beforeStats, List<PartitionStatisticsFile> afterStats) {
    Set<Pair<String, String>> result = Sets.newHashSet();
    if (beforeStats.isEmpty()) {
      return result;
    }

    Preconditions.checkArgument(
        beforeStats.size() == afterStats.size(),
        "Before and after path rewrite, partition statistic files count should be same");
    for (int i = 0; i < beforeStats.size(); i++) {
      PartitionStatisticsFile before = beforeStats.get(i);
      PartitionStatisticsFile after = afterStats.get(i);
      Preconditions.checkArgument(
          before.fileSizeInBytes() == after.fileSizeInBytes(),
          "Before and after path rewrite, partition statistic file size should be same");
      result.add(Pair.of(before.path(), after.path()));
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
    String outputPath = RewriteTablePathUtil.stagingPath(path, sourcePrefix, stagingDir);
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
    @Override
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
      Set<Snapshot> deltaSnapshots,
      TableMetadata tableMetadata,
      Set<ManifestFile> toRewrite,
      Broadcast<Map<String, Long>> rewrittenDeleteFileSizes) {
    if (toRewrite.isEmpty()) {
      return new RewriteContentFileResult();
    }

    Encoder<ManifestFile> manifestFileEncoder = Encoders.javaSerialization(ManifestFile.class);
    Dataset<ManifestFile> manifestDS =
        spark().createDataset(Lists.newArrayList(toRewrite), manifestFileEncoder);
    Set<Long> deltaSnapshotIds =
        deltaSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

    return manifestDS
        .repartition(toRewrite.size())
        .map(
            toManifests(
                tableBroadcast(),
                sparkContext().broadcast(deltaSnapshotIds),
                stagingDir,
                tableMetadata.formatVersion(),
                sourcePrefix,
                targetPrefix,
                rewrittenDeleteFileSizes),
            Encoders.bean(RewriteContentFileResult.class))
        // duplicates are expected here as the same data file can have different statuses
        // (e.g. added and deleted)
        .reduce((ReduceFunction<RewriteContentFileResult>) RewriteContentFileResult::append);
  }

  private static MapFunction<ManifestFile, RewriteContentFileResult> toManifests(
      Broadcast<Table> table,
      Broadcast<Set<Long>> deltaSnapshotIds,
      String stagingLocation,
      int format,
      String sourcePrefix,
      String targetPrefix,
      Broadcast<Map<String, Long>> rewrittenDeleteFileSizes) {

    return manifestFile -> {
      RewriteContentFileResult result = new RewriteContentFileResult();
      switch (manifestFile.content()) {
        case DATA:
          result.appendDataFile(
              writeDataManifest(
                  manifestFile,
                  table,
                  deltaSnapshotIds,
                  stagingLocation,
                  format,
                  sourcePrefix,
                  targetPrefix));
          break;
        case DELETES:
          result.appendDeleteFile(
              writeDeleteManifest(
                  manifestFile,
                  table,
                  deltaSnapshotIds,
                  stagingLocation,
                  format,
                  sourcePrefix,
                  targetPrefix,
                  rewrittenDeleteFileSizes));
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
      Broadcast<Set<Long>> snapshotIds,
      String stagingLocation,
      int format,
      String sourcePrefix,
      String targetPrefix) {
    try {
      String stagingPath =
          RewriteTablePathUtil.stagingPath(manifestFile.path(), sourcePrefix, stagingLocation);
      FileIO io = table.getValue().io();
      OutputFile outputFile = io.newOutputFile(stagingPath);
      Map<Integer, PartitionSpec> specsById = table.getValue().specs();
      Set<Long> deltaSnapshotIds = snapshotIds.value();
      return RewriteTablePathUtil.rewriteDataManifest(
          manifestFile,
          deltaSnapshotIds,
          outputFile,
          io,
          format,
          specsById,
          sourcePrefix,
          targetPrefix);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static RewriteResult<DeleteFile> writeDeleteManifest(
      ManifestFile manifestFile,
      Broadcast<Table> table,
      Broadcast<Set<Long>> snapshotIds,
      String stagingLocation,
      int format,
      String sourcePrefix,
      String targetPrefix,
      Broadcast<Map<String, Long>> rewrittenDeleteFileSizes) {
    try {
      String stagingPath =
          RewriteTablePathUtil.stagingPath(manifestFile.path(), sourcePrefix, stagingLocation);
      FileIO io = table.getValue().io();
      OutputFile outputFile = io.newOutputFile(stagingPath);
      Map<Integer, PartitionSpec> specsById = table.getValue().specs();
      Set<Long> deltaSnapshotIds = snapshotIds.value();
      return RewriteTablePathUtil.rewriteDeleteManifest(
          manifestFile,
          deltaSnapshotIds,
          outputFile,
          io,
          format,
          specsById,
          sourcePrefix,
          targetPrefix,
          stagingLocation,
          rewrittenDeleteFileSizes.value());
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  /**
   * Enumerate the distinct position delete files referenced by the given delete manifests. Deduped
   * by identity (location, offset, size) so a file shared across manifests is counted once; the
   * physical rewrite is further deduped by location in {@link #rewritePositionDeletes}.
   */
  private Set<DeleteFile> positionDeletesToRewrite(Set<ManifestFile> deleteManifests) {
    if (deleteManifests.isEmpty()) {
      return Collections.emptySet();
    }

    Encoder<ManifestFile> manifestFileEncoder = Encoders.javaSerialization(ManifestFile.class);
    Dataset<ManifestFile> manifestDS =
        spark().createDataset(Lists.newArrayList(deleteManifests), manifestFileEncoder);
    Encoder<DeleteFile> deleteFileEncoder = Encoders.javaSerialization(DeleteFile.class);

    List<DeleteFile> referencedDeleteFiles =
        manifestDS
            .repartition(deleteManifests.size())
            .flatMap(positionDeletesInManifest(tableBroadcast()), deleteFileEncoder)
            .collectAsList();

    return DeleteFileSet.of(referencedDeleteFiles);
  }

  private static FlatMapFunction<ManifestFile, DeleteFile> positionDeletesInManifest(
      Broadcast<Table> tableArg) {
    return manifestFile -> {
      Table table = tableArg.getValue();
      List<DeleteFile> deleteFiles = Lists.newArrayList();
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifestFile, table.io(), table.specs())) {
        for (DeleteFile deleteFile : reader) {
          if (deleteFile.content() == FileContent.POSITION_DELETES) {
            deleteFiles.add(deleteFile.copy());
          }
        }
      }
      return deleteFiles.iterator();
    };
  }

  /**
   * Rewrite the given position delete files in parallel, returning a map from each source delete
   * file path to the size of its rewritten file. Physical files are deduped by location, so a
   * Puffin file holding multiple DVs is rewritten once and its size keyed once.
   */
  private Map<String, Long> rewritePositionDeletes(Set<DeleteFile> toRewrite) {
    if (toRewrite.isEmpty()) {
      return Collections.emptyMap();
    }

    // Multiple DVs can share one Puffin file at different blob offsets; rewrite each physical file
    // once. The measured size is keyed by location and applied to every referencing manifest entry.
    Map<String, DeleteFile> byLocation = Maps.newHashMapWithExpectedSize(toRewrite.size());
    for (DeleteFile deleteFile : toRewrite) {
      byLocation.putIfAbsent(deleteFile.location(), deleteFile);
    }
    List<DeleteFile> physicalFiles = Lists.newArrayList(byLocation.values());

    Encoder<DeleteFile> deleteFileEncoder = Encoders.javaSerialization(DeleteFile.class);
    Dataset<DeleteFile> deleteFileDS = spark().createDataset(physicalFiles, deleteFileEncoder);

    PositionDeleteReaderWriter posDeleteReaderWriter = new SparkPositionDeleteReaderWriter();
    List<Tuple2<String, Long>> rewrittenSizes =
        deleteFileDS
            .repartition(physicalFiles.size())
            .map(
                rewritePositionDelete(
                    tableBroadcast(),
                    sourcePrefix,
                    targetPrefix,
                    stagingDir,
                    posDeleteReaderWriter),
                Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
            .collectAsList();

    Map<String, Long> sizesBySourcePath = Maps.newHashMapWithExpectedSize(rewrittenSizes.size());
    for (Tuple2<String, Long> entry : rewrittenSizes) {
      sizesBySourcePath.put(entry._1(), entry._2());
    }
    return sizesBySourcePath;
  }

  private static MapFunction<DeleteFile, Tuple2<String, Long>> rewritePositionDelete(
      Broadcast<Table> tableArg,
      String sourcePrefixArg,
      String targetPrefixArg,
      String stagingLocationArg,
      PositionDeleteReaderWriter posDeleteReaderWriter) {
    return deleteFile -> {
      FileIO io = tableArg.getValue().io();
      String newPath =
          RewriteTablePathUtil.stagingPath(
              deleteFile.location(), sourcePrefixArg, stagingLocationArg);
      OutputFile outputFile = io.newOutputFile(newPath);
      PartitionSpec spec = tableArg.getValue().specs().get(deleteFile.specId());
      long rewrittenLength =
          RewriteTablePathUtil.rewritePositionDelete(
              deleteFile,
              outputFile,
              io,
              spec,
              sourcePrefixArg,
              targetPrefixArg,
              posDeleteReaderWriter);
      return new Tuple2<>(deleteFile.location(), rewrittenLength);
    };
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

  private static CloseableIterable<Record> positionDeletesReader(
      InputFile inputFile, FileFormat format, PartitionSpec spec) {
    return FormatModelRegistry.readBuilder(format, Record.class, inputFile)
        .project(DeleteSchemaUtil.posDeleteReadSchema(spec.schema()))
        .reuseContainers()
        .build();
  }

  private static PositionDeleteWriter<Record> positionDeletesWriter(
      OutputFile outputFile,
      FileFormat format,
      PartitionSpec spec,
      StructLike partition,
      Schema rowSchema)
      throws IOException {
    if (rowSchema == null) {
      return FormatModelRegistry.<Record>positionDeleteWriteBuilder(
              format, EncryptedFiles.plainAsEncryptedOutput(outputFile))
          .partition(partition)
          .spec(spec)
          .build();
    } else {
      return switch (format) {
        case AVRO ->
            Avro.writeDeletes(outputFile)
                .createWriterFunc(DataWriter::create)
                .withPartition(partition)
                .rowSchema(rowSchema)
                .withSpec(spec)
                .buildPositionWriter();
        case PARQUET ->
            Parquet.writeDeletes(outputFile)
                .createWriterFunc(GenericParquetWriter::create)
                .withPartition(partition)
                .rowSchema(rowSchema)
                .withSpec(spec)
                .buildPositionWriter();
        case ORC ->
            ORC.writeDeletes(outputFile)
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .withPartition(partition)
                .rowSchema(rowSchema)
                .withSpec(spec)
                .buildPositionWriter();
        default -> throw new UnsupportedOperationException("Unsupported file format: " + format);
      };
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
