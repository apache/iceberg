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

import static org.apache.iceberg.MetadataTableType.ENTRIES;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ImmutableRepairManifests;
import org.apache.iceberg.actions.RepairManifests;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairManifestsSparkAction
    extends BaseManifestUpdateSparkAction<RepairManifestsSparkAction> implements RepairManifests {

  private static final Logger LOG = LoggerFactory.getLogger(RepairManifestsSparkAction.class);

  protected static final RepairManifests.Result EMPTY_RESULT =
      ImmutableRepairManifests.Result.builder()
          .rewrittenManifests(ImmutableList.of())
          .addedManifests(ImmutableList.of())
          .missingFilesRemoved(0L)
          .duplicateFilesRemoved(0L)
          .build();

  private final Table table;

  private PartitionSpec spec = null;

  private final long targetManifestSizeBytes;

  private String outputLocation = null;

  private final int formatVersion;

  private final boolean shouldStageManifests;

  private boolean dryRunOnly = false;

  private boolean shouldRemoveMissingFiles = false;

  private boolean shouldDeduplicate = false;

  RepairManifestsSparkAction(SparkSession spark, Table table) {
    super(spark, table);
    this.table = table;
    this.spec = table.spec();
    this.targetManifestSizeBytes =
        PropertyUtil.propertyAsLong(
            table.properties(),
            TableProperties.MANIFEST_TARGET_SIZE_BYTES,
            TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT);

    // default the output location to the metadata location
    TableOperations ops = ((HasTableOperations) table).operations();
    Path metadataFilePath = new Path(ops.metadataFileLocation("file"));
    this.outputLocation = metadataFilePath.getParent().toString();

    // use the current table format version for new manifests
    this.formatVersion = ops.current().formatVersion();

    boolean snapshotIdInheritanceEnabled =
        PropertyUtil.propertyAsBoolean(
            table.properties(),
            TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED,
            TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);
    this.shouldStageManifests = formatVersion == 1 && !snapshotIdInheritanceEnabled;
  }

  public RepairManifestsSparkAction dryRun() {
    this.dryRunOnly = true;
    return this;
  }

  public RepairManifestsSparkAction removeMissingFiles() {
    this.shouldRemoveMissingFiles = true;
    return this;
  }

  public RepairManifestsSparkAction removeDuplicateCommittedFiles() {
    this.shouldDeduplicate = true;
    return this;
  }

  @Override
  protected RepairManifestsSparkAction self() {
    return this;
  }

  @Override
  public RepairManifests.Result execute() {
    return withJobGroupInfo(newJobGroupInfo("REPAIR-MANIFEST-FILES", jobDesc()), this::doExecute);
  }

  private RepairManifests.Result doExecute() {
    Snapshot snapshot = table.currentSnapshot();
    RepairManifests.Result dataResult = rewriteManifestAction(ManifestContent.DATA, snapshot);
    RepairManifests.Result deleteResult = rewriteManifestAction(ManifestContent.DELETES, snapshot);

    List<ManifestFile> rewrittenManifests = Lists.newArrayList();
    List<ManifestFile> addedManifests = Lists.newArrayList();
    Long numberDuplicatesRemoved = 0L;
    Long numberMissingFilesRemoved = 0L;

    dataResult.rewrittenManifests().forEach(rewrittenManifests::add);
    dataResult.addedManifests().forEach(addedManifests::add);
    numberDuplicatesRemoved += dataResult.duplicateFilesRemoved();
    numberMissingFilesRemoved += dataResult.missingFilesRemoved();
    deleteResult.rewrittenManifests().forEach(rewrittenManifests::add);
    deleteResult.addedManifests().forEach(addedManifests::add);
    numberDuplicatesRemoved += deleteResult.duplicateFilesRemoved();
    numberMissingFilesRemoved += deleteResult.missingFilesRemoved();

    LOG.info(
        "RepairAction: replaced {} manifests, added {} manifests, removed {} duplicate files, removed {} missing files",
        rewrittenManifests.size(),
        addedManifests.size(),
        numberDuplicatesRemoved,
        numberMissingFilesRemoved);

    return ImmutableRepairManifests.Result.builder()
        .rewrittenManifests(rewrittenManifests)
        .addedManifests(addedManifests)
        .duplicateFilesRemoved(numberDuplicatesRemoved)
        .missingFilesRemoved(numberMissingFilesRemoved)
        .build();
  }

  private Dataset<Row> baseDataframe(List<ManifestFile> manifests) {
    Dataset<Row> manifestDF =
        spark()
            .createDataset(Lists.transform(manifests, ManifestFile::path), Encoders.STRING())
            .toDF("manifest");

    Dataset<Row> manifestEntryDF =
        loadMetadataTable(table, ENTRIES)
            .filter("status < 2") // select only live entries
            .selectExpr(
                "input_file_name() as manifest",
                "snapshot_id",
                "sequence_number",
                "file_sequence_number",
                "data_file",
                "status");

    Column joinCond = manifestDF.col("manifest").equalTo(manifestEntryDF.col("manifest"));
    return manifestEntryDF
        .join(manifestDF, joinCond, "left_semi")
        .select(
            "manifest",
            "snapshot_id",
            "sequence_number",
            "file_sequence_number",
            "data_file",
            "status");
  }

  private Dataset<Row> withFileExists(Dataset<Row> base) {
    FileIO io = table.io();
    UserDefinedFunction fileExists =
        functions
            .udf(
                (String path) -> {
                  if (path == null || path.isEmpty()) {
                    return false;
                  } else {
                    return io.newInputFile(path).exists();
                  }
                },
                DataTypes.BooleanType)
            .withName("FILE_EXISTS_AT_LOCATION");

    return base.withColumn("file_exists", fileExists.apply(base.col("data_file.file_path")));
  }

  private Pair<List<ManifestFile>, Dataset<Row>> manifestsAndRowsDF(
      ManifestContent content, Snapshot snapshot) {
    List<ManifestFile> contentManifests = loadManifests(content, snapshot, unused -> true);

    if (contentManifests.isEmpty()) {
      return null;
    }

    // All files
    Dataset<Row> manifestEntryDF = baseDataframe(contentManifests);
    if (shouldRemoveMissingFiles) {
      manifestEntryDF = withFileExists(manifestEntryDF);
    }

    manifestEntryDF.cache();
    return Pair.of(contentManifests, manifestEntryDF);
  }

  private RepairManifests.Result rewriteManifestAction(ManifestContent content, Snapshot snapshot) {
    Pair<List<ManifestFile>, Dataset<Row>> manifestsAndDF = manifestsAndRowsDF(content, snapshot);
    if (manifestsAndDF == null) {
      return EMPTY_RESULT;
    }
    Dataset<Row> manifestEntryDF = manifestsAndDF.second();

    List<ManifestFile> contentManifests = manifestsAndDF.first();
    Map<String, ManifestFile> manifestsByPath = Maps.newHashMap();
    contentManifests.forEach(manifest -> manifestsByPath.put(manifest.path(), manifest));

    if (shouldRemoveMissingFiles && !shouldDeduplicate) {
      ReplaceManifestHelper procedure =
          new RemoveMissingFilesHelper(
              manifestEntryDF, manifestsByPath, spec.isUnpartitioned(), content);
      return procedure.apply();
    } else if (shouldDeduplicate && !shouldRemoveMissingFiles) {
      ReplaceManifestHelper procedure =
          new DeduplicateHelper(
              manifestEntryDF, manifestsByPath, spec.isUnpartitioned(), content) {};
      return procedure.apply();
    } else if (shouldDeduplicate && shouldRemoveMissingFiles) {
      DeduplicateHelper deduplicateProcedure =
          new DeduplicateHelper(
              manifestEntryDF, manifestsByPath, spec.isUnpartitioned(), content) {};

      RemoveMissingFilesHelper missingFilesProcedure =
          new RemoveMissingFilesHelper(
              manifestEntryDF, manifestsByPath, spec.isUnpartitioned(), content);
      return new CombinationRewriteHelper(
              manifestEntryDF, deduplicateProcedure, missingFilesProcedure)
          .apply();
    } else {
      return EMPTY_RESULT;
    }
  }

  private List<ManifestFile> writeUnpartitionedManifests(
      ManifestContent content, Dataset<Row> manifestEntryDF, int numManifests, String filePrefix) {

    WriteManifests<?> writeFunc =
        newWriteManifestsFunc(
            content, manifestEntryDF.schema(), outputLocation, targetManifestSizeBytes, filePrefix);
    Dataset<Row> transformedManifestEntryDF = manifestEntryDF.repartition(numManifests);
    return writeFunc.apply(transformedManifestEntryDF).collectAsList();
  }

  private List<ManifestFile> writePartitionedManifests(
      ManifestContent content, Dataset<Row> manifestEntryDF, int numManifests, String filePrefix) {

    return withReusableDS(
        manifestEntryDF,
        df -> {
          WriteManifests<?> writeFunc =
              newWriteManifestsFunc(
                  content, df.schema(), outputLocation, targetManifestSizeBytes, filePrefix);
          Column partitionColumn = df.col("data_file.partition");
          Dataset<Row> transformedDF = repartitionAndSort(df, partitionColumn, numManifests);
          return writeFunc.apply(transformedDF).collectAsList();
        });
  }

  private Dataset<Row> repartitionAndSort(Dataset<Row> df, Column col, int numPartitions) {
    return df.repartitionByRange(numPartitions, col).sortWithinPartitions(col);
  }

  private String jobDesc() {
    List<String> options = Lists.newArrayList();
    options.add("dryRun=" + dryRunOnly);
    options.add("removeMissingFile=" + shouldRemoveMissingFiles);
    options.add("removeDuplicateFiles=" + shouldDeduplicate);
    String optionsAsString = COMMA_JOINER.join(options);
    return String.format("Repairing manifest files from %s (%s)", table.name(), optionsAsString);
  }

  @Override
  public RepairManifests stagingLocation(String newStagingLocation) {
    if (shouldStageManifests) {
      this.outputLocation = newStagingLocation;
    } else {
      LOG.warn("Ignoring provided staging location as new manifests will be committed directly");
    }
    return this;
  }

  @Override
  public RepairManifests dryRun(boolean value) {
    if (value) {
      dryRun();
    }
    return this;
  }

  private interface ReplaceManifestHelper {
    RepairManifests.Result apply();
  }

  private class DeduplicateHelper implements ReplaceManifestHelper {

    private final Dataset<Row> manifestEntryDF;
    private final Map<String, ManifestFile> manifestsByPath;

    private final ManifestContent manifestContent;

    private final boolean isUnpartitioned;

    private static final String FILE_PREFIX = "deduped-m-";

    DeduplicateHelper(
        Dataset<Row> manifestEntryDF,
        Map<String, ManifestFile> manifestsByPath,
        boolean isUnpartitioned,
        ManifestContent manifestContent) {
      this.manifestEntryDF = manifestEntryDF;
      this.manifestsByPath = manifestsByPath;
      this.isUnpartitioned = isUnpartitioned;
      this.manifestContent = manifestContent;
    }

    @Override
    public Result apply() {

      Pair<Long, Dataset<Row>> groupedByFilePathPair = groupedByFilePathResult();
      if (groupedByFilePathPair == null) {
        return EMPTY_RESULT;
      }

      long numDuplicateFiles = groupedByFilePathPair.first();
      Dataset<Row> groupedByFilePath = groupedByFilePathPair.second();
      Dataset<Row> duplicatedFilesWithManifests =
          groupedByFilePath
              .withColumn("manifest", functions.explode(functions.col("manifests")))
              .cache();

      Dataset<Row> existingManifestsToRewriteDF =
          existingManifestsToRewrite(duplicatedFilesWithManifests);
      Dataset<Row> filesToWriteNewManifestsEarliestSequenceDF =
          newManifestRowsToWrite(duplicatedFilesWithManifests);

      List<ManifestFile> allManifestsToRewrite = manifestsToRemove(duplicatedFilesWithManifests);

      BiFunction<Long, Long, Void> comparisonFunction =
          (createdManifestsFilesCount, replacedManifestsFilesCount) -> {
            if ((createdManifestsFilesCount + numDuplicateFiles) != replacedManifestsFilesCount) {
              throw new ValidationException(
                  "Replaced and created manifests must have the same number of active files: %d (new), %d (old) accounting for %d duplicates",
                  createdManifestsFilesCount, replacedManifestsFilesCount, numDuplicateFiles);
            }
            return null;
          };

      return applyManifests(
          allManifestsToRewrite,
          existingManifestsToRewriteDF,
          filesToWriteNewManifestsEarliestSequenceDF,
          numDuplicateFiles,
          0L,
          comparisonFunction,
          FILE_PREFIX);
    }

    protected Pair<Long, Dataset<Row>> groupedByFilePathResult() {
      Dataset<Row> groupedByFilePath =
          manifestEntryDF
              .select(
                  functions.col("data_file.file_path").as("data_file_path"),
                  functions.col("manifest"))
              .groupBy("data_file_path")
              .agg(functions.collect_list("manifest").as("manifests"))
              .filter("size(manifests) > 1")
              .cache();

      if (groupedByFilePath.isEmpty()) {
        return null;
      }

      long numDuplicateFiles =
          groupedByFilePath
              .select("manifests")
              .withColumn("num_elements", functions.array_size(functions.col("manifests")).minus(1))
              .agg(functions.sum("num_elements"))
              .head()
              .getLong(0);

      return Pair.of(numDuplicateFiles, groupedByFilePath);
    }

    protected List<ManifestFile> manifestsToRemove(Dataset<Row> df) {
      return df.select("manifest").distinct().collectAsList().stream()
          .map(row -> manifestsByPath.get(row.getString(0)))
          .collect(Collectors.toList());
    }

    protected Dataset<Row> existingManifestsToRewrite(Dataset<Row> duplicatedFilesWithManifests) {
      return manifestEntryDF
          .join(
              duplicatedFilesWithManifests,
              duplicatedFilesWithManifests.col("manifest").equalTo(manifestEntryDF.col("manifest")),
              "left_semi")
          .join(
              duplicatedFilesWithManifests,
              duplicatedFilesWithManifests
                  .col("data_file_path")
                  .equalTo(manifestEntryDF.col("data_file.file_path")),
              "left_anti")
          .withColumn("data_file_path", functions.col("data_file.file_path"))
          .dropDuplicates("data_file_path");
    }

    protected Dataset<Row> newManifestRowsToWrite(Dataset<Row> duplicatedFilesWithManifests) {
      Dataset<Row> filesToWriteNewManifestsDF =
          manifestEntryDF
              .join(
                  duplicatedFilesWithManifests,
                  duplicatedFilesWithManifests
                      .col("data_file_path")
                      .equalTo(manifestEntryDF.col("data_file.file_path")),
                  "left_semi")
              .withColumn("data_file_path", functions.col("data_file.file_path"))
              .cache();

      return filesToWriteNewManifestsDF
          .groupBy("data_file_path")
          .agg(functions.min("sequence_number").alias("min_seq"))
          .join(
              filesToWriteNewManifestsDF,
              functions.expr(
                  "data_file.file_path = data_file.file_path AND sequence_number = min_seq"))
          .withColumn("data_file_path", functions.col("data_file.file_path"))
          .dropDuplicates("data_file_path");
    }

    protected long numManifestsFromHistory(List<ManifestFile> manifests, long numberDuplicates) {
      List<ManifestFile> sample = manifests.stream().limit(20).collect(Collectors.toList());
      List<Integer> manifestFileCount =
          sample.stream().map(ManifestFile::addedFilesCount).collect(Collectors.toList());
      int total = manifestFileCount.stream().mapToInt(Integer::intValue).sum();
      int numFilesPerManifest = Math.max(1, total / sample.size());
      return Math.max(1, numberDuplicates / numFilesPerManifest);
    }

    protected RepairManifests.Result applyManifests(
        List<ManifestFile> rewrittenManifests,
        Dataset<Row> existingManifestFilesToRewrite,
        Dataset<Row> filesForNewManifests,
        long numDuplicates,
        long numFilesRemoved,
        BiFunction<Long, Long, Void> comparisonFunction,
        String filePrefix) {
      Dataset<Row> reformattedStrippedDF =
          existingManifestFilesToRewrite.select(
              "snapshot_id", "sequence_number", "file_sequence_number", "data_file");
      Dataset<Row> reformattedReAddedFilesDf =
          filesForNewManifests.select(
              "snapshot_id", "sequence_number", "file_sequence_number", "data_file");

      List<ManifestFile> newStrippedManifests;
      List<ManifestFile> newReAddedFilesManifests;

      int numManifests =
          Math.toIntExact(numManifestsFromHistory(rewrittenManifests, numDuplicates));

      if (isUnpartitioned) {
        newStrippedManifests =
            writeUnpartitionedManifests(
                manifestContent, reformattedStrippedDF, rewrittenManifests.size(), filePrefix);
        newReAddedFilesManifests =
            writeUnpartitionedManifests(
                manifestContent, reformattedReAddedFilesDf, numManifests, filePrefix);
      } else {
        newStrippedManifests =
            writePartitionedManifests(
                manifestContent, reformattedStrippedDF, rewrittenManifests.size(), filePrefix);
        newReAddedFilesManifests =
            writePartitionedManifests(
                manifestContent, reformattedReAddedFilesDf, numManifests, filePrefix);
      }

      List<ManifestFile> allAddedManifests =
          Stream.concat(newStrippedManifests.stream(), newReAddedFilesManifests.stream())
              .collect(Collectors.toList());

      if (!dryRunOnly) {
        replaceManifests(
            rewrittenManifests, allAddedManifests, shouldStageManifests, comparisonFunction);
      }

      return ImmutableRepairManifests.Result.builder()
          .rewrittenManifests(rewrittenManifests)
          .addedManifests(allAddedManifests)
          .duplicateFilesRemoved(numDuplicates)
          .missingFilesRemoved(numFilesRemoved)
          .build();
    }
  }

  private class RemoveMissingFilesHelper implements ReplaceManifestHelper {

    private final Dataset<Row> manifestEntryDF;
    private final Map<String, ManifestFile> manifestsByPath;

    private final ManifestContent manifestContent;

    private final boolean isUnpartitioned;

    private static final String FILE_PREFIX = "stripped-m-";

    RemoveMissingFilesHelper(
        Dataset<Row> manifestEntryDF,
        Map<String, ManifestFile> manifestsByPath,
        boolean isUnpartitioned,
        ManifestContent manifestContent) {
      this.manifestEntryDF = manifestEntryDF;
      this.manifestsByPath = manifestsByPath;
      this.isUnpartitioned = isUnpartitioned;
      this.manifestContent = manifestContent;
    }

    @Override
    public Result apply() {
      Dataset<Row> missingDataFilesDF = manifestEntryDF.filter("file_exists == false").cache();

      // number removed files
      long numDataFilesRemoved = missingDataFilesDF.count();
      if (numDataFilesRemoved == 0) {
        return EMPTY_RESULT;
      }

      List<ManifestFile> filesAndManifestsToRewriteManifests =
          manifestFileFromRow(manifestsByPath, missingDataFilesDF);

      Dataset<Row> filesAndManifestsToRewriteDF =
          manifestEntryDF
              .join(
                  missingDataFilesDF,
                  missingDataFilesDF.col("manifest").equalTo(manifestEntryDF.col("manifest")),
                  "left_semi")
              .filter("file_exists == true")
              .withColumn("data_file_path", functions.col("data_file.file_path"))
              .dropDuplicates("manifest", "data_file_path")
              .select("snapshot_id", "sequence_number", "file_sequence_number", "data_file");

      return writeManifests(
          filesAndManifestsToRewriteManifests, filesAndManifestsToRewriteDF, numDataFilesRemoved);
    }

    private RepairManifests.Result writeManifests(
        List<ManifestFile> rewrittenManifests, Dataset<Row> strippedDF, long numDataFilesRemoved) {

      List<ManifestFile> newStrippedManifests;

      if (isUnpartitioned) {
        newStrippedManifests =
            writeUnpartitionedManifests(
                manifestContent, strippedDF, rewrittenManifests.size(), FILE_PREFIX);
      } else {
        newStrippedManifests =
            writePartitionedManifests(
                manifestContent, strippedDF, rewrittenManifests.size(), FILE_PREFIX);
      }

      BiFunction<Long, Long, Void> comparisonFn =
          (createdManifestsFilesCount, replacedManifestsFilesCount) -> {
            if (!((createdManifestsFilesCount + numDataFilesRemoved)
                == replacedManifestsFilesCount)) {
              throw new ValidationException(
                  "Replaced and created manifests must have the same number of active files: %d (new), %d (old) accounting for %d non-existing files removed",
                  createdManifestsFilesCount, replacedManifestsFilesCount);
            }
            return null;
          };

      if (!dryRunOnly) {
        replaceManifests(
            rewrittenManifests, newStrippedManifests, shouldStageManifests, comparisonFn);
      }
      return ImmutableRepairManifests.Result.builder()
          .rewrittenManifests(rewrittenManifests)
          .addedManifests(Lists.newArrayList())
          .duplicateFilesRemoved(0L)
          .missingFilesRemoved(numDataFilesRemoved)
          .build();
    }

    private List<ManifestFile> manifestFileFromRow(
        Map<String, ManifestFile> allManifests, Dataset<Row> df) {
      return df.select("manifest").distinct().collectAsList().stream()
          .map(row -> allManifests.get(row.getString(0)))
          .collect(Collectors.toList());
    }
  }

  class CombinationRewriteHelper implements ReplaceManifestHelper {

    private final DeduplicateHelper dedupeProc;

    private final RemoveMissingFilesHelper missingFilesProc;
    private final Dataset<Row> manifestEntryDF;

    private static final String FILE_PREFIX = "combo-m-";

    CombinationRewriteHelper(
        Dataset<Row> manifestEntryDF,
        DeduplicateHelper dedupeProc,
        RemoveMissingFilesHelper missingFilesProc) {
      this.dedupeProc = dedupeProc;
      this.manifestEntryDF = manifestEntryDF;
      this.missingFilesProc = missingFilesProc;
    }

    @Override
    public Result apply() {
      Pair<Long, Dataset<Row>> groupedByFilePathPair = dedupeProc.groupedByFilePathResult();
      if (groupedByFilePathPair == null) {
        return missingFilesProc.apply();
      } else {
        long numDuplicateFiles = groupedByFilePathPair.first();

        Dataset<Row> missingDataFilesDF =
            manifestEntryDF
                .filter("file_exists == false")
                .withColumn("data_file_path", functions.col("data_file.file_path"))
                .cache();

        long numDataFilesRemoved = missingDataFilesDF.count();

        Dataset<Row> rowsFromManifestsWithMissingFilesDF =
            manifestEntryDF
                .join(
                    missingDataFilesDF,
                    missingDataFilesDF.col("manifest").equalTo(manifestEntryDF.col("manifest")),
                    "left_semi")
                .filter("file_exists == true")
                .withColumn("data_file_path", functions.col("data_file.file_path"))
                .select(
                    "snapshot_id",
                    "sequence_number",
                    "file_sequence_number",
                    "data_file",
                    "data_file_path");

        Dataset<Row> groupedByFilePath = groupedByFilePathPair.second();
        Dataset<Row> duplicatedFilesWithManifests =
            groupedByFilePath
                .withColumn("manifest", functions.explode(functions.col("manifests")))
                .cache();

        Dataset<Row> existingManifestsToRewriteDupesDF =
            dedupeProc
                .existingManifestsToRewrite(duplicatedFilesWithManifests)
                .select(
                    "snapshot_id",
                    "sequence_number",
                    "file_sequence_number",
                    "data_file",
                    "data_file_path");

        Dataset<Row> allExistingManifestRowsToRewriteDF =
            existingManifestsToRewriteDupesDF
                .union(rowsFromManifestsWithMissingFilesDF)
                .dropDuplicates("data_file_path");

        Dataset<Row> deduplicatedRowsForNewManifest =
            dedupeProc.newManifestRowsToWrite(duplicatedFilesWithManifests);

        // remove any missing files that are also dupes
        Dataset<Row> uniqueDeduplicatedRowsForNewManifest =
            deduplicatedRowsForNewManifest
                .join(
                    missingDataFilesDF,
                    missingDataFilesDF
                        .col("data_file.file_path")
                        .equalTo(deduplicatedRowsForNewManifest.col("data_file.file_path")),
                    "left_anti")
                .cache();

        List<ManifestFile> dedupeManifestsToRewrite =
            dedupeProc.manifestsToRemove(duplicatedFilesWithManifests);
        List<ManifestFile> missingFilesManifestsToRemove =
            dedupeProc.manifestsToRemove(missingDataFilesDF);

        // count of missing and/or deduplicated
        long numFilesRemovedAllSources =
            missingDataFilesDF.dropDuplicates("data_file_path").count() + numDuplicateFiles;

        System.out.println(numFilesRemovedAllSources);

        dedupeManifestsToRewrite.addAll(missingFilesManifestsToRemove);

        BiFunction<Long, Long, Void> comparisonFunction =
            (createdManifestsFilesCount, replacedManifestsFilesCount) -> {
              if ((createdManifestsFilesCount + numFilesRemovedAllSources)
                  != replacedManifestsFilesCount) {
                throw new ValidationException(
                    "Replaced and created manifests must have the same number of active files: %d (new), %d (old) accounting for %d duplicates/missing files",
                    createdManifestsFilesCount,
                    replacedManifestsFilesCount,
                    numFilesRemovedAllSources);
              }
              return null;
            };

        return dedupeProc.applyManifests(
            dedupeManifestsToRewrite.stream().distinct().collect(Collectors.toList()),
            allExistingManifestRowsToRewriteDF,
            uniqueDeduplicatedRowsForNewManifest,
            numDuplicateFiles,
            numDataFilesRemoved,
            comparisonFunction,
            FILE_PREFIX);
      }
    }
  }
}
