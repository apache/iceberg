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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ImmutableRewriteManifests;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action that rewrites manifests in a distributed manner and co-locates metadata for partitions.
 *
 * <p>By default, this action rewrites all manifests for the current partition spec and writes the
 * result to the metadata folder. The behavior can be modified by passing a custom predicate to
 * {@link #rewriteIf(Predicate)} and a custom spec ID to {@link #specId(int)}. In addition, there is
 * a way to configure a custom location for staged manifests via {@link #stagingLocation(String)}.
 * The provided staging location will be ignored if snapshot ID inheritance is enabled. In such
 * cases, the manifests are always written to the metadata folder and committed without staging.
 */
public class RewriteManifestsSparkAction
    extends BaseManifestUpdateSparkAction<RewriteManifestsSparkAction> implements RewriteManifests {

  public static final String USE_CACHING = "use-caching";
  public static final boolean USE_CACHING_DEFAULT = false;

  private static final String FILENAME_PREFIX = "optimized-m-";

  private static final Logger LOG = LoggerFactory.getLogger(RewriteManifestsSparkAction.class);
  private static final RewriteManifests.Result EMPTY_RESULT =
      ImmutableRewriteManifests.Result.builder()
          .rewrittenManifests(ImmutableList.of())
          .addedManifests(ImmutableList.of())
          .build();

  private final Table table;
  private final int formatVersion;
  private final long targetManifestSizeBytes;
  private final boolean shouldStageManifests;

  private PartitionSpec spec = null;
  private Predicate<ManifestFile> predicate = manifest -> true;
  private String outputLocation = null;

  RewriteManifestsSparkAction(SparkSession spark, Table table) {
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

  @Override
  protected RewriteManifestsSparkAction self() {
    return this;
  }

  @Override
  public RewriteManifestsSparkAction specId(int specId) {
    Preconditions.checkArgument(table.specs().containsKey(specId), "Invalid spec id %s", specId);
    this.spec = table.specs().get(specId);
    return this;
  }

  @Override
  public RewriteManifestsSparkAction rewriteIf(Predicate<ManifestFile> newPredicate) {
    this.predicate = newPredicate;
    return this;
  }

  @Override
  public RewriteManifestsSparkAction stagingLocation(String newStagingLocation) {
    if (shouldStageManifests) {
      this.outputLocation = newStagingLocation;
    } else {
      LOG.warn("Ignoring provided staging location as new manifests will be committed directly");
    }
    return this;
  }

  @Override
  public RewriteManifests.Result execute() {
    String desc = String.format("Rewriting manifests in %s", table.name());
    JobGroupInfo info = newJobGroupInfo("REWRITE-MANIFESTS", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private RewriteManifests.Result doExecute() {
    List<ManifestFile> rewrittenManifests = Lists.newArrayList();
    List<ManifestFile> addedManifests = Lists.newArrayList();

    RewriteManifests.Result dataResult = rewriteManifests(ManifestContent.DATA);
    Iterables.addAll(rewrittenManifests, dataResult.rewrittenManifests());
    Iterables.addAll(addedManifests, dataResult.addedManifests());

    RewriteManifests.Result deletesResult = rewriteManifests(ManifestContent.DELETES);
    Iterables.addAll(rewrittenManifests, deletesResult.rewrittenManifests());
    Iterables.addAll(addedManifests, deletesResult.addedManifests());

    if (rewrittenManifests.isEmpty()) {
      return EMPTY_RESULT;
    }

    replaceManifests(rewrittenManifests, addedManifests, shouldStageManifests);

    return ImmutableRewriteManifests.Result.builder()
        .rewrittenManifests(rewrittenManifests)
        .addedManifests(addedManifests)
        .build();
  }

  private RewriteManifests.Result rewriteManifests(ManifestContent content) {
    List<ManifestFile> matchingManifests = findMatchingManifests(content);
    if (matchingManifests.isEmpty()) {
      return EMPTY_RESULT;
    }

    int targetNumManifests = targetNumManifests(totalSizeBytes(matchingManifests));
    if (targetNumManifests == 1 && matchingManifests.size() == 1) {
      return EMPTY_RESULT;
    }

    Dataset<Row> manifestEntryDF = buildManifestEntryDF(matchingManifests);

    List<ManifestFile> newManifests;
    if (spec.isUnpartitioned()) {
      newManifests = writeUnpartitionedManifests(content, manifestEntryDF, targetNumManifests);
    } else {
      newManifests = writePartitionedManifests(content, manifestEntryDF, targetNumManifests);
    }

    return ImmutableRewriteManifests.Result.builder()
        .rewrittenManifests(matchingManifests)
        .addedManifests(newManifests)
        .build();
  }

  private Dataset<Row> buildManifestEntryDF(List<ManifestFile> manifests) {
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
                "data_file");

    Column joinCond = manifestDF.col("manifest").equalTo(manifestEntryDF.col("manifest"));
    return manifestEntryDF
        .join(manifestDF, joinCond, "left_semi")
        .select("snapshot_id", "sequence_number", "file_sequence_number", "data_file");
  }

  private List<ManifestFile> writeUnpartitionedManifests(
      ManifestContent content, Dataset<Row> manifestEntryDF, int numManifests) {

    WriteManifests<?> writeFunc =
        newWriteManifestsFunc(
            content,
            manifestEntryDF.schema(),
            outputLocation,
            targetManifestSizeBytes,
            FILENAME_PREFIX);
    Dataset<Row> transformedManifestEntryDF = manifestEntryDF.repartition(numManifests);
    return writeFunc.apply(transformedManifestEntryDF).collectAsList();
  }

  private List<ManifestFile> writePartitionedManifests(
      ManifestContent content, Dataset<Row> manifestEntryDF, int numManifests) {

    return withReusableDS(
        manifestEntryDF,
        df -> {
          WriteManifests<?> writeFunc =
              newWriteManifestsFunc(
                  content, df.schema(), outputLocation, targetManifestSizeBytes, FILENAME_PREFIX);
          Column partitionColumn = df.col("data_file.partition");
          Dataset<Row> transformedDF = repartitionAndSort(df, partitionColumn, numManifests);
          return writeFunc.apply(transformedDF).collectAsList();
        });
  }

  private Dataset<Row> repartitionAndSort(Dataset<Row> df, Column col, int numPartitions) {
    return df.repartitionByRange(numPartitions, col).sortWithinPartitions(col);
  }

  private List<ManifestFile> findMatchingManifests(ManifestContent content) {
    Snapshot currentSnapshot = table.currentSnapshot();

    if (currentSnapshot == null) {
      return ImmutableList.of();
    }

    List<ManifestFile> manifests =
        loadManifests(
            content,
            currentSnapshot,
            manifest -> manifest.partitionSpecId() == spec.specId() && predicate.test(manifest));

    return manifests.stream()
        .filter(manifest -> manifest.partitionSpecId() == spec.specId() && predicate.test(manifest))
        .collect(Collectors.toList());
  }

  private int targetNumManifests(long totalSizeBytes) {
    return (int) ((totalSizeBytes + targetManifestSizeBytes - 1) / targetManifestSizeBytes);
  }

  private long totalSizeBytes(Iterable<ManifestFile> manifests) {
    long totalSizeBytes = 0L;

    for (ManifestFile manifest : manifests) {
      validateManifest(manifest);
      totalSizeBytes += manifest.length();
    }

    return totalSizeBytes;
  }
}
