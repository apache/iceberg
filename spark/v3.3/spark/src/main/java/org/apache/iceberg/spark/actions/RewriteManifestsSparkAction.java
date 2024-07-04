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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ImmutableRewriteManifests;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
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
    extends BaseSnapshotUpdateSparkAction<RewriteManifestsSparkAction> implements RewriteManifests {

  public static final String USE_CACHING = "use-caching";
  public static final boolean USE_CACHING_DEFAULT = false;

  private static final Logger LOG = LoggerFactory.getLogger(RewriteManifestsSparkAction.class);

  private final Encoder<ManifestFile> manifestEncoder;
  private final Table table;
  private final int formatVersion;
  private final long targetManifestSizeBytes;
  private final boolean shouldStageManifests;

  private PartitionSpec spec;
  private Predicate<ManifestFile> predicate = manifest -> true;
  private String outputLocation;

  RewriteManifestsSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.manifestEncoder = Encoders.javaSerialization(ManifestFile.class);
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
    List<ManifestFile> matchingManifests = findMatchingManifests();
    if (matchingManifests.isEmpty()) {
      return ImmutableRewriteManifests.Result.builder()
          .addedManifests(ImmutableList.of())
          .rewrittenManifests(ImmutableList.of())
          .build();
    }

    long totalSizeBytes = 0L;
    int numEntries = 0;

    for (ManifestFile manifest : matchingManifests) {
      ValidationException.check(
          hasFileCounts(manifest), "No file counts in manifest: %s", manifest.path());

      totalSizeBytes += manifest.length();
      numEntries +=
          manifest.addedFilesCount() + manifest.existingFilesCount() + manifest.deletedFilesCount();
    }

    int targetNumManifests = targetNumManifests(totalSizeBytes);
    int targetNumManifestEntries = targetNumManifestEntries(numEntries, targetNumManifests);

    if (targetNumManifests == 1 && matchingManifests.size() == 1) {
      return ImmutableRewriteManifests.Result.builder()
          .addedManifests(ImmutableList.of())
          .rewrittenManifests(ImmutableList.of())
          .build();
    }

    Dataset<Row> manifestEntryDF = buildManifestEntryDF(matchingManifests);

    List<ManifestFile> newManifests;
    if (spec.fields().size() < 1) {
      newManifests = writeManifestsForUnpartitionedTable(manifestEntryDF, targetNumManifests);
    } else {
      newManifests =
          writeManifestsForPartitionedTable(
              manifestEntryDF, targetNumManifests, targetNumManifestEntries);
    }

    replaceManifests(matchingManifests, newManifests);

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

  private List<ManifestFile> writeManifestsForUnpartitionedTable(
      Dataset<Row> manifestEntryDF, int numManifests) {

    Broadcast<Table> tableBroadcast =
        sparkContext().broadcast(SerializableTableWithSize.copyOf(table));
    StructType sparkType = (StructType) manifestEntryDF.schema().apply("data_file").dataType();
    Types.StructType combinedPartitionType = Partitioning.partitionType(table);

    // we rely only on the target number of manifests for unpartitioned tables
    // as we should not worry about having too much metadata per partition
    long maxNumManifestEntries = Long.MAX_VALUE;

    return manifestEntryDF
        .repartition(numManifests)
        .mapPartitions(
            toManifests(
                tableBroadcast,
                maxNumManifestEntries,
                outputLocation,
                formatVersion,
                combinedPartitionType,
                spec,
                sparkType),
            manifestEncoder)
        .collectAsList();
  }

  private List<ManifestFile> writeManifestsForPartitionedTable(
      Dataset<Row> manifestEntryDF, int numManifests, int targetNumManifestEntries) {

    Broadcast<Table> tableBroadcast =
        sparkContext().broadcast(SerializableTableWithSize.copyOf(table));
    StructType sparkType = (StructType) manifestEntryDF.schema().apply("data_file").dataType();
    Types.StructType combinedPartitionType = Partitioning.partitionType(table);

    // we allow the actual size of manifests to be 10% higher if the estimation is not precise
    // enough
    long maxNumManifestEntries = (long) (1.1 * targetNumManifestEntries);

    return withReusableDS(
        manifestEntryDF,
        df -> {
          Column partitionColumn = df.col("data_file.partition");
          return df.repartitionByRange(numManifests, partitionColumn)
              .sortWithinPartitions(partitionColumn)
              .mapPartitions(
                  toManifests(
                      tableBroadcast,
                      maxNumManifestEntries,
                      outputLocation,
                      formatVersion,
                      combinedPartitionType,
                      spec,
                      sparkType),
                  manifestEncoder)
              .collectAsList();
        });
  }

  private <T, U> U withReusableDS(Dataset<T> ds, Function<Dataset<T>, U> func) {
    boolean useCaching =
        PropertyUtil.propertyAsBoolean(options(), USE_CACHING, USE_CACHING_DEFAULT);
    Dataset<T> reusableDS = useCaching ? ds.cache() : ds;

    try {
      return func.apply(reusableDS);
    } finally {
      if (useCaching) {
        reusableDS.unpersist(false);
      }
    }
  }

  private List<ManifestFile> findMatchingManifests() {
    Snapshot currentSnapshot = table.currentSnapshot();

    if (currentSnapshot == null) {
      return ImmutableList.of();
    }

    return currentSnapshot.dataManifests(table.io()).stream()
        .filter(manifest -> manifest.partitionSpecId() == spec.specId() && predicate.test(manifest))
        .collect(Collectors.toList());
  }

  private int targetNumManifests(long totalSizeBytes) {
    return (int) ((totalSizeBytes + targetManifestSizeBytes - 1) / targetManifestSizeBytes);
  }

  private int targetNumManifestEntries(int numEntries, int numManifests) {
    return (numEntries + numManifests - 1) / numManifests;
  }

  private boolean hasFileCounts(ManifestFile manifest) {
    return manifest.addedFilesCount() != null
        && manifest.existingFilesCount() != null
        && manifest.deletedFilesCount() != null;
  }

  private void replaceManifests(
      Iterable<ManifestFile> deletedManifests, Iterable<ManifestFile> addedManifests) {
    try {
      org.apache.iceberg.RewriteManifests rewriteManifests = table.rewriteManifests();
      deletedManifests.forEach(rewriteManifests::deleteManifest);
      addedManifests.forEach(rewriteManifests::addManifest);
      commit(rewriteManifests);

      if (shouldStageManifests) {
        // delete new manifests as they were rewritten before the commit
        deleteFiles(Iterables.transform(addedManifests, ManifestFile::path));
      }
    } catch (CommitStateUnknownException commitStateUnknownException) {
      // don't clean up added manifest files, because they may have been successfully committed.
      throw commitStateUnknownException;
    } catch (Exception e) {
      if (e instanceof CleanableFailure) {
        // delete all new manifests because the rewrite failed
        deleteFiles(Iterables.transform(addedManifests, ManifestFile::path));
      }

      throw e;
    }
  }

  private void deleteFiles(Iterable<String> locations) {
    Iterable<FileInfo> files =
        Iterables.transform(locations, location -> new FileInfo(location, MANIFEST));
    if (table.io() instanceof SupportsBulkOperations) {
      deleteFiles((SupportsBulkOperations) table.io(), files.iterator());
    } else {
      deleteFiles(
          ThreadPools.getWorkerPool(), file -> table.io().deleteFile(file), files.iterator());
    }
  }

  private static ManifestFile writeManifest(
      List<Row> rows,
      int startIndex,
      int endIndex,
      Broadcast<Table> tableBroadcast,
      String location,
      int format,
      Types.StructType combinedPartitionType,
      PartitionSpec spec,
      StructType sparkType)
      throws IOException {

    String manifestName = "optimized-m-" + UUID.randomUUID();
    Path manifestPath = new Path(location, manifestName);
    OutputFile outputFile =
        tableBroadcast
            .value()
            .io()
            .newOutputFile(FileFormat.AVRO.addExtension(manifestPath.toString()));

    Types.StructType combinedFileType = DataFile.getType(combinedPartitionType);
    Types.StructType manifestFileType = DataFile.getType(spec.partitionType());
    SparkDataFile wrapper = new SparkDataFile(combinedFileType, manifestFileType, sparkType);

    ManifestWriter<DataFile> writer = ManifestFiles.write(format, spec, outputFile, null);

    try {
      for (int index = startIndex; index < endIndex; index++) {
        Row row = rows.get(index);
        long snapshotId = row.getLong(0);
        long sequenceNumber = row.getLong(1);
        Long fileSequenceNumber = row.isNullAt(2) ? null : row.getLong(2);
        Row file = row.getStruct(3);
        writer.existing(wrapper.wrap(file), snapshotId, sequenceNumber, fileSequenceNumber);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  private static MapPartitionsFunction<Row, ManifestFile> toManifests(
      Broadcast<Table> tableBroadcast,
      long maxNumManifestEntries,
      String location,
      int format,
      Types.StructType combinedPartitionType,
      PartitionSpec spec,
      StructType sparkType) {

    return rows -> {
      List<Row> rowsAsList = Lists.newArrayList(rows);

      if (rowsAsList.isEmpty()) {
        return Collections.emptyIterator();
      }

      List<ManifestFile> manifests = Lists.newArrayList();
      if (rowsAsList.size() <= maxNumManifestEntries) {
        manifests.add(
            writeManifest(
                rowsAsList,
                0,
                rowsAsList.size(),
                tableBroadcast,
                location,
                format,
                combinedPartitionType,
                spec,
                sparkType));
      } else {
        int midIndex = rowsAsList.size() / 2;
        manifests.add(
            writeManifest(
                rowsAsList,
                0,
                midIndex,
                tableBroadcast,
                location,
                format,
                combinedPartitionType,
                spec,
                sparkType));
        manifests.add(
            writeManifest(
                rowsAsList,
                midIndex,
                rowsAsList.size(),
                tableBroadcast,
                location,
                format,
                combinedPartitionType,
                spec,
                sparkType));
      }

      return manifests.iterator();
    };
  }
}
