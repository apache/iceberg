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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.RollingManifestWriter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ImmutableRewriteManifests;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkContentFile;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkDeleteFile;
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
    super(spark);
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

    replaceManifests(rewrittenManifests, addedManifests);

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

    WriteManifests<?> writeFunc = newWriteManifestsFunc(content, manifestEntryDF.schema());
    Dataset<Row> transformedManifestEntryDF = manifestEntryDF.repartition(numManifests);
    return writeFunc.apply(transformedManifestEntryDF).collectAsList();
  }

  private List<ManifestFile> writePartitionedManifests(
      ManifestContent content, Dataset<Row> manifestEntryDF, int numManifests) {

    return withReusableDS(
        manifestEntryDF,
        df -> {
          WriteManifests<?> writeFunc = newWriteManifestsFunc(content, df.schema());
          Column partitionColumn = df.col("data_file.partition");
          Dataset<Row> transformedDF = repartitionAndSort(df, partitionColumn, numManifests);
          return writeFunc.apply(transformedDF).collectAsList();
        });
  }

  private WriteManifests<?> newWriteManifestsFunc(ManifestContent content, StructType sparkType) {
    ManifestWriterFactory writers = manifestWriters();

    StructType sparkFileType = (StructType) sparkType.apply("data_file").dataType();
    Types.StructType combinedFileType = DataFile.getType(Partitioning.partitionType(table));
    Types.StructType fileType = DataFile.getType(spec.partitionType());

    if (content == ManifestContent.DATA) {
      return new WriteDataManifests(writers, combinedFileType, fileType, sparkFileType);
    } else {
      return new WriteDeleteManifests(writers, combinedFileType, fileType, sparkFileType);
    }
  }

  private Dataset<Row> repartitionAndSort(Dataset<Row> df, Column col, int numPartitions) {
    return df.repartitionByRange(numPartitions, col).sortWithinPartitions(col);
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

  private List<ManifestFile> findMatchingManifests(ManifestContent content) {
    Snapshot currentSnapshot = table.currentSnapshot();

    if (currentSnapshot == null) {
      return ImmutableList.of();
    }

    List<ManifestFile> manifests = loadManifests(content, currentSnapshot);

    return manifests.stream()
        .filter(manifest -> manifest.partitionSpecId() == spec.specId() && predicate.test(manifest))
        .collect(Collectors.toList());
  }

  private List<ManifestFile> loadManifests(ManifestContent content, Snapshot snapshot) {
    switch (content) {
      case DATA:
        return snapshot.dataManifests(table.io());
      case DELETES:
        return snapshot.deleteManifests(table.io());
      default:
        throw new IllegalArgumentException("Unknown manifest content: " + content);
    }
  }

  private int targetNumManifests(long totalSizeBytes) {
    return (int) ((totalSizeBytes + targetManifestSizeBytes - 1) / targetManifestSizeBytes);
  }

  private long totalSizeBytes(Iterable<ManifestFile> manifests) {
    long totalSizeBytes = 0L;

    for (ManifestFile manifest : manifests) {
      ValidationException.check(
          hasFileCounts(manifest), "No file counts in manifest: %s", manifest.path());
      totalSizeBytes += manifest.length();
    }

    return totalSizeBytes;
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
      // delete all new manifests because the rewrite failed
      deleteFiles(Iterables.transform(addedManifests, ManifestFile::path));
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

  private ManifestWriterFactory manifestWriters() {
    return new ManifestWriterFactory(
        sparkContext().broadcast(SerializableTableWithSize.copyOf(table)),
        formatVersion,
        spec.specId(),
        outputLocation,
        // allow the actual size of manifests to be 20% higher as the estimation is not precise
        (long) (1.2 * targetManifestSizeBytes));
  }

  private static class WriteDataManifests extends WriteManifests<DataFile> {

    WriteDataManifests(
        ManifestWriterFactory manifestWriters,
        Types.StructType combinedPartitionType,
        Types.StructType partitionType,
        StructType sparkFileType) {
      super(manifestWriters, combinedPartitionType, partitionType, sparkFileType);
    }

    @Override
    protected SparkDataFile newFileWrapper() {
      return new SparkDataFile(combinedFileType(), fileType(), sparkFileType());
    }

    @Override
    protected RollingManifestWriter<DataFile> newManifestWriter() {
      return writers().newRollingManifestWriter();
    }
  }

  private static class WriteDeleteManifests extends WriteManifests<DeleteFile> {

    WriteDeleteManifests(
        ManifestWriterFactory manifestWriters,
        Types.StructType combinedFileType,
        Types.StructType fileType,
        StructType sparkFileType) {
      super(manifestWriters, combinedFileType, fileType, sparkFileType);
    }

    @Override
    protected SparkDeleteFile newFileWrapper() {
      return new SparkDeleteFile(combinedFileType(), fileType(), sparkFileType());
    }

    @Override
    protected RollingManifestWriter<DeleteFile> newManifestWriter() {
      return writers().newRollingDeleteManifestWriter();
    }
  }

  private abstract static class WriteManifests<F extends ContentFile<F>>
      implements MapPartitionsFunction<Row, ManifestFile> {

    private static final Encoder<ManifestFile> MANIFEST_ENCODER =
        Encoders.javaSerialization(ManifestFile.class);

    private final ManifestWriterFactory writers;
    private final Types.StructType combinedFileType;
    private final Types.StructType fileType;
    private final StructType sparkFileType;

    WriteManifests(
        ManifestWriterFactory writers,
        Types.StructType combinedFileType,
        Types.StructType fileType,
        StructType sparkFileType) {
      this.writers = writers;
      this.combinedFileType = combinedFileType;
      this.fileType = fileType;
      this.sparkFileType = sparkFileType;
    }

    protected abstract SparkContentFile<F> newFileWrapper();

    protected abstract RollingManifestWriter<F> newManifestWriter();

    public Dataset<ManifestFile> apply(Dataset<Row> input) {
      return input.mapPartitions(this, MANIFEST_ENCODER);
    }

    @Override
    public Iterator<ManifestFile> call(Iterator<Row> rows) throws Exception {
      SparkContentFile<F> fileWrapper = newFileWrapper();
      RollingManifestWriter<F> writer = newManifestWriter();

      try {
        while (rows.hasNext()) {
          Row row = rows.next();
          long snapshotId = row.getLong(0);
          long sequenceNumber = row.getLong(1);
          Long fileSequenceNumber = row.isNullAt(2) ? null : row.getLong(2);
          Row file = row.getStruct(3);
          writer.existing(fileWrapper.wrap(file), snapshotId, sequenceNumber, fileSequenceNumber);
        }
      } finally {
        writer.close();
      }

      return writer.toManifestFiles().iterator();
    }

    protected ManifestWriterFactory writers() {
      return writers;
    }

    protected Types.StructType combinedFileType() {
      return combinedFileType;
    }

    protected Types.StructType fileType() {
      return fileType;
    }

    protected StructType sparkFileType() {
      return sparkFileType;
    }
  }

  private static class ManifestWriterFactory implements Serializable {
    private final Broadcast<Table> tableBroadcast;
    private final int formatVersion;
    private final int specId;
    private final String outputLocation;
    private final long maxManifestSizeBytes;

    ManifestWriterFactory(
        Broadcast<Table> tableBroadcast,
        int formatVersion,
        int specId,
        String outputLocation,
        long maxManifestSizeBytes) {
      this.tableBroadcast = tableBroadcast;
      this.formatVersion = formatVersion;
      this.specId = specId;
      this.outputLocation = outputLocation;
      this.maxManifestSizeBytes = maxManifestSizeBytes;
    }

    public RollingManifestWriter<DataFile> newRollingManifestWriter() {
      return new RollingManifestWriter<>(this::newManifestWriter, maxManifestSizeBytes);
    }

    private ManifestWriter<DataFile> newManifestWriter() {
      return ManifestFiles.write(formatVersion, spec(), newOutputFile(), null);
    }

    public RollingManifestWriter<DeleteFile> newRollingDeleteManifestWriter() {
      return new RollingManifestWriter<>(this::newDeleteManifestWriter, maxManifestSizeBytes);
    }

    private ManifestWriter<DeleteFile> newDeleteManifestWriter() {
      return ManifestFiles.writeDeleteManifest(formatVersion, spec(), newOutputFile(), null);
    }

    private PartitionSpec spec() {
      return table().specs().get(specId);
    }

    private OutputFile newOutputFile() {
      return table().io().newOutputFile(newManifestLocation());
    }

    private String newManifestLocation() {
      String fileName = FileFormat.AVRO.addExtension("optimized-m-" + UUID.randomUUID());
      Path filePath = new Path(outputLocation, fileName);
      return filePath.toString();
    }

    private Table table() {
      return tableBroadcast.value();
    }
  }
}
