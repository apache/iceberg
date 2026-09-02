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
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.RollingManifestWriter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ImmutableRepairTable;
import org.apache.iceberg.actions.RepairTable;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.mapping.NameMapping;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * An action that repairs incorrect statistics in the manifests of a table.
 *
 * <p>The statistics of every live manifest entry are compared against the file the entry refers to.
 * Only manifests that contain at least one incorrect entry are rewritten, so the cost of the commit
 * is proportional to the number of incorrect entries rather than to the size of the table.
 */
public class RepairTableSparkAction extends BaseSnapshotUpdateSparkAction<RepairTableSparkAction>
    implements RepairTable {

  public static final String USE_CACHING = "use-caching";
  public static final boolean USE_CACHING_DEFAULT = false;

  /**
   * Whether to compare and repair column level statistics. When disabled, only record counts and
   * file sizes are compared and repaired.
   *
   * <p>This is disabled by default. Recomputed column statistics reflect the current metrics config
   * of the table, but the config a file was written under is not recorded, so a table whose config
   * changed reports column statistics that legitimately differ from the recomputed ones. Repairing
   * them in that case would overwrite correct statistics. Reading the footer of every candidate
   * file happens regardless of this option; it only controls whether column statistics are
   * compared.
   */
  public static final String REPAIR_COLUMN_METRICS = "repair-column-metrics";

  public static final boolean REPAIR_COLUMN_METRICS_DEFAULT = false;

  private static final Logger LOG = LoggerFactory.getLogger(RepairTableSparkAction.class);

  private static final RepairTable.Result EMPTY_RESULT =
      ImmutableRepairTable.Result.builder()
          .repairedManifests(ImmutableList.of())
          .repairedEntryCount(0L)
          .build();

  private static final String NEW_MANIFEST_PREFIX = "repaired-m-";

  private final Table table;
  private final int formatVersion;
  private final long targetManifestSizeBytes;
  private final boolean shouldStageManifests;
  private final String outputLocation;

  private boolean repairFileMetrics = false;
  private boolean dryRun = false;

  RepairTableSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.targetManifestSizeBytes =
        PropertyUtil.propertyAsLong(
            table.properties(),
            TableProperties.MANIFEST_TARGET_SIZE_BYTES,
            TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT);

    TableOperations ops = ((HasTableOperations) table).operations();
    Path metadataFilePath = new Path(ops.metadataFileLocation("file"));
    this.outputLocation = metadataFilePath.getParent().toString();
    this.formatVersion = ops.current().formatVersion();

    boolean snapshotIdInheritanceEnabled =
        PropertyUtil.propertyAsBoolean(
            table.properties(),
            TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED,
            TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);
    this.shouldStageManifests = formatVersion == 1 && !snapshotIdInheritanceEnabled;
  }

  @Override
  protected RepairTableSparkAction self() {
    return this;
  }

  @Override
  public RepairTableSparkAction repairFileMetrics() {
    this.repairFileMetrics = true;
    return this;
  }

  @Override
  public RepairTableSparkAction dryRun() {
    this.dryRun = true;
    return this;
  }

  @Override
  public RepairTable.Result execute() {
    String desc = String.format("Repairing manifests in %s (dryRun=%s)", table.name(), dryRun);
    JobGroupInfo info = newJobGroupInfo("REPAIR-TABLE", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private RepairTable.Result doExecute() {
    if (!repairFileMetrics) {
      // no repair was selected through the configuration methods, so there is nothing to do
      return EMPTY_RESULT;
    }

    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot == null) {
      return EMPTY_RESULT;
    }

    List<ManifestFile> repairedManifests = Lists.newArrayList();
    List<ManifestFile> newManifests = Lists.newArrayList();
    long repairedCount = 0L;

    for (ManifestContent content : ManifestContent.values()) {
      RepairedManifests repaired = repairTable(content, currentSnapshot);
      repairedManifests.addAll(repaired.repairedManifests());
      newManifests.addAll(repaired.newManifests());
      repairedCount += repaired.repairedCount();
    }

    if (repairedManifests.isEmpty()) {
      return EMPTY_RESULT;
    }

    // a dry run writes no manifests, so there is nothing to commit or clean up
    if (!dryRun) {
      replaceManifests(repairedManifests, newManifests);
    }

    LOG.info(
        "Repaired the stats of {} manifest entries, rewriting {} manifests as {} (dryRun={})",
        repairedCount,
        repairedManifests.size(),
        newManifests.size(),
        dryRun);

    return ImmutableRepairTable.Result.builder()
        .repairedManifests(repairedManifests)
        .repairedEntryCount(repairedCount)
        .build();
  }

  private RepairedManifests repairTable(ManifestContent content, Snapshot snapshot) {
    List<ManifestFile> manifests = loadManifests(content, snapshot);
    if (manifests.isEmpty()) {
      return RepairedManifests.empty();
    }

    // A manifest is rewritten with the spec it was written under, so manifests are grouped by
    // spec and each group is repaired separately. Rewriting a manifest of an older spec with the
    // current spec of the table would change the partition data of its entries.
    Map<Integer, List<ManifestFile>> manifestsBySpecId =
        manifests.stream().collect(Collectors.groupingBy(ManifestFile::partitionSpecId));

    List<ManifestFile> repairedManifests = Lists.newArrayList();
    List<ManifestFile> newManifests = Lists.newArrayList();
    long repairedCount = 0L;

    for (Map.Entry<Integer, List<ManifestFile>> group : manifestsBySpecId.entrySet()) {
      RepairedManifests repaired = repairManifests(content, group.getKey(), group.getValue());
      repairedManifests.addAll(repaired.repairedManifests());
      newManifests.addAll(repaired.newManifests());
      repairedCount += repaired.repairedCount();
    }

    return RepairedManifests.of(repairedManifests, newManifests, repairedCount);
  }

  private RepairedManifests repairManifests(
      ManifestContent content, int specId, List<ManifestFile> manifests) {
    Dataset<Row> entryDF = buildManifestEntryDF(manifests);

    return withReusableDS(
        entryDF,
        df -> {
          // the entries whose stats disagree with the files they refer to, as (manifest, path).
          // cached because it is small, one row per incorrect entry, and is read by several actions
          // below, whereas recomputing it would re-read every file.
          Dataset<Row> verdicts =
              df.mapPartitions(
                      newCheckStatsFunc(content, specId),
                      Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                  .toDF("manifest", "path")
                  .cache();

          try {
            List<String> manifestsToRewrite =
                verdicts.select("manifest").distinct().as(Encoders.STRING()).collectAsList();

            if (manifestsToRewrite.isEmpty()) {
              return RepairedManifests.empty();
            }

            long repairedCount = verdicts.count();
            List<ManifestFile> rewritten =
                manifests.stream()
                    .filter(manifest -> manifestsToRewrite.contains(manifest.path()))
                    .collect(Collectors.toList());

            // a dry run reports what would be repaired without writing any manifests
            if (dryRun) {
              return RepairedManifests.of(rewritten, ImmutableList.of(), repairedCount);
            }

            // mark every entry of the affected manifests with whether its stats need repair by
            // joining on the file path, so the unbounded per file set stays distributed rather than
            // being collected to the driver and broadcast back out
            Dataset<Row> entriesToRewrite =
                df.filter(df.col("manifest").isin(manifestsToRewrite.toArray()));
            Dataset<Row> markedEntries = markEntriesToRepair(entriesToRewrite, verdicts);
            List<ManifestFile> written =
                writeManifests(content, specId, markedEntries, rewritten.size());

            return RepairedManifests.of(rewritten, written, repairedCount);
          } finally {
            verdicts.unpersist(false);
          }
        });
  }

  /**
   * Marks every entry with a boolean {@code repair} column that is true when the entry's file has
   * incorrect statistics, by left joining the entries against the verdicts on the file path.
   */
  private Dataset<Row> markEntriesToRepair(Dataset<Row> entries, Dataset<Row> verdicts) {
    Dataset<Row> repairedPaths =
        verdicts.select(verdicts.col("path").as("repaired_path")).distinct();
    return entries
        .join(
            repairedPaths,
            entries.col("data_file.file_path").equalTo(repairedPaths.col("repaired_path")),
            "left")
        .withColumn("repair", functions.col("repaired_path").isNotNull())
        .drop("repaired_path")
        .select(
            "manifest",
            "snapshot_id",
            "sequence_number",
            "file_sequence_number",
            "data_file",
            "repair");
  }

  /**
   * Loads the live entries of the given manifests, keeping the manifest each entry was read from so
   * that only the manifests containing an incorrect entry are rewritten.
   */
  private Dataset<Row> buildManifestEntryDF(List<ManifestFile> manifests) {
    Dataset<Row> manifestDF =
        spark()
            .createDataset(Lists.transform(manifests, ManifestFile::path), Encoders.STRING())
            .toDF("manifest");

    Dataset<Row> entryDF =
        loadMetadataTable(table, ENTRIES)
            .filter("status < 2") // select only live entries
            .selectExpr(
                "input_file_name() as manifest",
                "snapshot_id",
                "sequence_number",
                "file_sequence_number",
                "data_file");

    return entryDF.join(
        manifestDF, manifestDF.col("manifest").equalTo(entryDF.col("manifest")), "left_semi");
  }

  private List<ManifestFile> writeManifests(
      ManifestContent content, int specId, Dataset<Row> entryDF, int numManifests) {
    StructType sparkType = (StructType) entryDF.schema().apply("data_file").dataType();
    Types.StructType combinedFileType = DataFile.getType(Partitioning.partitionType(table));
    Types.StructType fileType = DataFile.getType(table.specs().get(specId).partitionType());
    ManifestWriterFactory writers = manifestWriters(specId);
    RepairContext context = newRepairContext(content, specId);

    WriteManifests<?> writeFunc =
        content == ManifestContent.DATA
            ? new WriteDataManifests(writers, combinedFileType, fileType, sparkType, context)
            : new WriteDeleteManifests(writers, combinedFileType, fileType, sparkType, context);

    // repartition by manifest so the entries of each manifest are written together and the layout
    // of the table is preserved, rather than scattered round robin as a plain repartition(n) would.
    // this produces about as many manifests as are being replaced.
    return writeFunc
        .apply(entryDF.repartition(numManifests, entryDF.col("manifest")))
        .collectAsList();
  }

  private CheckStats newCheckStatsFunc(ManifestContent content, int specId) {
    return new CheckStats(newRepairContext(content, specId));
  }

  private RepairContext newRepairContext(ManifestContent content, int specId) {
    boolean repairColumnMetrics =
        PropertyUtil.propertyAsBoolean(
            options(), REPAIR_COLUMN_METRICS, REPAIR_COLUMN_METRICS_DEFAULT);
    return new RepairContext(
        sparkContext().broadcast(SerializableTableWithSize.copyOf(table)),
        content,
        specId,
        repairColumnMetrics);
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
    } catch (CommitStateUnknownException e) {
      // don't clean up added manifest files, because they may have been successfully committed
      throw e;
    } catch (Exception e) {
      if (e instanceof CleanableFailure) {
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

  private ManifestWriterFactory manifestWriters(int specId) {
    return new ManifestWriterFactory(
        sparkContext().broadcast(SerializableTableWithSize.copyOf(table)),
        formatVersion,
        specId,
        outputLocation,
        // allow the actual size of manifests to be 20% higher as the estimation is not precise
        (long) (1.2 * targetManifestSizeBytes));
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

  /** The outcome of repairing the manifests of one content type. */
  private static class RepairedManifests {
    private final List<ManifestFile> repairedManifests;
    private final List<ManifestFile> newManifests;
    private final long repairedCount;

    private RepairedManifests(
        List<ManifestFile> repairedManifests, List<ManifestFile> newManifests, long repairedCount) {
      this.repairedManifests = repairedManifests;
      this.newManifests = newManifests;
      this.repairedCount = repairedCount;
    }

    static RepairedManifests empty() {
      return new RepairedManifests(ImmutableList.of(), ImmutableList.of(), 0L);
    }

    static RepairedManifests of(
        List<ManifestFile> repairedManifests, List<ManifestFile> newManifests, long repairedCount) {
      return new RepairedManifests(repairedManifests, newManifests, repairedCount);
    }

    List<ManifestFile> repairedManifests() {
      return repairedManifests;
    }

    List<ManifestFile> newManifests() {
      return newManifests;
    }

    long repairedCount() {
      return repairedCount;
    }
  }

  /**
   * The state needed to read the statistics of a file on an executor.
   *
   * <p>The table is broadcast so that the file IO, schema and metrics config are available without
   * being resolved for every entry.
   */
  private static class RepairContext implements Serializable {
    private final Broadcast<Table> tableBroadcast;
    private final ManifestContent content;
    private final int specId;
    private final boolean repairColumnMetrics;

    private transient Map<FileContent, MetricsConfig> lazyMetricsConfigs = null;
    private transient NameMapping lazyNameMapping = null;
    private transient boolean nameMappingResolved = false;

    RepairContext(
        Broadcast<Table> tableBroadcast,
        ManifestContent content,
        int specId,
        boolean repairColumnMetrics) {
      this.tableBroadcast = tableBroadcast;
      this.content = content;
      this.specId = specId;
      this.repairColumnMetrics = repairColumnMetrics;
    }

    Table table() {
      return tableBroadcast.value();
    }

    FileIO io() {
      return table().io();
    }

    ManifestContent content() {
      return content;
    }

    boolean repairColumnMetrics() {
      return repairColumnMetrics;
    }

    PartitionSpec spec(int id) {
      return table().specs().get(id);
    }

    /**
     * Returns the metrics config for the content type of the given file. A delete manifest can hold
     * both position and equality deletes, whose configs differ, so the config is cached per content
     * type rather than once for the whole manifest.
     */
    MetricsConfig metricsConfig(ContentFile<?> file) {
      if (lazyMetricsConfigs == null) {
        this.lazyMetricsConfigs = new EnumMap<>(FileContent.class);
      }

      return lazyMetricsConfigs.computeIfAbsent(
          file.content(), fileContent -> RepairMetrics.metricsConfig(table(), fileContent));
    }

    NameMapping nameMapping() {
      if (!nameMappingResolved) {
        this.lazyNameMapping = RepairMetrics.nameMapping(table());
        this.nameMappingResolved = true;
      }

      return lazyNameMapping;
    }

    SparkContentFile<?> newFileWrapper(Types.StructType combinedFileType, StructType sparkType) {
      Types.StructType fileType = DataFile.getType(spec(specId).partitionType());
      return content == ManifestContent.DATA
          ? new SparkDataFile(combinedFileType, fileType, sparkType)
          : new SparkDeleteFile(combinedFileType, fileType, sparkType);
    }
  }

  /**
   * Compares the statistics of every entry against the file the entry refers to, emitting the
   * manifest path and file path of each entry whose statistics are incorrect.
   */
  private static class CheckStats implements MapPartitionsFunction<Row, Tuple2<String, String>> {
    private final RepairContext context;

    CheckStats(RepairContext context) {
      this.context = context;
    }

    @Override
    public Iterator<Tuple2<String, String>> call(Iterator<Row> rows) {
      List<Tuple2<String, String>> verdicts = Lists.newArrayList();
      // the combined file type and the wrapper are identical for every row of the partition
      Types.StructType combinedFileType =
          DataFile.getType(Partitioning.partitionType(context.table()));
      SparkContentFile<?> fileWrapper = null;

      while (rows.hasNext()) {
        Row row = rows.next();
        String manifest = row.getString(0);
        Row fileRow = row.getStruct(4);
        if (fileWrapper == null) {
          fileWrapper = context.newFileWrapper(combinedFileType, (StructType) fileRow.schema());
        }

        ContentFile<?> file = (ContentFile<?>) fileWrapper.wrap(fileRow);

        if (!RepairMetrics.supportsMetrics(file)) {
          continue;
        }

        String location = file.location().toString();

        try {
          InputFile input = context.io().newInputFile(location);
          long fileSizeInBytes = input.getLength();
          Metrics metrics =
              RepairMetrics.readMetrics(
                  input, file, context.metricsConfig(file), context.nameMapping());

          if (RepairMetrics.statsAreIncorrect(
              file, metrics, fileSizeInBytes, context.repairColumnMetrics())) {
            verdicts.add(new Tuple2<>(manifest, location));
          }
        } catch (Exception e) {
          // the stats of a file that cannot be read are left alone, as whether they are
          // correct cannot be told without reading it
          LOG.warn("Skipping the entry of {} as its statistics could not be read", location, e);
        }
      }

      return verdicts.iterator();
    }
  }

  private static class WriteDataManifests extends WriteManifests<DataFile> {
    WriteDataManifests(
        ManifestWriterFactory writers,
        Types.StructType combinedFileType,
        Types.StructType fileType,
        StructType sparkFileType,
        RepairContext context) {
      super(writers, combinedFileType, fileType, sparkFileType, context);
    }

    @Override
    protected SparkContentFile<DataFile> newFileWrapper() {
      return new SparkDataFile(combinedFileType(), fileType(), sparkFileType());
    }

    @Override
    protected RollingManifestWriter<DataFile> newManifestWriter() {
      return writers().newRollingManifestWriter();
    }
  }

  private static class WriteDeleteManifests extends WriteManifests<DeleteFile> {
    WriteDeleteManifests(
        ManifestWriterFactory writers,
        Types.StructType combinedFileType,
        Types.StructType fileType,
        StructType sparkFileType,
        RepairContext context) {
      super(writers, combinedFileType, fileType, sparkFileType, context);
    }

    @Override
    protected SparkContentFile<DeleteFile> newFileWrapper() {
      return new SparkDeleteFile(combinedFileType(), fileType(), sparkFileType());
    }

    @Override
    protected RollingManifestWriter<DeleteFile> newManifestWriter() {
      return writers().newRollingDeleteManifestWriter();
    }
  }

  /**
   * Writes the entries of the manifests being repaired, replacing the statistics of the entries
   * that were found to be incorrect.
   *
   * <p>Entries are always written with {@link RollingManifestWriter#existing}, carrying the
   * original snapshot id and sequence numbers so that the lineage of the files, and therefore the
   * delete files that apply to them, is preserved.
   */
  private abstract static class WriteManifests<F extends ContentFile<F>>
      implements MapPartitionsFunction<Row, ManifestFile> {

    private static final Encoder<ManifestFile> MANIFEST_ENCODER =
        Encoders.javaSerialization(ManifestFile.class);

    private final ManifestWriterFactory writers;
    private final Types.StructType combinedFileType;
    private final Types.StructType fileType;
    private final StructType sparkFileType;
    private final RepairContext context;

    WriteManifests(
        ManifestWriterFactory writers,
        Types.StructType combinedFileType,
        Types.StructType fileType,
        StructType sparkFileType,
        RepairContext context) {
      this.writers = writers;
      this.combinedFileType = combinedFileType;
      this.fileType = fileType;
      this.sparkFileType = sparkFileType;
      this.context = context;
    }

    protected abstract SparkContentFile<F> newFileWrapper();

    protected abstract RollingManifestWriter<F> newManifestWriter();

    public Dataset<ManifestFile> apply(Dataset<Row> input) {
      return input.mapPartitions(this, MANIFEST_ENCODER);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<ManifestFile> call(Iterator<Row> rows) throws Exception {
      SparkContentFile<F> fileWrapper = newFileWrapper();
      RollingManifestWriter<F> writer = newManifestWriter();

      try {
        while (rows.hasNext()) {
          Row row = rows.next();
          long snapshotId = row.getLong(1);
          long sequenceNumber = row.getLong(2);
          Long fileSequenceNumber = row.isNullAt(3) ? null : row.getLong(3);
          Row fileRow = row.getStruct(4);
          boolean repair = row.getBoolean(5);

          F file = fileWrapper.wrap(fileRow);
          if (repair) {
            file = (F) repairStats(file);
          }

          writer.existing(file, snapshotId, sequenceNumber, fileSequenceNumber);
        }
      } finally {
        writer.close();
      }

      return writer.toManifestFiles().iterator();
    }

    /** Rebuilds the file with the statistics read from the file itself. */
    private ContentFile<?> repairStats(ContentFile<?> file) {
      InputFile input = context.io().newInputFile(file.location());
      long fileSizeInBytes = input.getLength();
      Metrics metrics =
          RepairMetrics.readMetrics(
              input, file, context.metricsConfig(file), context.nameMapping());
      if (!context.repairColumnMetrics()) {
        // only the record count and file size are being repaired, so keep the stored column stats
        metrics = RepairMetrics.recordCountOnly(file, metrics);
      }

      return RepairMetrics.withStats(file, context.spec(file.specId()), metrics, fileSizeInBytes);
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

    RollingManifestWriter<DataFile> newRollingManifestWriter() {
      return new RollingManifestWriter<>(this::newManifestWriter, maxManifestSizeBytes);
    }

    private ManifestWriter<DataFile> newManifestWriter() {
      return ManifestFiles.write(formatVersion, spec(), newOutputFile(), null);
    }

    RollingManifestWriter<DeleteFile> newRollingDeleteManifestWriter() {
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
      String fileName = FileFormat.AVRO.addExtension(NEW_MANIFEST_PREFIX + UUID.randomUUID());
      Path filePath = new Path(outputLocation, fileName);
      return filePath.toString();
    }

    private Table table() {
      return tableBroadcast.value();
    }
  }
}
