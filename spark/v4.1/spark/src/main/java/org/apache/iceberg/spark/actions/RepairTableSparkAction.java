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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RollingManifestWriter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ImmutableRepairTable;
import org.apache.iceberg.actions.RepairTable;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkContentFile;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkDeleteFile;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
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
 * An action that repairs incorrect statistics in the manifests of a table, rewriting only the
 * manifests that contain an incorrect entry.
 *
 * <p>Deletion vectors are not repaired. NaN counts and floating-point and geometry bounds are kept
 * as stored, because file footers cannot reconstruct them reliably.
 */
public class RepairTableSparkAction extends BaseSnapshotUpdateSparkAction<RepairTableSparkAction>
    implements RepairTable {

  public static final String USE_CACHING = USE_CACHING_OPTION;
  public static final boolean USE_CACHING_DEFAULT = USE_CACHING_BY_DEFAULT;

  /**
   * Whether to also repair column statistics, not only record counts and file sizes. Disabled by
   * default: the metrics config a file was written under is not recorded, so correct column
   * statistics can differ from the recomputed ones after the table's config changes.
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

  // positions of the columns that markEntriesToRepair selects, which WriteManifests reads
  private static final int MANIFEST_INDEX = 0;
  private static final int SNAPSHOT_ID_INDEX = 1;
  private static final int FILE_PATH_INDEX = 2;
  private static final int REPAIR_INDEX = 3;

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

    this.outputLocation = ManifestWriterFactory.defaultOutputLocation(table);
    TableOperations ops = ((HasTableOperations) table).operations();
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
    if (!repairFileMetrics) {
      return EMPTY_RESULT;
    }

    String desc = String.format("Repairing manifests in %s (dryRun=%s)", table.name(), dryRun);
    JobGroupInfo info = newJobGroupInfo("REPAIR-TABLE", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private RepairTable.Result doExecute() {
    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot == null) {
      return EMPTY_RESULT;
    }

    Broadcast<Table> tableBroadcast =
        sparkContext().broadcast(SerializableTableWithSize.copyOf(table));
    List<ManifestFile> repairedManifests = Lists.newArrayList();
    List<ManifestFile> newManifests = Lists.newArrayList();
    long repairedCount = 0L;

    try {
      for (ManifestContent content : ManifestContent.values()) {
        RepairContext context = newRepairContext(tableBroadcast, content);
        // rewriting a manifest with a spec other than its own would change its partition data
        Map<Integer, List<ManifestFile>> manifestsBySpecId =
            loadManifests(content, currentSnapshot).stream()
                .collect(Collectors.groupingBy(ManifestFile::partitionSpecId));
        for (Map.Entry<Integer, List<ManifestFile>> group : manifestsBySpecId.entrySet()) {
          RepairedManifests repaired = repairManifests(context, group.getKey(), group.getValue());
          repairedManifests.addAll(repaired.repairedManifests());
          newManifests.addAll(repaired.newManifests());
          repairedCount += repaired.repairedCount();
        }
      }
    } catch (Exception e) {
      // none of the manifests written so far are committed, so delete them rather than orphan them
      CatalogUtil.deleteFiles(
          table.io(), Iterables.transform(newManifests, ManifestFile::path), "manifest");
      throw e;
    }

    if (repairedManifests.isEmpty()) {
      return EMPTY_RESULT;
    }

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

  private RepairedManifests repairManifests(
      RepairContext context, int specId, List<ManifestFile> manifests) {
    Dataset<Row> entryDF = buildManifestEntryDF(manifests);

    return withReusableDS(
        entryDF,
        df -> {
          // cached because it is read several times and recomputing it would read every file again
          Dataset<Row> verdicts =
              df.mapPartitions(
                      new CheckStats(context), Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                  .toDF("manifest", "path")
                  .cache();

          try {
            Set<String> manifestsToRewrite =
                Sets.newHashSet(
                    verdicts.select("manifest").distinct().as(Encoders.STRING()).collectAsList());

            if (manifestsToRewrite.isEmpty()) {
              return RepairedManifests.empty();
            }

            long repairedCount = verdicts.count();
            List<ManifestFile> rewritten =
                manifests.stream()
                    .filter(manifest -> manifestsToRewrite.contains(manifest.path()))
                    .collect(Collectors.toList());

            if (dryRun) {
              return RepairedManifests.of(rewritten, ImmutableList.of(), repairedCount);
            }

            // join rather than collect the paths, which can cover every file of the table
            Dataset<Row> entriesToRewrite =
                df.filter(df.col("manifest").isin(manifestsToRewrite.toArray()));
            Dataset<Row> markedEntries = markEntriesToRepair(entriesToRewrite, verdicts);
            List<ManifestFile> written = writeManifests(context, specId, markedEntries, rewritten);

            return RepairedManifests.of(rewritten, written, repairedCount);
          } finally {
            verdicts.unpersist(false);
          }
        });
  }

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
        // WriteManifests reads these columns by position, see MANIFEST_INDEX
        .select("manifest", "snapshot_id", "data_file.file_path", "repair");
  }

  private Dataset<Row> buildManifestEntryDF(List<ManifestFile> manifests) {
    Dataset<Row> manifestDF =
        spark()
            .createDataset(Lists.transform(manifests, ManifestFile::path), Encoders.STRING())
            .toDF("manifest");

    Dataset<Row> entryDF =
        loadMetadataTable(table, ENTRIES)
            .filter("status < 2") // select only live entries
            .selectExpr("input_file_name() as manifest", "snapshot_id", "data_file");

    return entryDF.join(
        manifestDF, manifestDF.col("manifest").equalTo(entryDF.col("manifest")), "left_semi");
  }

  private List<ManifestFile> writeManifests(
      RepairContext context, int specId, Dataset<Row> entryDF, List<ManifestFile> manifests) {
    ManifestWriterFactory writers = manifestWriters(context.tableBroadcast(), specId);
    WriteManifests<?> writeFunc =
        context.content() == ManifestContent.DATA
            ? new WriteDataManifests(writers, manifests, context)
            : new WriteDeleteManifests(writers, manifests, context);

    // repartition by manifest so each manifest's entries stay together, keeping the table's layout
    return writeFunc
        .apply(entryDF.repartition(manifests.size(), entryDF.col("manifest")))
        .collectAsList();
  }

  private RepairContext newRepairContext(Broadcast<Table> tableBroadcast, ManifestContent content) {
    boolean repairColumnMetrics =
        PropertyUtil.propertyAsBoolean(
            options(), REPAIR_COLUMN_METRICS, REPAIR_COLUMN_METRICS_DEFAULT);
    return new RepairContext(tableBroadcast, content, repairColumnMetrics);
  }

  private List<ManifestFile> loadManifests(ManifestContent content, Snapshot snapshot) {
    return switch (content) {
      case DATA -> snapshot.dataManifests(table.io());
      case DELETES -> snapshot.deleteManifests(table.io());
    };
  }

  private void replaceManifests(
      List<ManifestFile> deletedManifests, List<ManifestFile> addedManifests) {
    try {
      RewriteManifests rewriteManifests = table.rewriteManifests();
      deletedManifests.forEach(rewriteManifests::deleteManifest);
      addedManifests.forEach(rewriteManifests::addManifest);
      commit(rewriteManifests);

      if (shouldStageManifests) {
        // delete new manifests as they were rewritten before the commit
        CatalogUtil.deleteFiles(
            table.io(), Iterables.transform(addedManifests, ManifestFile::path), "manifest");
      }
    } catch (CommitStateUnknownException e) {
      // don't clean up added manifest files, because they may have been successfully committed
      throw e;
    } catch (Exception e) {
      if (e instanceof CleanableFailure) {
        CatalogUtil.deleteFiles(
            table.io(), Iterables.transform(addedManifests, ManifestFile::path), "manifest");
      }

      throw e;
    }
  }

  private ManifestWriterFactory manifestWriters(Broadcast<Table> tableBroadcast, int specId) {
    return new ManifestWriterFactory(
        tableBroadcast,
        formatVersion,
        specId,
        outputLocation,
        NEW_MANIFEST_PREFIX,
        // allow the actual size of manifests to be 20% higher as the estimation is not precise
        (long) (1.2 * targetManifestSizeBytes));
  }

  /** The outcome of repairing one group of manifests that share a partition spec. */
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

  private static class RepairContext implements Serializable {
    private final Broadcast<Table> tableBroadcast;
    private final ManifestContent content;
    private final boolean repairColumnMetrics;

    private transient EncryptingFileIO lazyEncryptingIO = null;
    private transient Map<FileContent, MetricsConfig> lazyMetricsConfigs = null;
    private transient NameMapping lazyNameMapping = null;
    private transient boolean nameMappingResolved = false;

    RepairContext(
        Broadcast<Table> tableBroadcast, ManifestContent content, boolean repairColumnMetrics) {
      this.tableBroadcast = tableBroadcast;
      this.content = content;
      this.repairColumnMetrics = repairColumnMetrics;
    }

    Broadcast<Table> tableBroadcast() {
      return tableBroadcast;
    }

    ManifestContent content() {
      return content;
    }

    Table table() {
      return tableBroadcast.value();
    }

    FileIO io() {
      return table().io();
    }

    EncryptingFileIO encryptingIO() {
      if (lazyEncryptingIO == null) {
        this.lazyEncryptingIO = EncryptingFileIO.combine(table().io(), table().encryption());
      }

      return lazyEncryptingIO;
    }

    InputFile newInputFile(ContentFile<?> file, long fileSizeInBytes) {
      if (file.keyMetadata() != null) {
        return encryptingIO()
            .newDecryptingInputFile(file.location(), fileSizeInBytes, file.keyMetadata());
      }

      return encryptingIO().newInputFile(file.location(), fileSizeInBytes);
    }

    boolean repairColumnMetrics() {
      return repairColumnMetrics;
    }

    PartitionSpec spec(int id) {
      return table().specs().get(id);
    }

    // a delete manifest can hold both position and equality deletes, whose configs differ
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
      return content == ManifestContent.DATA
          ? new SparkDataFile(combinedFileType, sparkType)
          : new SparkDeleteFile(combinedFileType, sparkType);
    }
  }

  private static class CheckStats implements MapPartitionsFunction<Row, Tuple2<String, String>> {
    private final RepairContext context;

    CheckStats(RepairContext context) {
      this.context = context;
    }

    @Override
    public Iterator<Tuple2<String, String>> call(Iterator<Row> rows) {
      List<Tuple2<String, String>> verdicts = Lists.newArrayList();
      Types.StructType combinedFileType =
          DataFile.getType(Partitioning.partitionType(context.table()));
      SparkContentFile<?> fileWrapper = null;

      while (rows.hasNext()) {
        Row row = rows.next();
        String manifest = row.getString(0);
        Row fileRow = row.getStruct(2);
        if (fileWrapper == null) {
          fileWrapper = context.newFileWrapper(combinedFileType, (StructType) fileRow.schema());
        }

        ContentFile<?> file = (ContentFile<?>) fileWrapper.wrap(fileRow);

        if (!RepairMetrics.supportsMetrics(file)) {
          continue;
        }

        String location = file.location().toString();

        try {
          long fileSizeInBytes = context.io().newInputFile(location).getLength();
          InputFile input = context.newInputFile(file, fileSizeInBytes);
          Metrics expected =
              RepairMetrics.expectedMetrics(
                  input,
                  file,
                  context.metricsConfig(file),
                  context.nameMapping(),
                  context.table().schema());

          if (RepairMetrics.statsAreIncorrect(
              file, expected, fileSizeInBytes, context.repairColumnMetrics())) {
            verdicts.add(new Tuple2<>(manifest, location));
          }
        } catch (Exception e) {
          // A file we can't read keeps its stats as is, so skipping can only leave an entry
          // unrepaired, never make it worse.
          LOG.warn("Skipping the entry of {} as its statistics could not be read", location, e);
        }
      }

      return verdicts.iterator();
    }
  }

  private static class WriteDataManifests extends WriteManifests<DataFile> {
    WriteDataManifests(
        ManifestWriterFactory writers, List<ManifestFile> manifests, RepairContext context) {
      super(writers, manifests, context);
    }

    @Override
    protected ManifestReader<DataFile> readManifest(ManifestFile manifest, Table table) {
      return ManifestFiles.read(manifest, table.io(), table.specs());
    }

    @Override
    protected RollingManifestWriter<DataFile> newManifestWriter() {
      return writers().newRollingManifestWriter();
    }
  }

  private static class WriteDeleteManifests extends WriteManifests<DeleteFile> {
    WriteDeleteManifests(
        ManifestWriterFactory writers, List<ManifestFile> manifests, RepairContext context) {
      super(writers, manifests, context);
    }

    @Override
    protected ManifestReader<DeleteFile> readManifest(ManifestFile manifest, Table table) {
      return ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs());
    }

    @Override
    protected RollingManifestWriter<DeleteFile> newManifestWriter() {
      return writers().newRollingDeleteManifestWriter();
    }
  }

  // entries are written as existing with their original snapshot id and sequence numbers, so the
  // delete files that apply to them still do
  private abstract static class WriteManifests<F extends ContentFile<F>>
      implements MapPartitionsFunction<Row, ManifestFile> {

    private static final Encoder<ManifestFile> MANIFEST_ENCODER =
        Encoders.javaSerialization(ManifestFile.class);

    private final ManifestWriterFactory writers;
    private final List<ManifestFile> manifests;
    private final RepairContext context;

    WriteManifests(
        ManifestWriterFactory writers, List<ManifestFile> manifests, RepairContext context) {
      this.writers = writers;
      this.manifests = manifests;
      this.context = context;
    }

    protected abstract ManifestReader<F> readManifest(ManifestFile manifest, Table table);

    protected abstract RollingManifestWriter<F> newManifestWriter();

    public Dataset<ManifestFile> apply(Dataset<Row> input) {
      return input.mapPartitions(this, MANIFEST_ENCODER);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<ManifestFile> call(Iterator<Row> rows) throws Exception {
      Map<String, Map<String, Row>> entriesByManifest = Maps.newHashMap();
      rows.forEachRemaining(
          row ->
              entriesByManifest
                  .computeIfAbsent(row.getString(MANIFEST_INDEX), path -> Maps.newHashMap())
                  .put(row.getString(FILE_PATH_INDEX), row));
      RollingManifestWriter<F> writer = newManifestWriter();

      try (writer) {
        for (ManifestFile manifest : manifests) {
          Map<String, Row> entries = entriesByManifest.get(manifest.path());
          if (entries == null) {
            continue;
          }

          // read the original manifest, since the entries metadata table omits partition fields
          // whose source column was dropped
          try (ManifestReader<F> reader = readManifest(manifest, context.table())) {
            for (F file : reader) {
              Row row = entries.get(file.location());
              Preconditions.checkState(
                  row != null, "Cannot find entry for %s in %s", file.location(), manifest.path());
              F repaired = row.getBoolean(REPAIR_INDEX) ? (F) repairStats(file) : file;
              writer.existing(
                  repaired,
                  row.getLong(SNAPSHOT_ID_INDEX),
                  file.dataSequenceNumber(),
                  file.fileSequenceNumber());
            }
          }
        }
      } catch (Exception e) {
        // a failed task attempt never returns its manifests to the driver, so delete them here
        deleteManifests(writer, e);
        throw e;
      }

      return writer.toManifestFiles().iterator();
    }

    private void deleteManifests(RollingManifestWriter<F> writer, Exception failure) {
      try {
        Tasks.foreach(writer.toManifestFiles())
            .suppressFailureWhenFinished()
            .onFailure((manifest, exc) -> failure.addSuppressed(exc))
            .run(manifest -> context.io().deleteFile(manifest.path()));
      } catch (RuntimeException e) {
        // the writer did not close, so the manifests it wrote are unknown
        failure.addSuppressed(e);
      }
    }

    /** Rebuilds the file with its expected statistics. */
    private ContentFile<?> repairStats(ContentFile<?> file) {
      long fileSizeInBytes = context.io().newInputFile(file.location()).getLength();
      InputFile input = context.newInputFile(file, fileSizeInBytes);
      Metrics metrics =
          RepairMetrics.expectedMetrics(
              input,
              file,
              context.metricsConfig(file),
              context.nameMapping(),
              context.table().schema());
      if (!context.repairColumnMetrics()) {
        // only the record count and file size are being repaired, so keep the stored column stats
        metrics = RepairMetrics.recordCountOnly(file, metrics);
      }

      return RepairMetrics.withStats(file, context.spec(file.specId()), metrics, fileSizeInBytes);
    }

    protected ManifestWriterFactory writers() {
      return writers;
    }
  }
}
