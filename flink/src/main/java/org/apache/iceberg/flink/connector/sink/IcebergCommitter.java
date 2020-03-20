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

package org.apache.iceberg.flink.connector.sink;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.flink.connector.model.CommitMetadata;
import org.apache.iceberg.flink.connector.model.CommitMetadataUtil;
import org.apache.iceberg.flink.connector.model.FlinkManifestFile;
import org.apache.iceberg.flink.connector.model.FlinkManifestFileUtil;
import org.apache.iceberg.flink.connector.model.GenericFlinkManifestFile;
import org.apache.iceberg.flink.connector.model.ManifestFileState;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This operator commit data files to Iceberg table.
 * <p>
 * This operator should always run with parallelism of 1.
 * Because Iceberg lib perform optimistic concurrency control,
 * this can help reduce contention and retries
 * when committing files to Iceberg table.
 * <p>
 * Here are some known contentions
 * 1) Flink jobs running in multiple regions,
 * since Iceberg metadata service and commit only happens in us-east-1.
 * 2) auto tuning and auto lift services may update Iceberg table infrequently.
 */
@SuppressWarnings("checkstyle:HiddenField")
public class IcebergCommitter extends RichSinkFunction<FlinkDataFile>
    implements CheckpointedFunction, CheckpointListener {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitter.class);

  private static final String COMMIT_REGION_KEY = "flink.commit.region";
  private static final String COMMIT_MANIFEST_HASHES_KEY = "flink.commit.manifest.hashes";
  private static final String VTTS_WATERMARK_PROP_KEY_PREFIX = "flink.watermark.";

  private final String metacatHost;
  //private final String jobName;
  //private final String catalog;
  private final String database;
  private final String tableName;
  private final String region;
  private final boolean vttsWatermarkEnabled;
  private final String vttsWatermarkPropKey;
  private final long snapshotRetentionHours;
  private final boolean commitRestoredManifestFiles;
  private final String icebergManifestFileDir;
  private final PartitionSpec spec;
  private final HadoopFileIO hadoopFileIO;
  private final String flinkJobId;

  private transient Registry registry;
  private transient Table table;
  private transient List<FlinkDataFile> pendingDataFiles;
  private transient List<FlinkManifestFile> flinkManifestFiles;
  private transient ListState<ManifestFileState> manifestFilesState;
  private transient CommitMetadata metadata;
  private transient ListState<CommitMetadata> metadataState;
  private transient IcebergCommitterMetrics metrics;

  public IcebergCommitter(Configuration config) {
    // current Iceberg sink implementation can't work with concurrent checkpoints.
    // We disable concurrent checkpoints by default as min pause is set to 60s by default.
    // Add an assertion to fail explicit in case job enables concurrent checkpoints.
    CheckpointConfig checkpointConfig = StreamExecutionEnvironment.getExecutionEnvironment().getCheckpointConfig();
    if (checkpointConfig.getMaxConcurrentCheckpoints() > 1) {
      throw new IllegalArgumentException("Iceberg sink doesn't support concurrent checkpoints");
    }

    metacatHost = config.getString(IcebergConnectorConstant.METACAT_HOST,
                                   IcebergConnectorConstant.DEFAULT_METACAT_HOST);
    //jobName = config.getString(System.getenv("JOB_CLUSTER_NAME"), "");
    //catalog = config.getString(IcebergConnectorConstant.CATALOG, "");
    database = config.getString(IcebergConnectorConstant.DATABASE, "");
    tableName = config.getString(IcebergConnectorConstant.TABLE, "");

    String region = System.getenv("EC2_REGION");
    if (Strings.isNullOrEmpty(region)) {
      region = "us-east-1";
      LOG.info("Iceberg committer {}.{} default region to us-east-1", database, tableName);
    }
    this.region = region;

    vttsWatermarkEnabled = !Strings.isNullOrEmpty(
        config.getString(IcebergConnectorConstant.VTTS_WATERMARK_TIMESTAMP_FIELD, ""));
    vttsWatermarkPropKey = VTTS_WATERMARK_PROP_KEY_PREFIX + region;
    snapshotRetentionHours = config.getLong(IcebergConnectorConstant.SNAPSHOT_RETENTION_HOURS,
        IcebergConnectorConstant.DEFAULT_SNAPSHOT_RETENTION_HOURS);
    commitRestoredManifestFiles = config.getBoolean(IcebergConnectorConstant.COMMIT_RESTORED_MANIFEST_FILES,
        IcebergConnectorConstant.DEFAULT_COMMIT_RESTORED_MANIFEST_FILES);
    icebergManifestFileDir = getIcebergManifestFileDir(config);
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    hadoopConfig.set(IcebergConnectorConstant.METACAT_HOST_HADOOP_CONF_KEY, metacatHost);
    // Avoid Netflix code just to make it compile
    //final MetacatIcebergCatalog icebergCatalog
    // = new MetacatIcebergCatalog(hadoopConfig, jobName, IcebergConnectorConstant.ICEBERG_APP_TYPE);
//    final BaseMetastoreCatalog icebergCatalog = null;
//    final TableIdentifier tableIdentifier = TableIdentifier.of(catalog, database, tableName);
//    final Table table = icebergCatalog.loadTable(tableIdentifier);

    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    hadoopConf.set(IcebergConnectorConstant.METACAT_HOST_HADOOP_CONF_KEY, metacatHost);
    hadoopConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
                   config.getString(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, ""));

    Catalog icebergCatalog = HiveCatalogs.loadCatalog(hadoopConf);
    table = icebergCatalog.loadTable(TableIdentifier.of(database, tableName));

    spec = table.spec();
    hadoopFileIO = new HadoopFileIO(hadoopConfig);
    final JobExecutionResult jobExecutionResult
        = ExecutionEnvironment.getExecutionEnvironment().getLastJobExecutionResult();
    if (jobExecutionResult != null) {
      flinkJobId = jobExecutionResult.getJobID().toString();
      LOG.info("Get Flink job ID from execution environment: {}", flinkJobId);
    } else {
      flinkJobId = new JobID().toString();
      LOG.info("Execution environment doesn't have executed job. Generate a random job ID : {}", flinkJobId);
    }
    LOG.info("Iceberg committer {}.{} created with sink config", database, tableName);
    LOG.info("Iceberg committer {}.{} loaded table partition spec: {}", database, tableName, spec);
  }

  @VisibleForTesting
  IcebergCommitter(Configuration icebergSinkConfig, Registry registry) {
    this(icebergSinkConfig);
    this.registry = registry;
  }

  @VisibleForTesting
  List<FlinkDataFile> getPendingDataFiles() {
    return pendingDataFiles;
  }

  @VisibleForTesting
  List<FlinkManifestFile> getFlinkManifestFiles() {
    return flinkManifestFiles;
  }

  @VisibleForTesting
  CommitMetadata getMetadata() {
    return metadata;
  }

  private String getIcebergManifestFileDir(Configuration config) {
    final String checkpointDir = config.getString(
        CheckpointingOptions.CHECKPOINTS_DIRECTORY, null);
    if (null == checkpointDir) {
      throw new IllegalArgumentException("checkpoint dir is null");
    }
//    final URI uri = URI.create(checkpointDir);
//    final String scheme = uri.getScheme().toLowerCase();
//    AmazonS3URI s3URI = new AmazonS3URI(checkpointDir);
//    return String.format("s3://%s/iceberg/manifest/", s3URI.getBucket());
    return String.format("%s/iceberg/manifest/", checkpointDir);
  }

  @Override
  public void close() throws Exception {
    if (null != metrics) {
      metrics.stop();
      LOG.info("Stopped committer metrics");
    }

    super.close();
  }

  void init(Registry registry) {
    this.registry = registry;
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    hadoopConfig.set(IcebergConnectorConstant.METACAT_HOST_HADOOP_CONF_KEY, metacatHost);
    // Avoid Netflix code just to make it compile
    //final MetacatIcebergCatalog icebergCatalog
    // = new MetacatIcebergCatalog(hadoopConfig, jobName, IcebergConnectorConstant.ICEBERG_APP_TYPE);
//    final BaseMetastoreCatalog icebergCatalog = null;
//    final TableIdentifier tableIdentifier = TableIdentifier.of(catalog, database, tableName);
//    table = icebergCatalog.loadTable(tableIdentifier);

    Catalog icebergCatalog = HiveCatalogs.loadCatalog(hadoopConfig);
    table = icebergCatalog.loadTable(TableIdentifier.of(database, tableName));
//    if (table == null) {
//      LOG.error("table loaded by {}.{} is null in init()", database, tableName);
//    } else {
//      LOG.info("table = [{}] in init()", table.location());
//    }

    pendingDataFiles = new ArrayList<>();
    flinkManifestFiles = new ArrayList<>();
    metadata = CommitMetadata.newBuilder()
        .setLastCheckpointId(0)
        .setLastCheckpointTimestamp(0)
        .setLastCommitTimestamp(System.currentTimeMillis())
        .build();

    metrics = new IcebergCommitterMetrics(registry, database, tableName);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    init(new DefaultRegistry());

    Preconditions.checkState(manifestFilesState == null,
        "checkpointedFilesState has already been initialized.");
    Preconditions.checkState(metadataState == null,
        "metadataState has already been initialized.");
    manifestFilesState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
        "iceberg-committer-manifest-files-state", ManifestFileState.class));
    metadataState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
        "iceberg-committer-metadata-state", CommitMetadata.class));

    if (context.isRestored()) {
      final Iterable<CommitMetadata> restoredMetadata = metadataState.get();
      if (null != restoredMetadata) {
        LOG.info("Iceberg committer {}.{} restoring metadata", database, tableName);
        List<CommitMetadata> metadataList = new ArrayList<>();
        for (CommitMetadata entry : restoredMetadata) {
          metadataList.add(entry);
        }
        Preconditions.checkState(1 == metadataList.size(),
            "metadata list size should be 1. got " + metadataList.size());
        metadata = metadataList.get(0);
        LOG.info("Iceberg committer {}.{} restored metadata: {}",
            database, tableName, CommitMetadataUtil.getInstance().encodeAsJson(metadata));
      } else {
        LOG.info("Iceberg committer {}.{} has nothing to restore for metadata", database, tableName);
      }

      Iterable<ManifestFileState> restoredManifestFileStates = manifestFilesState.get();
      if (null != restoredManifestFileStates) {
        LOG.info("Iceberg committer {}.{} restoring manifest files",
            database, tableName);
        for (ManifestFileState manifestFileState : restoredManifestFileStates) {
          flinkManifestFiles.add(GenericFlinkManifestFile.fromState(manifestFileState));
        }
        LOG.info("Iceberg committer {}.{} restored {} manifest files: {}",
            database, tableName, flinkManifestFiles.size(), flinkManifestFiles);
        final long now = System.currentTimeMillis();
        if (now - metadata.getLastCheckpointTimestamp() > TimeUnit.HOURS.toMillis(snapshotRetentionHours)) {
          flinkManifestFiles.clear();
          LOG.info("Iceberg committer {}.{} cleared restored manifest files as checkpoint timestamp is too old: " +
                  "checkpointTimestamp = {}, now = {}, snapshotRetentionHours = {}",
              database, tableName, metadata.getLastCheckpointTimestamp(), now, snapshotRetentionHours);
        } else {
          flinkManifestFiles = removeCommittedManifests(flinkManifestFiles);
          if (flinkManifestFiles.isEmpty()) {
            LOG.info("Iceberg committer {}.{} has zero uncommitted manifest files from restored state",
                database, tableName);
          } else {
            if (commitRestoredManifestFiles) {
              commitRestoredManifestFiles();
            } else {
              LOG.info("skip commit of restored manifest files");
            }
          }
        }
      } else {
        LOG.info("Iceberg committer {}.{} has nothing to restore for manifest files", database, tableName);
      }
    }
  }

  @VisibleForTesting
  void commitRestoredManifestFiles() throws Exception {
    LOG.info("Iceberg committer {}.{} committing last uncompleted transaction upon recovery: " +
            "metadata = {}, flink manifest files ({}) = {}", database, tableName,
        CommitMetadataUtil.getInstance().encodeAsJson(metadata),
        flinkManifestFiles.size(), flinkManifestFiles);
    commit();
    LOG.info("Iceberg committer {}.{} committed last uncompleted transaction upon recovery: " +
            "metadata = {}, flink manifest files ({}) = {}", database, tableName,
        CommitMetadataUtil.getInstance().encodeAsJson(metadata),
        flinkManifestFiles.size(), flinkManifestFiles);
    postCommitSuccess();
  }

  private List<FlinkManifestFile> removeCommittedManifests(List<FlinkManifestFile> flinkManifestFiles) {
    int snapshotCount = 0;
    String result = "succeeded";
    final long start = registry.clock().monotonicTime();
    try {
      final Set<String> manifestHashes = flinkManifestFiles.stream()
          .map(f -> f.hash())
          .collect(Collectors.toSet());
      final Set<String> committedHashes = new HashSet<>(flinkManifestFiles.size());
      final Iterable<Snapshot> snapshots = table.snapshots();
      for (Snapshot snapshot : snapshots) {
        ++snapshotCount;
        final Map<String, String> summary = snapshot.summary();
        final List<String> hashes = FlinkManifestFileUtil.hashesStringToList(summary.get(COMMIT_MANIFEST_HASHES_KEY));
        for (String hash : hashes) {
          if (manifestHashes.contains(hash)) {
            committedHashes.add(hash);
          }
        }
      }
      final List<FlinkManifestFile> uncommittedManifestFiles = flinkManifestFiles.stream()
          .filter(f -> !committedHashes.contains(f.hash()))
          .collect(Collectors.toList());
      metrics.incrementCheckTransactionCompletedSuccess();
      return uncommittedManifestFiles;
    } catch (Throwable t) {
      result = "failed";
      metrics.incrementCheckTransactionCompletedFailure(t);
      //LOG.error(String.format("Iceberg committer %s.%s failed to check transaction completed", database, tableName),
      //          t);
      LOG.error("Iceberg committer {}.{} failed to check transaction completed. Throwable = {}",
                database, tableName, t);
      throw t;
    } finally {
      final long duration = registry.clock().monotonicTime() - start;
      metrics.recordCheckTransactionCommittedLatency(duration, TimeUnit.NANOSECONDS);
      LOG.info("Iceberg committer {}.{} {} to check transaction completed" +
               " after iterating {} snapshots and {} milli-seconds",
               database, tableName, result, snapshotCount, TimeUnit.NANOSECONDS.toMillis(duration));
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    LOG.info("Iceberg committer {}.{} snapshot state: checkpointId = {}, triggerTime = {}",
        database, tableName, context.getCheckpointId(), context.getCheckpointTimestamp());
    Preconditions.checkState(null != manifestFilesState,
        "manifest files state has not been properly initialized.");
    Preconditions.checkState(null != metadataState,
        "metadata state has not been properly initialized.");

    // set transaction to null to indicate a start of a new checkpoint/commit/transaction
    synchronized (this) {
      snapshot(context, pendingDataFiles);
      checkpointState(flinkManifestFiles, metadata);
      postSnapshotSuccess();
    }
  }

  @VisibleForTesting
  void snapshot(FunctionSnapshotContext context, List<FlinkDataFile> pendingDataFiles) throws Exception {
    FlinkManifestFile flinkManifestFile = null;
    if (!pendingDataFiles.isEmpty()) {
      flinkManifestFile = createManifestFile(context, pendingDataFiles);
      flinkManifestFiles.add(flinkManifestFile);
    }
    metadata = updateMetadata(metadata, context, flinkManifestFile);
  }

  private FlinkManifestFile createManifestFile(
      FunctionSnapshotContext context, List<FlinkDataFile> pendingDataFiles) throws Exception {
    LOG.info("Iceberg committer {}.{} checkpointing {} pending data files}",
        database, tableName, pendingDataFiles.size());
    String result = "succeeded";
    final long start = registry.clock().monotonicTime();
    try {
      final String manifestFileName = Joiner.on("_")
          .join(flinkJobId, context.getCheckpointId(), context.getCheckpointTimestamp());
      // Iceberg requires file format suffix right now
      final String manifestFileNameWithSuffix = manifestFileName + ".avro";
      OutputFile outputFile = hadoopFileIO.newOutputFile(icebergManifestFileDir + manifestFileNameWithSuffix);
      ManifestWriter manifestWriter = ManifestWriter.write(spec, outputFile);

      // stats
      long recordCount = 0;
      long byteCount = 0;
      long lowWatermark = Long.MAX_VALUE;
      long highWatermark = Long.MIN_VALUE;
      for (FlinkDataFile flinkDataFile : pendingDataFiles) {
        DataFile dataFile = flinkDataFile.getIcebergDataFile();
        manifestWriter.add(dataFile);
        // update stas
        recordCount += dataFile.recordCount();
        byteCount += dataFile.fileSizeInBytes();
        if (flinkDataFile.getLowWatermark() < lowWatermark) {
          lowWatermark = flinkDataFile.getLowWatermark();
        }
        if (flinkDataFile.getHighWatermark() > highWatermark) {
          highWatermark = flinkDataFile.getHighWatermark();
        }
        metrics.recordFileSize(dataFile.fileSizeInBytes());
      }
      manifestWriter.close();
      ManifestFile manifestFile = manifestWriter.toManifestFile();

      FlinkManifestFile flinkManifestFile = GenericFlinkManifestFile.builder()
          .setPath(manifestFile.path())
          .setLength(manifestFile.length())
          .setSpecId(manifestFile.partitionSpecId())
          .setCheckpointId(context.getCheckpointId())
          .setCheckpointTimestamp(context.getCheckpointTimestamp())
          .setDataFileCount(pendingDataFiles.size())
          .setRecordCount(recordCount)
          .setByteCount(byteCount)
          .setLowWatermark(lowWatermark)
          .setHighWatermark(highWatermark)
          .build();

      // don't want to log a giant list at one line.
      // split the complete list into smaller chunks with 50 files.
      // each S3 file location is about 200-250 bytes.
      // that should give us log line of ~10 KB.
      final AtomicInteger counter = new AtomicInteger(0);
      Collection<List<String>> listOfFileList = pendingDataFiles.stream()
          .map(flinkDataFile -> flinkDataFile.toCompactDump())
          .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / 50))
          .values();
      for (List<String> fileList : listOfFileList) {
        LOG.info("Iceberg committer {}.{} created manifest file {} for {}/{} data files: {}",
            database, tableName, manifestFile.path(), fileList.size(), pendingDataFiles.size(), fileList);
      }
      metrics.incrementCreateManifestFileSuccess();
      return flinkManifestFile;
    } catch (Throwable t) {
      result = "failed";
      metrics.incrementCreateManifestFileFailure(t);
      //LOG.error(String.format("Iceberg committer %s.%s failed to create manifest file for %d pending data files",
      //          database, tableName, pendingDataFiles.size()), t);
      LOG.error("Iceberg committer {}.{} failed to create manifest file for {} pending data files. Throwable={}",
          database, tableName, pendingDataFiles.size(), t);
      throw t;
    } finally {
      final long duration = registry.clock().monotonicTime() - start;
      metrics.recordCreateManifestFileLatency(duration, TimeUnit.NANOSECONDS);
      LOG.info("Iceberg committer {}.{} {} to create manifest file with {} data files after {} milli-seconds",
          database, tableName, result, pendingDataFiles.size(), TimeUnit.NANOSECONDS.toMillis(duration));
    }
  }

  private CommitMetadata updateMetadata(
      CommitMetadata oldMetadata,
      FunctionSnapshotContext context,
      @Nullable FlinkManifestFile flinkManifestFile) {
    LOG.info("Iceberg committer {}.{} updating metadata {} with manifest file {}",
        database, tableName, CommitMetadataUtil.getInstance().encodeAsJson(oldMetadata), flinkManifestFile);
    CommitMetadata.Builder metadataBuilder = CommitMetadata.newBuilder(oldMetadata)
        .setLastCheckpointId(context.getCheckpointId())
        .setLastCheckpointTimestamp(context.getCheckpointTimestamp());
    if (vttsWatermarkEnabled) {
      Long vttsWatermark = oldMetadata.getVttsWatermark();
      if (null == flinkManifestFile) {
        // DPS-412: when there is no data to be committed
        // use elapsed wall clock time to move the VTTS watermark forward.
        if (null != vttsWatermark) {
          final long elapsedTimeMs = System.currentTimeMillis() - oldMetadata.getLastCommitTimestamp();
          vttsWatermark += elapsedTimeMs;
        } else {
          vttsWatermark = System.currentTimeMillis();
        }
      } else {
        // use lowWatermark.
        if (null == flinkManifestFile.lowWatermark()) {
          throw new IllegalArgumentException("VTTS is enabled but lowWatermark is null");
        }
        // in case one container/slot is lagging behind,
        // we want to move watermark forward based on the slowest.
        final long newWatermark = flinkManifestFile.lowWatermark();
        // make sure VTTS watermark doesn't go back in time
        if (null == vttsWatermark || newWatermark > vttsWatermark) {
          vttsWatermark = newWatermark;
        }
      }
      metadataBuilder.setVttsWatermark(vttsWatermark);
    }
    CommitMetadata metadata = metadataBuilder.build();
    LOG.info("Iceberg committer {}.{} updated metadata {} with manifest file {}",
        database, tableName, CommitMetadataUtil.getInstance().encodeAsJson(metadata), flinkManifestFile);
    return metadata;
  }

  private void checkpointState(List<FlinkManifestFile> flinkManifestFiles, CommitMetadata metadata) throws Exception {
    LOG.info("Iceberg committer {}.{} checkpointing state", database, tableName);
    List<ManifestFileState> manifestFileStates = flinkManifestFiles.stream()
        .map(f -> f.toState())
        .collect(Collectors.toList());
    manifestFilesState.clear();
    manifestFilesState.addAll(manifestFileStates);
    metadataState.clear();
    metadataState.add(metadata);
    LOG.info("Iceberg committer {}.{} checkpointed state: metadata = {}, flinkManifestFiles({}) = {}",
        database, tableName, CommitMetadataUtil.getInstance().encodeAsJson(metadata),
        flinkManifestFiles.size(), flinkManifestFiles);
  }

  private void postSnapshotSuccess() {
    pendingDataFiles.clear();
    metrics.setUncommittedManifestFiles(flinkManifestFiles.size());
    metrics.setUncommittedFiles(flinkManifestFiles.stream().map(FlinkManifestFile::dataFileCount)
        .collect(Collectors.summingLong(l -> l)));
    metrics.setUncommittedRecords(flinkManifestFiles.stream().map(FlinkManifestFile::recordCount)
        .collect(Collectors.summingLong(l -> l)));
    metrics.setUncommittedBytes(flinkManifestFiles.stream().map(FlinkManifestFile::byteCount)
        .collect(Collectors.summingLong(l -> l)));
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    LOG.info("Iceberg committer {}.{} checkpoint {} completed", database, tableName, checkpointId);
    synchronized (this) {
      if (checkpointId == metadata.getLastCheckpointId()) {
        try {
          commit();
          postCommitSuccess();
        } catch (Exception t) {
          // swallow the exception to avoid job restart in case of commit failure
          //LOG.error(String.format("Iceberg committer %s.%s failed to do post checkpoint commit", database, tableName),
          //          t);
          LOG.error("Iceberg committer {}.{} failed to do post checkpoint commit. Throwable = ",
              database, tableName, t);
        }
      } else {
        // TODO: it would be nice to fix this and allow concurrent checkpoint
        LOG.info("Iceberg committer {}.{} skip committing transaction: " +
                "notify complete checkpoint id = {}, last manifest checkpoint id = {}",
            database, tableName, checkpointId, metadata.getLastCheckpointId());
        metrics.incrementSkipCommitCheckpointIdMismatch();
      }
    }
  }

  @VisibleForTesting
  void commit() {
    if (!flinkManifestFiles.isEmpty() || vttsWatermarkEnabled) {
      final long start = registry.clock().monotonicTime();
      try {
        // prepare and commit transactions in two separate methods
        // so that we can measure latency separately.
        Transaction transaction = prepareTransaction(flinkManifestFiles, metadata);
        commitTransaction(transaction);
        metrics.incrementCommitSuccess();
        final long duration = registry.clock().monotonicTime() - start;
        LOG.info("Iceberg committer {}.{} succeeded to commit {} manifest files after {} milli-seconds",
            database, tableName, flinkManifestFiles.size(), TimeUnit.NANOSECONDS.toMillis(duration));
      } catch (Throwable t) {
        metrics.incrementCommitFailure(t);
        final long duration = registry.clock().monotonicTime() - start;
        //LOG.error(String.format("Iceberg committer %s.%s failed to commit %d manifest files after %d milli-seconds",
        //database, tableName, flinkManifestFiles.size(), TimeUnit.NANOSECONDS.toMillis(duration)), t);
        LOG.error("Iceberg committer {}.{} failed to commit {} manifest files after {} milli-seconds." +
                  " Throwable = ",
                  database, tableName, flinkManifestFiles.size(), TimeUnit.NANOSECONDS.toMillis(duration), t);
        throw t;
      }
    } else {
      LOG.info("Iceberg committer {}.{} skip commit, " +
               "as there are no uncommitted data files and VTTS watermark is disabled",
               database, tableName);
    }
  }

  private Transaction prepareTransaction(List<FlinkManifestFile> flinkManifestFiles, CommitMetadata metadata) {
    LOG.info("Iceberg committer {}.{} start to prepare transaction: {}",
        database, tableName, CommitMetadataUtil.getInstance().encodeAsJson(metadata));
    final long start = registry.clock().monotonicTime();
    try {
//      if (table == null) {
//        LOG.error("Iceberg committer {}.{} table = null in prepareTransaction()", database, tableName);
//      }
      Transaction transaction = table.newTransaction();
      if (!flinkManifestFiles.isEmpty()) {
        List<String> hashes = new ArrayList<>(flinkManifestFiles.size());
        AppendFiles appendFiles = transaction.newAppend();
        for (FlinkManifestFile flinkManifestFile : flinkManifestFiles) {
          appendFiles.appendManifest(flinkManifestFile);
          hashes.add(flinkManifestFile.hash());
        }
        appendFiles.set(COMMIT_REGION_KEY, region);
        appendFiles.set(COMMIT_MANIFEST_HASHES_KEY, FlinkManifestFileUtil.hashesListToString(hashes));
        appendFiles.commit();
        LOG.info("Iceberg committer {}.{} appended {} manifest files to transaction",
            database, tableName, flinkManifestFiles.size());
      }
      if (vttsWatermarkEnabled) {
        UpdateProperties updateProperties = transaction.updateProperties();
        updateProperties.set(vttsWatermarkPropKey, Long.toString(metadata.getVttsWatermark()));
        updateProperties.commit();
        LOG.info("Iceberg committer {}.{} set VTTS watermark to {}",
            database, tableName, metadata.getVttsWatermark());
      }
      return transaction;
    } finally {
      final long duration = registry.clock().monotonicTime() - start;
      metrics.recordPrepareTransactionLatency(duration, TimeUnit.NANOSECONDS);
    }
  }

  private void commitTransaction(Transaction transaction) {
    LOG.info("Iceberg committer {}.{} start to commit transaction: {}",
        database, tableName, CommitMetadataUtil.getInstance().encodeAsJson(metadata));
    final long start = registry.clock().monotonicTime();
    try {
      transaction.commitTransaction();
    } finally {
      final long duration = registry.clock().monotonicTime() - start;
      metrics.recordCommitLatency(duration, TimeUnit.NANOSECONDS);
    }
  }

  private void postCommitSuccess() {
    LOG.info("Iceberg committer {}.{} update metrics and metadata post commit success", database, tableName);
    metrics.incrementCommittedManifestFiles(flinkManifestFiles.size());
    metrics.incrementCommittedFiles(FlinkManifestFileUtil.getDataFileCount(flinkManifestFiles));
    metrics.incrementCommittedRecords(FlinkManifestFileUtil.getRecordCount(flinkManifestFiles));
    metrics.incrementCommittededBytes(FlinkManifestFileUtil.getByteCount(flinkManifestFiles));
    final Long lowWatermark = FlinkManifestFileUtil.getLowWatermark(flinkManifestFiles);
    if (null != lowWatermark) {
      metrics.setLowWatermark(lowWatermark);
    }
    final Long highWatermark = FlinkManifestFileUtil.getHighWatermark(flinkManifestFiles);
    if (null != highWatermark) {
      metrics.setHighWatermark(highWatermark);
    }
    if (null != metadata.getVttsWatermark()) {
      metrics.setVttsWatermark(metadata.getVttsWatermark());
    }
    final long lastCommitTimestamp = System.currentTimeMillis();
    metadata.setLastCommitTimestamp(lastCommitTimestamp);
    metrics.setLastCommitTimestamp(lastCommitTimestamp);
    metrics.setUncommittedManifestFiles(0L);
    metrics.setUncommittedFiles(0L);
    metrics.setUncommittedRecords(0L);
    metrics.setUncommittedBytes(0L);
    flinkManifestFiles.clear();
  }

  @Override
  public void invoke(FlinkDataFile value, Context context) throws Exception {
    pendingDataFiles.add(value);
    metrics.incrementReceivedFiles();
    metrics.incrementReceivedRecords(value.getIcebergDataFile().recordCount());
    metrics.incrementReceivedBytes(value.getIcebergDataFile().fileSizeInBytes());
    LOG.debug("Iceberg committer {}.{} has total pending files {} after receiving new data file: {}",
        database, tableName, pendingDataFiles.size(), value.toCompactDump());
  }

}
