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
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
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
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.io.FileIO;
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
  private static final String WATERMARK_PROP_KEY_PREFIX = "flink.watermark.";

  private Configuration config;
  private final String namespace;
  private final String tableName;

  private final String region;
  private final boolean watermarkEnabled;
  private final String watermarkPropKey;
  private final long snapshotRetentionHours;
  private final boolean commitRestoredManifestFiles;
  private final String icebergManifestFileDir;
  private final PartitionSpec spec;
  private final FileIO io;
  private final String flinkJobId;

  private transient Table table;
  private transient List<FlinkDataFile> pendingDataFiles;
  private transient List<FlinkManifestFile> flinkManifestFiles;
  private transient ListState<ManifestFileState> manifestFileState;
  private transient CommitMetadata metadata;
  private transient ListState<CommitMetadata> commitMetadataState;

  public IcebergCommitter(Table table, Configuration config) {
    this.config = config;

    // current Iceberg sink implementation can't work with concurrent checkpoints.
    // We disable concurrent checkpoints by default as min pause is set to 60s by default.
    // Add an assertion to fail explicit in case job enables concurrent checkpoints.
    CheckpointConfig checkpointConfig = StreamExecutionEnvironment.getExecutionEnvironment().getCheckpointConfig();
    if (checkpointConfig.getMaxConcurrentCheckpoints() > 1) {
      throw new IllegalArgumentException("Iceberg sink doesn't support concurrent checkpoints");
    }

    namespace = config.getString(IcebergConnectorConstant.NAMESPACE, "");
    tableName = config.getString(IcebergConnectorConstant.TABLE, "");

    String region = System.getenv("EC2_REGION");
    if (Strings.isNullOrEmpty(region)) {
      region = "us-east-1";
      LOG.info("Iceberg committer {}.{} default region to us-east-1", namespace, tableName);
    }
    this.region = region;

    watermarkEnabled = !Strings.isNullOrEmpty(
        config.getString(IcebergConnectorConstant.WATERMARK_TIMESTAMP_FIELD, ""));
    watermarkPropKey = WATERMARK_PROP_KEY_PREFIX + region;
    snapshotRetentionHours = config.getLong(IcebergConnectorConstant.SNAPSHOT_RETENTION_HOURS,
        IcebergConnectorConstant.DEFAULT_SNAPSHOT_RETENTION_HOURS);
    commitRestoredManifestFiles = config.getBoolean(IcebergConnectorConstant.COMMIT_RESTORED_MANIFEST_FILES,
        IcebergConnectorConstant.DEFAULT_COMMIT_RESTORED_MANIFEST_FILES);
    icebergManifestFileDir = getIcebergManifestFileDir(config);

    // The only final fields yielded by table inputted
    spec = table.spec();
    io = table.io();

    final JobExecutionResult jobExecutionResult
        = ExecutionEnvironment.getExecutionEnvironment().getLastJobExecutionResult();
    if (jobExecutionResult != null) {
      flinkJobId = jobExecutionResult.getJobID().toString();
      LOG.info("Get Flink job ID from execution environment: {}", flinkJobId);
    } else {
      flinkJobId = new JobID().toString();
      LOG.info("Execution environment doesn't have executed job. Generate a random job ID : {}", flinkJobId);
    }
    LOG.info("Iceberg committer {}.{} created with sink config", namespace, tableName);
    LOG.info("Iceberg committer {}.{} loaded table partition spec: {}", namespace, tableName, spec);
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

    return String.format("%s/iceberg/manifest/", checkpointDir);
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  void init() {
    // TODO: duplicate logic, to extract
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    String catalogType = config.getString(IcebergConnectorConstant.CATALOG_TYPE,
                                          IcebergConnectorConstant.CATALOG_TYPE_DEFAULT);
    Catalog catalog = null;
    switch (catalogType.toUpperCase()) {
      case IcebergConnectorConstant.HIVE_CATALOG:
        hadoopConf.set(ConfVars.METASTOREURIS.varname, config.getString(ConfVars.METASTOREURIS.varname, ""));
        catalog = HiveCatalogs.loadCatalog(hadoopConf);
        break;

      case IcebergConnectorConstant.HADOOP_CATALOG:
        catalog = new HadoopCatalog(hadoopConf,
                                    config.getString(IcebergConnectorConstant.HADOOP_CATALOG_WAREHOUSE_LOCATION, ""));
        break;

      default:
        throw new UnsupportedOperationException("Unknown catalog type or not set: " + catalogType);
    }

    this.table = catalog.loadTable(TableIdentifier.parse(namespace + "." + tableName));

    pendingDataFiles = new ArrayList<>();
    flinkManifestFiles = new ArrayList<>();
    metadata = CommitMetadata.newBuilder()
        .setLastCheckpointId(0)
        .setLastCheckpointTimestamp(0)
        .setLastCommitTimestamp(System.currentTimeMillis())
        .build();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    init();

    Preconditions.checkState(manifestFileState == null,
        "checkpointedFilesState has already been initialized.");
    Preconditions.checkState(commitMetadataState == null,
        "commitMetadataState has already been initialized.");
    manifestFileState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
        "iceberg-committer-manifest-files-state", ManifestFileState.class));
    commitMetadataState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
        "iceberg-committer-metadata-state", CommitMetadata.class));

    if (context.isRestored()) {
      final Iterable<CommitMetadata> restoredMetadata = commitMetadataState.get();
      if (null != restoredMetadata) {
        LOG.info("Iceberg committer {}.{} restoring metadata", namespace, tableName);
        List<CommitMetadata> metadataList = new ArrayList<>();
        for (CommitMetadata entry : restoredMetadata) {
          metadataList.add(entry);
        }
        Preconditions.checkState(1 == metadataList.size(),
            "metadata list size should be 1. got " + metadataList.size());
        metadata = metadataList.get(0);
        LOG.info("Iceberg committer {}.{} restored metadata: {}",
            namespace, tableName, CommitMetadataUtil.encodeAsJson(metadata));
      } else {
        LOG.info("Iceberg committer {}.{} has nothing to restore for metadata", namespace, tableName);
      }

      Iterable<ManifestFileState> restoredManifestFileStates = manifestFileState.get();
      if (null != restoredManifestFileStates) {
        LOG.info("Iceberg committer {}.{} restoring manifest files",
            namespace, tableName);
        for (ManifestFileState manifestFileState : restoredManifestFileStates) {
          flinkManifestFiles.add(GenericFlinkManifestFile.fromState(manifestFileState));
        }
        LOG.info("Iceberg committer {}.{} restored {} manifest files: {}",
            namespace, tableName, flinkManifestFiles.size(), flinkManifestFiles);
        final long now = System.currentTimeMillis();
        if (now - metadata.getLastCheckpointTimestamp() > TimeUnit.HOURS.toMillis(snapshotRetentionHours)) {
          flinkManifestFiles.clear();
          LOG.info("Iceberg committer {}.{} cleared restored manifest files as checkpoint timestamp is too old: " +
                  "checkpointTimestamp = {}, now = {}, snapshotRetentionHours = {}",
              namespace, tableName, metadata.getLastCheckpointTimestamp(), now, snapshotRetentionHours);
        } else {
          flinkManifestFiles = removeCommittedManifests(flinkManifestFiles);
          if (flinkManifestFiles.isEmpty()) {
            LOG.info("Iceberg committer {}.{} has zero uncommitted manifest files from restored state",
                namespace, tableName);
          } else {
            if (commitRestoredManifestFiles) {
              commitRestoredManifestFiles();
            } else {
              LOG.info("skip commit of restored manifest files");
            }
          }
        }
      } else {
        LOG.info("Iceberg committer {}.{} has nothing to restore for manifest files", namespace, tableName);
      }
    }
  }

  @VisibleForTesting
  void commitRestoredManifestFiles() throws Exception {
    LOG.info("Iceberg committer {}.{} committing last uncompleted transaction upon recovery: " +
            "metadata = {}, flink manifest files ({}) = {}", namespace, tableName,
        CommitMetadataUtil.encodeAsJson(metadata),
        flinkManifestFiles.size(), flinkManifestFiles);
    commit();
    LOG.info("Iceberg committer {}.{} committed last uncompleted transaction upon recovery: " +
            "metadata = {}, flink manifest files ({}) = {}", namespace, tableName,
        CommitMetadataUtil.encodeAsJson(metadata),
        flinkManifestFiles.size(), flinkManifestFiles);
    postCommitSuccess();
  }

  private List<FlinkManifestFile> removeCommittedManifests(List<FlinkManifestFile> flinkManifestFiles) {
    int snapshotCount = 0;
    String result = "succeeded";
    final long start = System.currentTimeMillis();
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
      return uncommittedManifestFiles;
    } catch (Throwable t) {
      result = "failed";
      //LOG.error(String.format("Iceberg committer %s.%s failed to check transaction completed", database, tableName),
      //          t);
      LOG.error("Iceberg committer {}.{} failed to check transaction completed. Throwable = {}",
                namespace, tableName, t);
      throw t;
    } finally {
      final long duration = System.currentTimeMillis() - start;
      LOG.info("Iceberg committer {}.{} {} to check transaction completed" +
               " after iterating {} snapshots and {} milli-seconds",
               namespace, tableName, result, snapshotCount, duration);
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    LOG.info("Iceberg committer {}.{} snapshot state: checkpointId = {}, triggerTime = {}",
        namespace, tableName, context.getCheckpointId(), context.getCheckpointTimestamp());
    Preconditions.checkState(manifestFileState != null,
        "manifest files state has not been properly initialized.");
    Preconditions.checkState(commitMetadataState != null,
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
        namespace, tableName, pendingDataFiles.size());
    String result = "succeeded";
    final long start = System.currentTimeMillis();
    try {
      final String manifestFileName = Joiner.on("_")
          .join(flinkJobId, context.getCheckpointId(), context.getCheckpointTimestamp());
      // Iceberg requires file format suffix right now
      final String manifestFileNameWithSuffix = manifestFileName + ".avro";
      OutputFile outputFile = io.newOutputFile(icebergManifestFileDir + manifestFileNameWithSuffix);
      ManifestWriter manifestWriter = ManifestWriter.write(spec, outputFile);  // TODO: deprecating

      // stats
      long recordCount = 0;
      long byteCount = 0;
      long lowWatermark = Long.MAX_VALUE;
      long highWatermark = Long.MIN_VALUE;
      for (FlinkDataFile flinkDataFile : pendingDataFiles) {
        DataFile dataFile = flinkDataFile.getIcebergDataFile();
        manifestWriter.add(dataFile);
        // update stats
        recordCount += dataFile.recordCount();
        byteCount += dataFile.fileSizeInBytes();
        if (flinkDataFile.getLowWatermark() < lowWatermark) {
          lowWatermark = flinkDataFile.getLowWatermark();
        }
        if (flinkDataFile.getHighWatermark() > highWatermark) {
          highWatermark = flinkDataFile.getHighWatermark();
        }
        LOG.debug("Data file with size of {} bytes added to manifest", dataFile.fileSizeInBytes());
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
      final AtomicInteger counter = new AtomicInteger(0);
      Collection<List<String>> listOfFileList = pendingDataFiles.stream()
          .map(flinkDataFile -> flinkDataFile.toCompactDump())
          .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / 50))
          .values();
      for (List<String> fileList : listOfFileList) {
        LOG.info("Iceberg committer {}.{} created manifest file {} for {}/{} data files: {}",
            namespace, tableName, manifestFile.path(), fileList.size(), pendingDataFiles.size(), fileList);
      }
      return flinkManifestFile;
    } catch (Throwable t) {
      result = "failed";
      //LOG.error(String.format("Iceberg committer %s.%s failed to create manifest file for %d pending data files",
      //          database, tableName, pendingDataFiles.size()), t);
      LOG.error("Iceberg committer {}.{} failed to create manifest file for {} pending data files. Throwable={}",
          namespace, tableName, pendingDataFiles.size(), t);
      throw t;
    } finally {
      final long duration = System.currentTimeMillis() - start;
      LOG.info("Iceberg committer {}.{} {} to create manifest file with {} data files after {} milli-seconds",
          namespace, tableName, result, pendingDataFiles.size(), duration);
    }
  }

  /**
   * Extract watermark from old {@link CommitMetadata} and {@link FlinkManifestFile},
   * to build a new {@link CommitMetadata}.
   */
  private CommitMetadata updateMetadata(
      CommitMetadata oldMetadata,
      FunctionSnapshotContext context,
      @Nullable FlinkManifestFile flinkManifestFile) {
    LOG.info("Iceberg committer {}.{} updating metadata {} with manifest file {}",
        namespace, tableName, CommitMetadataUtil.encodeAsJson(oldMetadata), flinkManifestFile);
    CommitMetadata.Builder metadataBuilder = CommitMetadata.newBuilder(oldMetadata)
        .setLastCheckpointId(context.getCheckpointId())
        .setLastCheckpointTimestamp(context.getCheckpointTimestamp());
    if (watermarkEnabled) {
      Long watermark = oldMetadata.getWatermark();
      if (flinkManifestFile == null) {
        // when there is no data to be committed
        // use elapsed wall clock time to move watermark forward.
        if (watermark != null) {
          final long elapsedTimeMs = System.currentTimeMillis() - oldMetadata.getLastCommitTimestamp();
          watermark += elapsedTimeMs;
        } else {
          watermark = System.currentTimeMillis();
        }
      } else {
        // use lowWatermark.
        if (flinkManifestFile.lowWatermark() == null) {
          throw new IllegalArgumentException("Watermark is enabled but lowWatermark is null");
        }
        // in case one container/slot is lagging behind,
        // we want to move watermark forward based on the slowest.
        final long newWatermark = flinkManifestFile.lowWatermark();
        // make sure watermark doesn't go back in time
        if (watermark == null || newWatermark > watermark) {
          watermark = newWatermark;
        }
      }
      metadataBuilder.setWatermark(watermark);
    }
    CommitMetadata newMetadata = metadataBuilder.build();
    LOG.info("Iceberg committer {}.{} updated metadata {} with manifest file {}",
        namespace, tableName, CommitMetadataUtil.encodeAsJson(newMetadata), flinkManifestFile);
    return newMetadata;
  }

  private void checkpointState(List<FlinkManifestFile> flinkManifestFiles, CommitMetadata metadata) throws Exception {
    LOG.info("Iceberg committer {}.{} checkpointing state", namespace, tableName);
    List<ManifestFileState> manifestFileStates = flinkManifestFiles.stream()
        .map(f -> f.toState())
        .collect(Collectors.toList());
    manifestFileState.clear();
    manifestFileState.addAll(manifestFileStates);
    commitMetadataState.clear();
    commitMetadataState.add(metadata);
    LOG.info("Iceberg committer {}.{} checkpointed state: metadata = {}, flinkManifestFiles({}) = {}",
        namespace, tableName, CommitMetadataUtil.encodeAsJson(metadata),
        flinkManifestFiles.size(), flinkManifestFiles);
  }

  private void postSnapshotSuccess() {
    pendingDataFiles.clear();
    LOG.debug("Un-committed manifest file count {}, containing data file count {}, record count {} and byte count {}",
        flinkManifestFiles.size(),
        FlinkManifestFileUtil.getDataFileCount(flinkManifestFiles),
        FlinkManifestFileUtil.getRecordCount(flinkManifestFiles),
        FlinkManifestFileUtil.getByteCount(flinkManifestFiles)
    );
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    LOG.info("Iceberg committer {}.{} checkpoint {} completed", namespace, tableName, checkpointId);
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
              namespace, tableName, t);
        }
      } else {
        // TODO: it would be nice to fix this and allow concurrent checkpoint
        LOG.info("Iceberg committer {}.{} skip committing transaction: " +
                "notify complete checkpoint id = {}, last manifest checkpoint id = {}",
            namespace, tableName, checkpointId, metadata.getLastCheckpointId());
      }
    }
  }

  @VisibleForTesting
  void commit() {
    if (!flinkManifestFiles.isEmpty() || watermarkEnabled) {
      final long start = System.currentTimeMillis();
      try {
        // prepare and commit transactions in two separate methods
        // so that we can measure latency separately.
        Transaction transaction = prepareTransaction(flinkManifestFiles, metadata);
        commitTransaction(transaction);
        final long duration = System.currentTimeMillis() - start;
        LOG.info("Iceberg committer {}.{} succeeded to commit {} manifest files after {} milli-seconds",
            namespace, tableName, flinkManifestFiles.size(), TimeUnit.NANOSECONDS.toMillis(duration));
      } catch (Throwable t) {
        final long duration = System.currentTimeMillis() - start;
        //LOG.error(String.format("Iceberg committer %s.%s failed to commit %d manifest files after %d milli-seconds",
        //database, tableName, flinkManifestFiles.size(), TimeUnit.NANOSECONDS.toMillis(duration)), t);
        LOG.error("Iceberg committer {}.{} failed to commit {} manifest files after {} milli-seconds." +
                  " Throwable = ",
            namespace, tableName, flinkManifestFiles.size(), duration, t);
        throw t;
      }
    } else {
      LOG.info("Iceberg committer {}.{} skip commit, " +
               "as there are no uncommitted data files and watermark is disabled",
               namespace, tableName);
    }
  }

  private Transaction prepareTransaction(List<FlinkManifestFile> flinkManifestFiles, CommitMetadata metadata) {
    LOG.info("Iceberg committer {}.{} start to prepare transaction: {}",
        namespace, tableName, CommitMetadataUtil.encodeAsJson(metadata));
    final long start = System.currentTimeMillis();
    try {
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
            namespace, tableName, flinkManifestFiles.size());
      }
      if (watermarkEnabled) {
        UpdateProperties updateProperties = transaction.updateProperties();
        updateProperties.set(watermarkPropKey, Long.toString(metadata.getWatermark()));
        updateProperties.commit();
        LOG.info("Iceberg committer {}.{} set watermark to {}", namespace, tableName, metadata.getWatermark());
      }
      return transaction;
    } finally {
      final long duration = System.currentTimeMillis() - start;
      LOG.debug("Transaction prepared in {} milli-seconds", duration);
    }
  }

  private void commitTransaction(Transaction transaction) {
    LOG.info("Iceberg committer {}.{} start to commit transaction: {}",
        namespace, tableName, CommitMetadataUtil.encodeAsJson(metadata));
    final long start = System.currentTimeMillis();
    try {
      transaction.commitTransaction();
    } finally {
      final long duration = System.currentTimeMillis() - start;
      LOG.debug("Transaction committed in {} milli-seconds", duration);
    }
  }

  private void postCommitSuccess() {
    LOG.info("Iceberg committer {}.{} update metrics and metadata post commit success", namespace, tableName);
    LOG.debug("Committed manifest file count {}, containing data file count {}, record count {} and byte count {}",
        flinkManifestFiles.size(),
        FlinkManifestFileUtil.getDataFileCount(flinkManifestFiles),
        FlinkManifestFileUtil.getRecordCount(flinkManifestFiles),
        FlinkManifestFileUtil.getByteCount(flinkManifestFiles));
    final Long lowWatermark = FlinkManifestFileUtil.getLowWatermark(flinkManifestFiles);
    if (null != lowWatermark) {
      LOG.debug("Low watermark as {}", lowWatermark);
    }
    final Long highWatermark = FlinkManifestFileUtil.getHighWatermark(flinkManifestFiles);
    if (null != highWatermark) {
      LOG.debug("High watermark as {}", highWatermark);
    }
    if (metadata.getWatermark() != null) {
      LOG.debug("Watermark as {}", metadata.getWatermark());
    }
    final long lastCommitTimestamp = System.currentTimeMillis();
    metadata.setLastCommitTimestamp(lastCommitTimestamp);
    flinkManifestFiles.clear();
  }

  @Override
  public void invoke(FlinkDataFile value, Context context) throws Exception {
    pendingDataFiles.add(value);
    LOG.debug("Receive file count {}, containing record count {} and file size in byte {}",
        value.getIcebergDataFile().recordCount(),
        value.getIcebergDataFile().fileSizeInBytes());
    LOG.debug("Iceberg committer {}.{} has total pending files {} after receiving new data file: {}",
        namespace, tableName, pendingDataFiles.size(), value.toCompactDump());
  }

}
