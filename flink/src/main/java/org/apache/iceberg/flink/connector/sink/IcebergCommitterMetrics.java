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

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.histogram.BucketDistributionSummary;
import com.netflix.spectator.api.patterns.PolledMeter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;

/**
 * This doesn't need to be singleton, since we always set committer parallelism to 1.
 * <p>
 * Make this a regular class so that we can leverage Spectator to auto deregister
 * garbage collected polled gauge (PolledMeter).
 * https://github.com/Netflix/spectator/blob/master/docs/intro/gauge.md
 */
public class IcebergCommitterMetrics {

  private static final String UNCOMMITTED_MANIFEST_FILES = "iceberg_sink.uncommitted_manifest_files";
  private static final String UNCOMMITTED_FILES = "iceberg_sink.uncommitted_files";
  private static final String UNCOMMITTED_RECORDS = "iceberg_sink.uncommitted_records";
  private static final String UNCOMMITTED_BYTES = "iceberg_sink.uncommitted_bytes";
  private static final String LOW_WATERMARK = "iceberg_sink.low_watermark";
  private static final String HIGH_WATERMARK = "iceberg_sink.high_watermark";
  private static final String VTTS_WATERMAK = "iceberg_sink.vtts_watermark";
  private static final String ELAPSED_SECONDS_SINCE_LAST_COMMIT = "iceberg_sink.elapsed_seconds_since_last_commit";

  private final Registry registry;
  private final String database;
  private final String table;

  // committer metrics
  private final Counter receivedFiles;
  private final Counter receivedRecords;
  private final Counter receivedBytes;
  private final Counter checkTransactionCompletedSuccess;
  private final Id checkTransactionCompletedFailureId;
  private final Timer checkTransactionCommittedLatency;
  private final Counter createManifestFileSuccess;
  private final Id createManifestFileFailureId;
  private final Timer createManifestFileLatency;
  private final Counter prepareTransactionSuccess;
  private final Id prepareTransactionFailureId;
  private final Timer prepareTransactionLatency;
  private final Counter commitSuccess;
  private final Id commitFailureId;
  private final Timer commitLatency;
  private final Counter committedManifestFiles;
  private final Counter committedFiles;
  private final Counter committedRecords;
  private final Counter committedBytes;
  private final Counter skipCommitCheckpointIdMismatch;
  private final AtomicLong uncommittedManifestFiles;
  private final AtomicLong uncommittedFiles;
  private final AtomicLong uncommittedRecords;
  private final AtomicLong uncommittedBytes;
  private final AtomicLong lowWatermark;
  private final AtomicLong highWatermark;
  private final AtomicLong vttsWatermark;
  private final AtomicLong lastCommitTimestamp;
  private final DistributionSummary fileSizeDistributionHistogram;

  public IcebergCommitterMetrics(final Registry registry, final String database, final String table) {
    this.registry = registry;
    this.database = database;
    this.table = table;

    receivedFiles = registry.counter(createId("iceberg_sink.committer_received_files"));
    receivedRecords = registry.counter(createId("iceberg_sink.committer_received_records"));
    receivedBytes = registry.counter(createId("iceberg_sink.committer_received_bytes"));
    checkTransactionCompletedSuccess = registry.counter(createId("iceberg_sink.check_transaction_completed_success"));
    checkTransactionCompletedFailureId = createId("iceberg_sink.check_transaction_completed_failure");
    checkTransactionCommittedLatency = registry.timer("iceberg_sink.check_transaction_committed_latency");
    createManifestFileSuccess = registry.counter(createId("iceberg_sink.create_manifest_file_success"));
    createManifestFileFailureId = createId("iceberg_sink.create_manifest_file_failure");
    createManifestFileLatency = registry.timer("iceberg_sink.create_manifest_file_latency");
    prepareTransactionSuccess = registry.counter(createId("iceberg_sink.prepare_transaction_success"));
    prepareTransactionFailureId = createId("iceberg_sink.prepare_transaction_failure");
    prepareTransactionLatency = registry.timer("iceberg_sink.prepare_transaction_latency");
    commitSuccess = registry.counter(createId("iceberg_sink.commit_success"));
    commitFailureId = createId("iceberg_sink.commit_failure");
    commitLatency = registry.timer("iceberg_sink.commit_latency");
    committedManifestFiles = registry.counter(createId("iceberg_sink.committed_manifest_files"));
    committedFiles = registry.counter(createId("iceberg_sink.committed_files"));
    committedRecords = registry.counter(createId("iceberg_sink.committed_records"));
    committedBytes = registry.counter(createId("iceberg_sink.committed_bytes"));
    skipCommitCheckpointIdMismatch =
        registry.counter(createId("iceberg_sink.skip_commit_checkpoint_id_mismatch"));

    uncommittedManifestFiles = PolledMeter.using(registry)
        .withId(createId(UNCOMMITTED_MANIFEST_FILES))
        .monitorValue(new AtomicLong(0));
    uncommittedFiles = PolledMeter.using(registry)
        .withId(createId(UNCOMMITTED_FILES))
        .monitorValue(new AtomicLong(0));
    uncommittedRecords = PolledMeter.using(registry)
        .withId(createId(UNCOMMITTED_RECORDS))
        .monitorValue(new AtomicLong(0));
    uncommittedBytes = PolledMeter.using(registry)
        .withId(createId(UNCOMMITTED_BYTES))
        .monitorValue(new AtomicLong(0));
    lowWatermark = PolledMeter.using(registry)
        .withId(createId(LOW_WATERMARK))
        .monitorValue(new AtomicLong(System.currentTimeMillis()));
    highWatermark = PolledMeter.using(registry)
        .withId(createId(HIGH_WATERMARK))
        .monitorValue(new AtomicLong(System.currentTimeMillis()));
    vttsWatermark = PolledMeter.using(registry)
        .withId(createId(VTTS_WATERMAK))
        .monitorValue(new AtomicLong(System.currentTimeMillis()));
    this.lastCommitTimestamp = new AtomicLong(System.currentTimeMillis());
    PolledMeter.using(registry)
        .withId(createId(ELAPSED_SECONDS_SINCE_LAST_COMMIT))
        .monitorValue(this.lastCommitTimestamp, v ->
            TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - v.get()));

    final Id fileSizeDistributionHistogramId = createId("iceberg_sink.file_size_distribution_histogram");

    @SuppressWarnings("checkstyle:MemberName")
    final LongFunction<String> sizeBucketingFunc = new LongFunction<String>() {
      private final long MB = 1024 * 1024;
      private final long GB = 1024 * MB;
      private final long TB = 1024 * GB;

      @Override
      public String apply(long value) {
        // it is advised that label maintain lexical order
        if (value < 1 * MB) {
          return "file_size_b01_1MB";
        } else if (value < 10 * MB) {
          return "file_size_b02_10MB";
        } else if (value < 100 * MB) {
          return "file_size_b03_100MB";
        } else if (value < 200 * MB) {
          return "file_size_b04_200MB";
        } else if (value < 500 * MB) {
          return "file_size_b05_500MB";
        } else if (value < 1 * GB) {
          return "file_size_b06_1GB";
        } else if (value < 10 * GB) {
          return "file_size_b07_10GB";
        } else if (value < 100 * GB) {
          return "file_size_b08_100GB";
        } else if (value < 1 * TB) {
          return "file_size_b09_1TB";
        } else {
          return "file_size_b10_over_1TB";
        }
      }
    };
    fileSizeDistributionHistogram =
        BucketDistributionSummary.get(registry, fileSizeDistributionHistogramId, sizeBucketingFunc);
  }

  private Id createId(final String name) {
    return registry.createId(name)
        .withTag(IcebergConnectorConstant.SINK_TAG_KEY, IcebergConnectorConstant.TYPE)
        .withTag(IcebergConnectorConstant.OUTPUT_TAG_KEY, table)
        .withTag(IcebergConnectorConstant.OUTPUT_CLUSTER_TAG_KEY, database);
  }

  /**
   * Actively stop polled meter for gauges
   */
  public void stop() {
    PolledMeter.remove(registry, createId(UNCOMMITTED_MANIFEST_FILES));
    PolledMeter.remove(registry, createId(UNCOMMITTED_FILES));
    PolledMeter.remove(registry, createId(UNCOMMITTED_RECORDS));
    PolledMeter.remove(registry, createId(UNCOMMITTED_BYTES));
    PolledMeter.remove(registry, createId(LOW_WATERMARK));
    PolledMeter.remove(registry, createId(HIGH_WATERMARK));
    PolledMeter.remove(registry, createId(VTTS_WATERMAK));
    PolledMeter.remove(registry, createId(ELAPSED_SECONDS_SINCE_LAST_COMMIT));
  }


  public void incrementReceivedFiles() {
    receivedFiles.increment();
  }

  public void incrementReceivedRecords(final long delta) {
    receivedRecords.increment(delta);
  }

  public void incrementReceivedBytes(final long delta) {
    receivedBytes.increment(delta);
  }

  public void recordCheckTransactionCommittedLatency(final long amount, TimeUnit timeUnit) {
    checkTransactionCommittedLatency.record(amount, timeUnit);
  }

  public void incrementCheckTransactionCompletedSuccess() {
    checkTransactionCompletedSuccess.increment();
  }

  public void incrementCheckTransactionCompletedFailure(final Throwable throwable) {
    Id id = checkTransactionCompletedFailureId.withTag(IcebergConnectorConstant.EXCEPTION_CLASS,
                                                       throwable.getClass().getSimpleName());
    registry.counter(id).increment();
  }

  public void incrementCreateManifestFileSuccess() {
    createManifestFileSuccess.increment();
  }

  public void incrementCreateManifestFileFailure(final Throwable throwable) {
    Id id = createManifestFileFailureId.withTag(IcebergConnectorConstant.EXCEPTION_CLASS,
                                                throwable.getClass().getSimpleName());
    registry.counter(id).increment();
  }

  public void recordCreateManifestFileLatency(final long amount, TimeUnit timeUnit) {
    createManifestFileLatency.record(amount, timeUnit);
  }

  public void incrementPrepareTransactionSuccess() {
    prepareTransactionSuccess.increment();
  }

  public void incrementPrepareTransactionFailure(final Throwable throwable) {
    Id id = prepareTransactionFailureId.withTag(IcebergConnectorConstant.EXCEPTION_CLASS,
                                                throwable.getClass().getSimpleName());
    registry.counter(id).increment();
  }

  public void recordPrepareTransactionLatency(final long amount, TimeUnit timeUnit) {
    prepareTransactionLatency.record(amount, timeUnit);
  }

  public void incrementCommitSuccess() {
    commitSuccess.increment();
  }

  public void incrementCommitFailure(final Throwable throwable) {
    Id id = commitFailureId.withTag(IcebergConnectorConstant.EXCEPTION_CLASS,
                                    throwable.getClass().getCanonicalName());
    registry.counter(id).increment();
  }

  public void recordCommitLatency(final long amount, TimeUnit timeUnit) {
    commitLatency.record(amount, timeUnit);
  }

  public void incrementCommittedManifestFiles(final long delta) {
    committedManifestFiles.increment(delta);
  }

  public void incrementCommittedFiles(final long delta) {
    committedFiles.increment(delta);
  }

  public void incrementCommittedRecords(final long delta) {
    committedRecords.increment(delta);
  }

  public void incrementCommittededBytes(final long delta) {
    committedBytes.increment(delta);
  }

  public void incrementSkipCommitCheckpointIdMismatch() {
    skipCommitCheckpointIdMismatch.increment();
  }

  public void setUncommittedManifestFiles(final long count) {
    uncommittedManifestFiles.set(count);
  }

  public void setUncommittedFiles(final long count) {
    uncommittedFiles.set(count);
  }

  public void setUncommittedRecords(final long count) {
    uncommittedRecords.set(count);
  }

  public void setUncommittedBytes(final long count) {
    uncommittedBytes.set(count);
  }

  public void setLowWatermark(final long value) {
    lowWatermark.set(value);
  }

  public void setHighWatermark(final long value) {
    highWatermark.set(value);
  }

  public void setVttsWatermark(final long value) {
    vttsWatermark.set(value);
  }

  public void setLastCommitTimestamp(final long value) {
    lastCommitTimestamp.set(value);
  }

  public void recordFileSize(final long amount) {
    fileSizeDistributionHistogram.record(amount);
  }

}
