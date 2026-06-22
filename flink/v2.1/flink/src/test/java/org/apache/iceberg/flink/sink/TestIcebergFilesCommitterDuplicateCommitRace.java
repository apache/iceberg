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
package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.flink.sink.IcebergFilesCommitter.MAX_CONTINUOUS_EMPTY_COMMITS;
import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reproducer for <a href="https://github.com/apache/iceberg/issues/14425">issue #14425</a> against
 * the v1 {@code FlinkSink} committer ({@link IcebergFilesCommitter}).
 *
 * <p>The race: between {@code SinkUtil.getMaxCommittedCheckpointId(table, ...)} in {@code
 * initializeState} and {@code SnapshotProducer.refresh()} inside the subsequent {@code
 * AppendFiles.commit()}, an in-flight commit issued by the previous run can land in the catalog.
 * The committer's first read sees {@code -1} so it decides to commit, but the refresh inside commit
 * picks up the late-arriving snapshot, sets parent to it, and a second snapshot gets created with
 * the same data files. Iceberg has no validator that catches this; only stricter catalogs (e.g.
 * Databricks Unity Catalog) reject the duplicate POST.
 *
 * <p>The test wraps the {@link TableLoader} with one whose {@link TableOperations#current()}
 * returns a pre-injection snapshot of the metadata, while {@link TableOperations#refresh()} returns
 * the live metadata. That gives a deterministic TOCTOU window. The phantom snapshot is committed
 * directly to the underlying table to simulate the in-flight commit landing.
 *
 * <p>The fix lives in {@link IcebergFilesCommitter#commitOperation} (the {@code
 * MaxCommittedCheckpointIdValidator} attached via {@link
 * org.apache.iceberg.SnapshotUpdate#validateWith}). This test is the regression gate that proves
 * the fix continues to hold.
 */
public class TestIcebergFilesCommitterDuplicateCommitRace extends TestBase {

  private static final String BRANCH = SnapshotRef.MAIN_BRANCH;
  private static final FileFormat FORMAT = FileFormat.PARQUET;
  private static final Configuration CONF = new Configuration();

  private File flinkManifestFolder;

  @BeforeEach
  public void setupTable() throws IOException {
    // TestBase reads formatVersion from a @Parameter; we run as a plain @Test, so set it here.
    this.formatVersion = 1;
    flinkManifestFolder = Files.createTempDirectory(temp, "flink").toFile();
    this.metadataDir = new File(tableDir, "metadata");

    table = create(SimpleDataUtil.SCHEMA, PartitionSpec.unpartitioned());
    table
        .updateProperties()
        .set(DEFAULT_FILE_FORMAT, FORMAT.name())
        .set(FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath())
        .set(MAX_CONTINUOUS_EMPTY_COMMITS, "1")
        .commit();
  }

  @Test
  public void recoveryDoesNotCreateDuplicateSnapshotWhenInFlightCommitLandsAfterStaleCheck()
      throws Exception {
    long checkpointId = 1L;
    long timestamp = 1L;
    JobID jobId = new JobID();
    OperatorID operatorId;
    OperatorSubtaskState recoveredState;
    DataFile dataFile;
    RowData row = SimpleDataUtil.createRowData(1, "hello");

    // Lifetime 1: write data for chk-1, snapshot operator state. Do not notify-complete: the
    // simulated in-flight commit never reported success back to the client.
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId, new TestTableLoader(table.location()))) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      dataFile = writeDataFile("data-chk-1", ImmutableList.of(row));
      harness.processElement(of(checkpointId, dataFile), timestamp);
      recoveredState = harness.snapshot(checkpointId, ++timestamp);
    }

    // Snapshot the metadata BEFORE the in-flight commit lands. This is the "stale" view the
    // recovering committer will read during its max-committed-checkpoint-id check.
    table.refresh();
    TableMetadata staleMetadata = ((HasTableOperations) table).operations().current();

    // Inject the phantom snapshot — the in-flight commit landing late in the catalog. Its
    // summary carries the same flink job/operator/checkpoint markers the committer would have
    // written.
    table
        .newAppend()
        .appendFile(dataFile)
        .set(SinkUtil.FLINK_JOB_ID, jobId.toString())
        .set(SinkUtil.OPERATOR_ID, operatorId.toString())
        .set(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId))
        .toBranch(BRANCH)
        .commit();
    table.refresh();
    assertThat(ImmutableList.copyOf(table.snapshots())).hasSize(1);

    // Lifetime 2: recover. The TocTouTableLoader makes ops.current() return the pre-phantom view
    // until ops.refresh() is called inside SnapshotProducer.apply(). The recovery sees
    // max-committed-checkpoint-id = -1, decides to commit chk-1, then refresh picks up the
    // phantom and a second snapshot for chk-1 gets created. With the fix, no second snapshot is
    // created.
    TableLoader staleLoader = new StaleCurrentTableLoader(table.location(), staleMetadata);
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId, staleLoader)) {
      harness.getStreamConfig().setOperatorID(operatorId);
      harness.setup();
      harness.initializeState(recoveredState);
      harness.open();
    }

    table.refresh();
    long checkpointsCommitted =
        StreamSupport.stream(table.snapshots().spliterator(), false)
            .map(s -> s.summary().get(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID))
            .filter(v -> Long.toString(checkpointId).equals(v))
            .count();

    assertThat(checkpointsCommitted)
        .as(
            "Recovery must not produce a second snapshot for an already-committed checkpoint. "
                + "Two snapshots carrying flink.max-committed-checkpoint-id=%d reproduces #14425.",
            checkpointId)
        .isEqualTo(1L);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> createStreamSink(
      JobID jobID, TableLoader tableLoader) throws Exception {
    TestOperatorFactory factory = new TestOperatorFactory(tableLoader, BRANCH, table.spec());
    return new OneInputStreamOperatorTestHarness<>(factory, createEnvironment(jobID));
  }

  private static MockEnvironment createEnvironment(JobID jobID) {
    return new MockEnvironmentBuilder()
        .setTaskName("test task")
        .setManagedMemorySize(32 * 1024)
        .setInputSplitProvider(new MockInputSplitProvider())
        .setBufferSize(256)
        .setTaskConfiguration(new org.apache.flink.configuration.Configuration())
        .setExecutionConfig(new ExecutionConfig())
        .setMaxParallelism(16)
        .setJobID(jobID)
        .build();
  }

  private DataFile writeDataFile(String fileName, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(
        table,
        table.schema(),
        table.spec(),
        CONF,
        table.location(),
        FORMAT.addExtension(fileName),
        rows);
  }

  private static FlinkWriteResult of(long checkpointId, DataFile dataFile) {
    return new FlinkWriteResult(checkpointId, WriteResult.builder().addDataFiles(dataFile).build());
  }

  private static final class TestOperatorFactory extends AbstractStreamOperatorFactory<Void>
      implements OneInputStreamOperatorFactory<FlinkWriteResult, Void> {
    private final TableLoader tableLoader;
    private final String branch;
    private final PartitionSpec spec;

    TestOperatorFactory(TableLoader tableLoader, String branch, PartitionSpec spec) {
      this.tableLoader = tableLoader;
      this.branch = branch;
      this.spec = spec;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Void>> T createStreamOperator(
        StreamOperatorParameters<Void> params) {
      IcebergFilesCommitter committer =
          new IcebergFilesCommitter(
              params,
              tableLoader,
              false,
              Collections.singletonMap(
                  "flink.test", TestIcebergFilesCommitterDuplicateCommitRace.class.getName()),
              ThreadPools.WORKER_THREAD_POOL_SIZE,
              branch,
              spec);
      return (T) committer;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergFilesCommitter.class;
    }
  }

  /**
   * A {@link TableLoader} whose returned {@link Table} reports a pre-captured {@link TableMetadata}
   * via {@link TableOperations#current()} until {@link TableOperations#refresh()} is called. Once
   * refreshed, both {@code current()} and {@code refresh()} delegate to the real ops. This isolates
   * the TOCTOU window in a deterministic way.
   */
  private static final class StaleCurrentTableLoader implements TableLoader {
    private final String location;
    private final TableMetadata staleMetadata;

    StaleCurrentTableLoader(String location, TableMetadata staleMetadata) {
      this.location = location;
      this.staleMetadata = staleMetadata;
    }

    @Override
    public void open() {}

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public Table loadTable() {
      Table real = new TestTableLoader(location).loadTable();
      TableOperations realOps = ((HasTableOperations) real).operations();
      return new BaseTable(new StaleCurrentTableOperations(realOps, staleMetadata), real.name());
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public TableLoader clone() {
      return new StaleCurrentTableLoader(location, staleMetadata);
    }

    @Override
    public void close() {}
  }

  private static final class StaleCurrentTableOperations implements TableOperations {
    private final TableOperations delegate;
    private final TableMetadata staleMetadata;
    private boolean refreshed = false;

    StaleCurrentTableOperations(TableOperations delegate, TableMetadata staleMetadata) {
      this.delegate = delegate;
      this.staleMetadata = staleMetadata;
    }

    @Override
    public TableMetadata current() {
      return refreshed ? delegate.current() : staleMetadata;
    }

    @Override
    public TableMetadata refresh() {
      this.refreshed = true;
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      delegate.commit(base, metadata);
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
      return delegate.temp(uncommittedMetadata);
    }
  }
}
