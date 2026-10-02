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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reproducer for the non-dynamic {@link IcebergCommitter} exposure to the duplicate-commit race
 * described in apache/iceberg issue #14425.
 *
 * <p>Interleaving reproduced (deterministically, via an intercepted {@link AppendFiles#commit()}):
 *
 * <ol>
 *   <li>Committer A receives the commit request for checkpoint 1, reads
 *       getMaxCommittedCheckpointId() == -1 (nothing committed yet), decides to commit.
 *   <li>Before A's table commit lands, the SAME committable is committed by another committer
 *       instance B (in production: the first commit request was in flight on the REST catalog when
 *       the job restarted; the restarted job re-delivers the committable and commits it while the
 *       original request also lands).
 *   <li>A's commit then proceeds: SnapshotProducer.refresh() picks up B's snapshot as the new
 *       parent, so A's append applies cleanly on top -- no conflict, no retry.
 * </ol>
 *
 * <p>Expected (fixed) behavior -- what DynamicCommitter does since PR #14517: detect that the
 * branch already contains a commit for this checkpointId (MaxCommittedCheckpointIdValidator) and
 * skip the second commit, leaving ONE snapshot. The assertion below asserts that correct behavior,
 * so this test FAILS on the unpatched IcebergCommitter, demonstrating duplicate data.
 */
class TestIcebergCommitterDuplicateCommit {

  private static final String OPERATOR_ID = "flink-sink";
  private static final String JOB_ID = "jobId";
  private static final long CHECKPOINT_ID = 1L;
  private static final long ROW_COUNT = 5L;

  @TempDir private File temporaryFolder;
  @TempDir private File flinkManifestFolder;

  private Table table;
  private TableLoader tableLoader;

  private final DataFile dataFile =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(0)
          .withMetrics(
              new Metrics(
                  ROW_COUNT,
                  null,
                  ImmutableMap.of(1, 5L),
                  ImmutableMap.of(1, 0L),
                  null,
                  ImmutableMap.of(
                      1,
                      org.apache.iceberg.types.Conversions.toByteBuffer(
                          org.apache.iceberg.types.Types.LongType.get(), 0L)),
                  ImmutableMap.of(
                      1,
                      org.apache.iceberg.types.Conversions.toByteBuffer(
                          org.apache.iceberg.types.Types.LongType.get(), 4L))))
          .build();

  @BeforeEach
  public void before() throws Exception {
    String warehouse = temporaryFolder.getAbsolutePath();
    String tablePath = warehouse.concat("/test");
    assertThat(new File(tablePath).mkdir()).isTrue();
    String tableLocation = "file:" + tablePath;
    Map<String, String> props =
        ImmutableMap.of(
            org.apache.iceberg.TableProperties.FORMAT_VERSION, "2",
            ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION,
                flinkManifestFolder.getAbsolutePath(),
            IcebergCommitter.MAX_CONTINUOUS_EMPTY_COMMITS, "1");
    table = SimpleDataUtil.createTable(tableLocation, props, false);
    tableLoader = TableLoader.fromHadoopTable(tableLocation);
  }

  @Test
  public void testConcurrentCommitOfSameCheckpointCausesDuplicate() throws Exception {
    Committer.CommitRequest<IcebergCommittable> requestForA =
        buildCommitRequestFor(JOB_ID, CHECKPOINT_ID);
    Committer.CommitRequest<IcebergCommittable> requestForB =
        buildCommitRequestFor(JOB_ID, CHECKPOINT_ID);

    IcebergCommitter committerB = committer(tableLoader);

    AtomicBoolean fired = new AtomicBoolean(false);
    Runnable concurrentCommit =
        () -> {
          if (fired.compareAndSet(false, true)) {
            try {
              committerB.commit(Collections.singletonList(requestForB));
            } catch (Exception e) {
              throw new RuntimeException("Concurrent commit by committer B failed", e);
            }
          }
        };

    IcebergCommitter committerA = committer(interceptingLoader(tableLoader, concurrentCommit));
    committerA.commit(Collections.singletonList(requestForA));

    table.refresh();
    long snapshots = Lists.newArrayList(table.snapshots()).size();
    long totalRecords =
        Long.parseLong(table.currentSnapshot().summary().getOrDefault("total-records", "0"));

    System.out.println(
        "REPRO RESULT: snapshots="
            + snapshots
            + " total-records="
            + totalRecords
            + " (expected on FIXED committer: snapshots=1 total-records="
            + ROW_COUNT
            + ")");

    // Correct (fixed) behavior: exactly one commit for checkpoint 1 lands.
    // On the unpatched IcebergCommitter this fails with snapshots=2 / total-records=10:
    // the same checkpoint's data files are committed twice (duplicate data).
    assertThat(snapshots)
        .as("Only one commit should land for a single checkpoint's committable")
        .isEqualTo(1L);
    assertThat(totalRecords).isEqualTo(ROW_COUNT);
  }

  // ---------------- helpers ----------------

  private IcebergCommitter committer(TableLoader loader) {
    IcebergFilesCommitterMetrics metrics = mock(IcebergFilesCommitterMetrics.class);
    return new IcebergCommitter(
        loader,
        SnapshotRef.MAIN_BRANCH,
        Collections.singletonMap("flink.test", getClass().getName()),
        false,
        10,
        "sinkId",
        metrics,
        false,
        0);
  }

  /** TableLoader whose loaded Table intercepts AppendFiles.commit() to run {@code hook} first. */
  private TableLoader interceptingLoader(TableLoader delegate, Runnable hook) {
    return new TableLoader() {
      @Override
      public void open() {
        delegate.open();
      }

      @Override
      public boolean isOpen() {
        return delegate.isOpen();
      }

      @Override
      public Table loadTable() {
        return interceptTable(delegate.loadTable(), hook);
      }

      @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
      @Override
      public TableLoader clone() {
        return interceptingLoader(delegate.clone(), hook);
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }

  private static Table interceptTable(Table delegate, Runnable hook) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          Object result = invoke(delegate, method, args);
          if ("newAppend".equals(method.getName())) {
            return interceptAppend((AppendFiles) result, hook);
          }
          return result;
        };
    return (Table)
        Proxy.newProxyInstance(Table.class.getClassLoader(), new Class<?>[] {Table.class}, handler);
  }

  private static AppendFiles interceptAppend(AppendFiles delegate, Runnable hook) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          if ("commit".equals(method.getName())) {
            hook.run();
          }
          Object result = invoke(delegate, method, args);
          // fluent API returns the delegate; keep returning the proxy instead
          return result == delegate ? proxy : result;
        };
    return (AppendFiles)
        Proxy.newProxyInstance(
            AppendFiles.class.getClassLoader(), new Class<?>[] {AppendFiles.class}, handler);
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Exception {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw e;
    }
  }

  private Committer.CommitRequest<IcebergCommittable> buildCommitRequestFor(
      String myJobID, long checkpoint) throws IOException {
    WriteResult writeResult = WriteResult.builder().addDataFiles(dataFile).build();
    IcebergCommittable committable =
        new IcebergCommittable(
            buildIcebergWriteAggregator(myJobID, OPERATOR_ID)
                .writeToManifest(Lists.newArrayList(writeResult), checkpoint),
            myJobID,
            OPERATOR_ID,
            checkpoint);
    CommittableWithLineage<IcebergCommittable> committableWithLineage =
        new CommittableWithLineage<>(committable, checkpoint, 1);
    @SuppressWarnings("unchecked")
    Committer.CommitRequest<IcebergCommittable> commitRequest = mock(Committer.CommitRequest.class);
    doReturn(committableWithLineage.getCommittable()).when(commitRequest).getCommittable();
    return commitRequest;
  }

  private IcebergWriteAggregator buildIcebergWriteAggregator(String myJobId, String operatorId) {
    IcebergWriteAggregator icebergWriteAggregator = spy(new IcebergWriteAggregator(tableLoader));
    StreamTask ctx = mock(StreamTask.class);
    Environment env = mock(Environment.class);
    StreamingRuntimeContext streamingRuntimeContext = mock(StreamingRuntimeContext.class);
    TaskInfo taskInfo = mock(TaskInfo.class);
    JobID myJobID = mock(JobID.class);
    OperatorID operatorID = mock(OperatorID.class);
    doReturn(myJobId).when(myJobID).toString();
    doReturn(myJobID).when(env).getJobID();
    doReturn(env).when(ctx).getEnvironment();
    doReturn(ctx).when(icebergWriteAggregator).getContainingTask();
    doReturn(operatorId).when(operatorID).toString();
    doReturn(operatorID).when(icebergWriteAggregator).getOperatorID();
    doReturn(0).when(taskInfo).getAttemptNumber();
    doReturn(taskInfo).when(streamingRuntimeContext).getTaskInfo();
    doReturn(streamingRuntimeContext).when(icebergWriteAggregator).getRuntimeContext();
    try {
      icebergWriteAggregator.open();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return icebergWriteAggregator;
  }
}
