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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Two committers commit the same checkpoint, as when a slow commit lands after the job restarted
 * and the restored committable is committed again (apache/iceberg#14425). The committer that reads
 * the committed checkpoint id from a table view taken before the other commit landed must not
 * commit the checkpoint a second time.
 */
class TestIcebergCommitterDuplicateCommit {
  private static final String JOB_ID = "jobId";
  private static final String SINK_ID = "sinkId";
  private static final String OPERATOR_ID = "flink-sink";
  private static final long CHECKPOINT_ID = 1L;
  private static final long ROW_COUNT = 5L;

  @TempDir private File warehouse;
  @TempDir private File manifestFolder;

  private Table table;
  private TableLoader tableLoader;

  @BeforeEach
  void before() {
    String location = "file:" + new File(warehouse, "test").getAbsolutePath();
    Map<String, String> props =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION,
            manifestFolder.getAbsolutePath());
    this.table = SimpleDataUtil.createTable(location, props, false);
    this.tableLoader = TableLoader.fromHadoopTable(location);
  }

  @Test
  void staleCommitterDoesNotCommitCheckpointTwice() throws Exception {
    IcebergCommitter stale = committer();
    IcebergCommitter other = committer();

    other.commit(Collections.singletonList(new MockCommitRequest<>(committable(0))));
    stale.commit(Collections.singletonList(new MockCommitRequest<>(committable(1))));

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.currentSnapshot().summary())
        .containsEntry("total-records", String.valueOf(ROW_COUNT));
  }

  private IcebergCommitter committer() {
    return new IcebergCommitter(
        tableLoader.clone(),
        SnapshotRef.MAIN_BRANCH,
        Collections.emptyMap(),
        false,
        1,
        SINK_ID,
        new IcebergFilesCommitterMetrics(new UnregisteredMetricsGroup(), table.name()),
        false,
        0);
  }

  /** A committable of checkpoint 1 with its own manifest, written as by the given attempt. */
  private IcebergCommittable committable(int attempt) throws IOException {
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data-1.parquet")
            .withFileSizeInBytes(0)
            .withMetrics(new Metrics(ROW_COUNT, null, null, null, null))
            .build();
    DeltaManifests manifests =
        FlinkManifestUtil.writeCompletedFiles(
            WriteResult.builder().addDataFiles(dataFile).build(),
            () ->
                FlinkManifestUtil.createOutputFileFactory(
                        () -> table, table.properties(), JOB_ID, OPERATOR_ID, 0, attempt)
                    .create(CHECKPOINT_ID),
            table.spec(),
            2);
    return new IcebergCommittable(
        SimpleVersionedSerialization.writeVersionAndSerialize(
            DeltaManifestsSerializer.INSTANCE, manifests),
        JOB_ID,
        SINK_ID,
        OPERATOR_ID,
        CHECKPOINT_ID);
  }
}
