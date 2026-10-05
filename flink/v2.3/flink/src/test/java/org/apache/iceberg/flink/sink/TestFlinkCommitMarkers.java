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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import java.util.List;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.sink.FlinkCommitMarkers.CommitMarker;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestFlinkCommitMarkers {
  private static final String DB = "db";
  private static final String TABLE = "table";
  private static final String MAIN = SnapshotRef.MAIN_BRANCH;

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DB, TABLE);

  private final String jobId = JobID.generate().toHexString();
  private final String operatorId = new OperatorID().toHexString();
  private Table table;
  private int files;

  @BeforeEach
  void before() {
    table = CATALOG_EXTENSION.catalog().createTable(identifier(), SimpleDataUtil.SCHEMA);
  }

  @Test
  void testCommitRecordsTheMarkerInTheSummaryAndTheProperties() {
    String branch = "release.1%x";
    long before = System.currentTimeMillis();
    commit(table, branch, jobId, operatorId, 7);
    long after = System.currentTimeMillis();

    table.refresh();
    assertThat(table.snapshot(branch).summary())
        .containsEntry(SinkUtil.FLINK_JOB_ID, jobId)
        .containsEntry(SinkUtil.OPERATOR_ID, operatorId)
        .containsEntry(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID, "7")
        .containsEntry(SinkUtil.BRANCH, branch);
    assertThat(FlinkCommitMarkers.markers(table))
        .singleElement()
        .satisfies(
            marker -> {
              assertThat(marker)
                  .isEqualTo(
                      new CommitMarker(branch, jobId, operatorId, 7, marker.updatedAtMillis()));
              assertThat(marker.updatedAtMillis()).isBetween(before, after);
            });
    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, branch, jobId, operatorId))
        .isEqualTo(7);
    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, MAIN, jobId, operatorId))
        .isEqualTo(-1);
  }

  @Test
  void testEndOfInputStaysInTheSummary() {
    commit(table, MAIN, jobId, operatorId, 3);
    commit(table, MAIN, jobId, operatorId, IcebergStreamWriter.END_INPUT_CHECKPOINT_ID);
    FlinkCommitMarkers.recordCommittedCheckpoint(
        table, MAIN, jobId, operatorId, IcebergStreamWriter.END_INPUT_CHECKPOINT_ID);

    table.refresh();
    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, MAIN, jobId, operatorId))
        .isEqualTo(IcebergStreamWriter.END_INPUT_CHECKPOINT_ID);
    assertThat(markers()).containsExactly(tuple(jobId, 3L));
  }

  @Test
  void testUnparseableMarkers() {
    commit(table, MAIN, jobId, operatorId, 7);
    String otherJob = JobID.generate().toHexString();
    String thirdJob = JobID.generate().toHexString();
    table
        .updateProperties()
        .set(FlinkCommitMarkers.committedKey(MAIN, otherJob, operatorId), "not json")
        .set(
            FlinkCommitMarkers.committedKey(MAIN, thirdJob, operatorId) + ".9",
            "{\"checkpoint-id\":4,\"updated-at-ms\":0}")
        .set(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID + ".no-parts", "{}")
        .commit();

    assertThat(markers()).containsExactly(tuple(jobId, 7L));
    assertThatThrownBy(
            () -> FlinkCommitMarkers.maxCommittedCheckpointId(table, MAIN, otherJob, operatorId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid Flink commit marker %s=not json",
            FlinkCommitMarkers.committedKey(MAIN, otherJob, operatorId));
    assertThatThrownBy(
            () -> FlinkCommitMarkers.maxCommittedCheckpointId(table, MAIN, thirdJob, operatorId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Invalid Flink commit marker "
                + FlinkCommitMarkers.committedKey(MAIN, thirdJob, operatorId)
                + ".9=");
  }

  @Test
  void testNonCanonicalKeysAreIgnored() {
    // "b%X" is encoded as "b%25X"; a key spelling it "b%X" decodes to the same writer.
    commit(table, "b%X", jobId, operatorId, 8);
    String canonical = FlinkCommitMarkers.committedKey("b%X", jobId, operatorId);
    String nonCanonical =
        SinkUtil.MAX_COMMITTED_CHECKPOINT_ID + "." + operatorId + "." + jobId + ".b%X";
    table
        .updateProperties()
        .set(nonCanonical, "{\"checkpoint-id\":3,\"updated-at-ms\":0}")
        .commit();

    assertThat(FlinkCommitMarkers.markers(table))
        .extracting(CommitMarker::branch, CommitMarker::checkpointId)
        .containsExactly(tuple("b%X", 8L));

    FlinkCommitMarkers.removeMarkers(table, marker -> marker.checkpointId() < 5);

    table.refresh();
    assertThat(table.properties()).containsKeys(canonical, nonCanonical);
  }

  @Test
  void testWalkSkipsSnapshotsOfOtherBranches() {
    commit(table, MAIN, jobId, operatorId, 7);
    table.manageSnapshots().createBranch("b1").commit();

    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, "b1", jobId, operatorId))
        .isEqualTo(-1);

    // Snapshots written before the branch was recorded count for every branch.
    commitSummaryOnly(8);
    table.manageSnapshots().createBranch("b2").commit();

    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, "b2", jobId, operatorId))
        .isEqualTo(8);
  }

  @Test
  void testLargerOfPropertyAndWalkWins() {
    commit(table, MAIN, jobId, operatorId, 5);
    // Older committers of the same job record checkpoints in summaries only.
    commitSummaryOnly(3);
    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, MAIN, jobId, operatorId))
        .isEqualTo(5);

    commitSummaryOnly(7);
    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, MAIN, jobId, operatorId))
        .isEqualTo(7);
    assertThat(markers()).containsExactly(tuple(jobId, 5L));
  }

  @Test
  void testRecordCommittedCheckpoint() {
    commitSummaryOnly(3);
    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, MAIN, jobId, operatorId))
        .isEqualTo(3);
    assertThat(markers()).isEmpty();

    FlinkCommitMarkers.recordCommittedCheckpoint(table, MAIN, jobId, operatorId, 3);
    assertThat(markers()).containsExactly(tuple(jobId, 3L));
    String metadata = metadataLocation();

    FlinkCommitMarkers.recordCommittedCheckpoint(table, MAIN, jobId, operatorId, 3);
    FlinkCommitMarkers.recordCommittedCheckpoint(table, MAIN, jobId, operatorId, 2);
    assertThat(metadataLocation()).as("nothing to record").isEqualTo(metadata);

    SinkTestUtil.expireAllButHeads(table);
    commit(table, MAIN, JobID.generate().toHexString(), operatorId, 1);
    SinkTestUtil.expireAllButHeads(table);
    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, MAIN, jobId, operatorId))
        .isEqualTo(3);
  }

  /**
   * A recording request that lands after the writer committed a later checkpoint, as a delayed
   * request can on a catalog that applies property changes unconditionally, adds a key and never
   * lowers the writer's checkpoint.
   */
  @Test
  void testALateRecordingNeverLowersTheCheckpoint() {
    commit(table, MAIN, jobId, operatorId, 8);
    table
        .updateProperties()
        .set(
            FlinkCommitMarkers.recordedKey(MAIN, jobId, operatorId, 3),
            "{\"checkpoint-id\":3,\"updated-at-ms\":0}")
        .commit();

    table.refresh();
    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, MAIN, jobId, operatorId))
        .isEqualTo(8);
    assertThat(markers()).containsExactlyInAnyOrder(tuple(jobId, 8L), tuple(jobId, 3L));
  }

  @Test
  void testRemoveMarkers() {
    String otherJob = JobID.generate().toHexString();
    commit(table, MAIN, jobId, operatorId, 3);
    commit(table, MAIN, otherJob, operatorId, 5);
    commitSummaryOnly(4);
    FlinkCommitMarkers.recordCommittedCheckpoint(table, MAIN, jobId, operatorId, 4);
    assertThat(markers())
        .containsExactlyInAnyOrder(tuple(jobId, 3L), tuple(jobId, 4L), tuple(otherJob, 5L));

    FlinkCommitMarkers.removeMarkers(table, marker -> marker.jobId().equals(jobId));

    assertThat(markers()).containsExactly(tuple(otherJob, 5L));
  }

  @Test
  void testRemoveMarkersJudgesAMarkerRewrittenInTheMeantimeOnItsNewValue() {
    commit(table, MAIN, jobId, operatorId, 3);

    InterceptingCatalog intercepting = new InterceptingCatalog(CATALOG_EXTENSION.warehouse());
    Table interceptedTable = intercepting.loadTable(identifier());
    intercepting.onPublish(
        (delegate, base, metadata) -> {
          // The writer commits checkpoint 8 between the selection and the removal.
          commit(CATALOG_EXTENSION.catalog().loadTable(identifier()), MAIN, jobId, operatorId, 8);
          delegate.commit(base, metadata);
        });

    FlinkCommitMarkers.removeMarkers(interceptedTable, marker -> marker.checkpointId() < 5);

    assertThat(intercepting.publications()).isEqualTo(1);
    assertThat(markers()).containsExactly(tuple(jobId, 8L));
  }

  private void commit(
      Table target, String branch, String writerJobId, String writerOperatorId, long checkpointId) {
    Transaction transaction = target.newTransaction();
    AppendFiles append = transaction.newAppend().appendFile(dataFile());
    FlinkCommitMarkers.commit(
        transaction, append, branch, writerJobId, writerOperatorId, checkpointId);
  }

  // How committers before table-property markers recorded a checkpoint.
  private void commitSummaryOnly(long checkpointId) {
    table
        .newAppend()
        .appendFile(dataFile())
        .set(SinkUtil.FLINK_JOB_ID, jobId)
        .set(SinkUtil.OPERATOR_ID, operatorId)
        .set(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId))
        .commit();
    table.refresh();
  }

  private List<Tuple> markers() {
    table.refresh();
    List<Tuple> markers = Lists.newArrayList();
    FlinkCommitMarkers.markers(table)
        .forEach(marker -> markers.add(tuple(marker.jobId(), marker.checkpointId())));
    return markers;
  }

  private String metadataLocation() {
    table.refresh();
    return ((HasTableOperations) table).operations().current().metadataFileLocation();
  }

  private DataFile dataFile() {
    files += 1;
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("/path/to/data-" + files + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static TableIdentifier identifier() {
    return TableIdentifier.of(DB, TABLE);
  }
}
