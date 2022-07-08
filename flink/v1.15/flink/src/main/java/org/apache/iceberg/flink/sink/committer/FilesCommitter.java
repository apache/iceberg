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
package org.apache.iceberg.flink.sink.committer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilesCommitter implements Committer<FilesCommittable>, Serializable {
  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;
  private static final Logger LOG = LoggerFactory.getLogger(FilesCommitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";

  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";

  // TableLoader to load iceberg table lazily.
  private final TableLoader tableLoader;
  private final boolean replacePartitions;
  private final Map<String, String> snapshotProperties;

  // It will have an unique identifier for one job.
  private final Table table;
  private transient Map<String, Long> maxCommittedCheckpointIdForJob = Maps.newHashMap();

  private transient ExecutorService workerPool;

  public FilesCommitter(
      TableLoader tableLoader,
      boolean replacePartitions,
      Map<String, String> snapshotProperties,
      int workerPoolSize) {
    this.tableLoader = tableLoader;
    this.replacePartitions = replacePartitions;
    this.snapshotProperties = snapshotProperties;

    this.workerPool =
        ThreadPools.newWorkerPool(
            "iceberg-worker-pool-" + Thread.currentThread().getName(), workerPoolSize);

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();
  }

  @Override
  public void commit(Collection<CommitRequest<FilesCommittable>> commitRequests)
      throws IOException, InterruptedException {
    int dataFilesNum = 0;
    int deleteFilesNum = 0;

    List<CommitRequest<FilesCommittable>> store = Lists.newArrayList();
    String jobId = null;
    long maxCommittableCheckpointId = INITIAL_CHECKPOINT_ID;

    for (CommitRequest<FilesCommittable> commitRequest : commitRequests) {
      WriteResult committable = commitRequest.getCommittable().committable();
      Long committableCheckpointId = commitRequest.getCommittable().checkpointId();
      jobId = commitRequest.getCommittable().jobID();
      if (committableCheckpointId
              > maxCommittedCheckpointIdForJob.getOrDefault(jobId, Long.MAX_VALUE)
          || getMaxCommittedCheckpointId(table, jobId) == -1) {
        // committableCheckpointId > maxCommittableCheckpointId or the job id has never written data
        // to the table.
        store.add(commitRequest);
        dataFilesNum = dataFilesNum + committable.dataFiles().length;
        deleteFilesNum = deleteFilesNum + committable.deleteFiles().length;
        maxCommittableCheckpointId = committableCheckpointId;
      } else if (committableCheckpointId > getMaxCommittedCheckpointId(table, jobId)) {
        // committable is restored from the previous job that committed data to the table.
        commitResult(
            committable.dataFiles().length,
            committable.deleteFiles().length,
            Lists.newArrayList(commitRequest));
        maxCommittedCheckpointIdForJob.put(jobId, committableCheckpointId);
      } else {
        commitRequest.signalFailedWithKnownReason(
            new CommitFailedException(
                "The data could not be committed, perhaps because the data was committed."));
      }
    }

    commitResult(dataFilesNum, deleteFilesNum, store);
    // Since flink's checkpoint id will start from the max-committed-checkpoint-id + 1 in the new
    // flink job even if it's restored from a snapshot created by another different flink job, so
    // it's safe to assign the max committed checkpoint id from restored flink job to the current
    // flink job.
    maxCommittedCheckpointIdForJob.put(jobId, maxCommittableCheckpointId);
  }

  @Override
  public void close() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }

    if (workerPool != null) {
      workerPool.shutdown();
    }
  }

  private void commitResult(
      int dataFilesNum, int deleteFilesNum, List<CommitRequest<FilesCommittable>> commitRequests) {
    int totalFiles = dataFilesNum + deleteFilesNum;

    if (totalFiles != 0) {
      if (replacePartitions) {
        replacePartitions(commitRequests, dataFilesNum, deleteFilesNum);
      } else {
        commitDeltaTxn(commitRequests, dataFilesNum, deleteFilesNum);
      }
    }
  }

  private void replacePartitions(
      List<CommitRequest<FilesCommittable>> writeResults, int dataFilesNum, int deleteFilesNum) {
    // Partition overwrite does not support delete files.
    Preconditions.checkState(deleteFilesNum == 0, "Cannot overwrite partitions with delete files.");

    // Commit the overwrite transaction.
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions().scanManifestsWith(workerPool);

    String flinkJobId = null;
    long checkpointId = -1;
    for (CommitRequest<FilesCommittable> commitRequest : writeResults) {
      WriteResult result = commitRequest.getCommittable().committable();
      Preconditions.checkState(
          result.referencedDataFiles().length == 0, "Should have no referenced data files.");
      flinkJobId = commitRequest.getCommittable().jobID();
      checkpointId = commitRequest.getCommittable().checkpointId();
      Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
    }

    commitOperation(
        writeResults,
        dynamicOverwrite,
        dataFilesNum,
        0,
        "dynamic partition overwrite",
        flinkJobId,
        checkpointId);
  }

  private void commitDeltaTxn(
      List<CommitRequest<FilesCommittable>> commitRequests, int dataFilesNum, int deleteFilesNum) {
    if (deleteFilesNum == 0) {
      // To be compatible with iceberg format V1.
      AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
      String flinkJobId = null;
      long checkpointId = -1;
      for (CommitRequest<FilesCommittable> commitRequest : commitRequests) {
        WriteResult result = commitRequest.getCommittable().committable();
        Preconditions.checkState(
            result.referencedDataFiles().length == 0, "Should have no referenced data files.");
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
        flinkJobId = commitRequest.getCommittable().jobID();
        checkpointId = commitRequest.getCommittable().checkpointId();
      }

      commitOperation(
          commitRequests, appendFiles, dataFilesNum, 0, "append", flinkJobId, checkpointId);
    } else {
      // To be compatible with iceberg format V2.
      for (CommitRequest<FilesCommittable> commitRequest : commitRequests) {
        // We don't commit the merged result into a single transaction because for the sequential
        // transaction txn1 and txn2, the equality-delete files of txn2 are required to be applied
        // to data files from txn1. Committing the merged one will lead to the incorrect delete
        // semantic.
        WriteResult result = commitRequest.getCommittable().committable();

        // Row delta validations are not needed for streaming changes that write equality deletes.
        // Equality deletes are applied to data in all previous sequence numbers, so retries may
        // push deletes further in the future, but do not affect correctness. Position deletes
        // committed to the table in this path are used only to delete rows from data files that are
        // being added in this commit. There is no way for data files added along with the delete
        // files to be concurrently removed, so there is no need to validate the files referenced by
        // the position delete files that are being committed.
        RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);
        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
        Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

        String flinkJobId = commitRequest.getCommittable().jobID();
        long checkpointId = commitRequest.getCommittable().checkpointId();
        commitOperation(
            Lists.newArrayList(commitRequest),
            rowDelta,
            dataFilesNum,
            deleteFilesNum,
            "rowDelta",
            flinkJobId,
            checkpointId);
      }
    }
  }

  private void commitOperation(
      List<CommitRequest<FilesCommittable>> commitRequests,
      SnapshotUpdate<?> operation,
      int numDataFiles,
      int numDeleteFiles,
      String description,
      String newFlinkJobId,
      long checkpointId) {
    try {
      LOG.info(
          "Committing {} with {} data files and {} delete files to table {}",
          description,
          numDataFiles,
          numDeleteFiles,
          table);
      // custom snapshot metadata properties will be overridden if they conflict with internal ones
      // used by the sink.
      snapshotProperties.forEach(operation::set);
      operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
      operation.set(FLINK_JOB_ID, newFlinkJobId);

      long start = System.currentTimeMillis();
      operation.commit(); // abort is automatically called if this fails.
      long duration = System.currentTimeMillis() - start;
      LOG.info("Committed in {} ms", duration);
    } catch (AlreadyExistsException | UnsupportedOperationException e) {
      commitRequests.forEach(request -> request.signalFailedWithKnownReason(e));
    } catch (Exception e) {
      commitRequests.forEach(request -> request.signalFailedWithUnknownReason(e));
    }
  }

  static long getMaxCommittedCheckpointId(Table table, String flinkJobId) {
    return table.history().stream()
        .map(history -> table.snapshot(history.snapshotId()).summary())
        .filter(summary -> StringUtils.equals(summary.get(FLINK_JOB_ID), flinkJobId))
        .findFirst()
        .map(summary -> Long.parseLong(summary.get(MAX_COMMITTED_CHECKPOINT_ID)))
        .orElse(INITIAL_CHECKPOINT_ID);
  }
}
