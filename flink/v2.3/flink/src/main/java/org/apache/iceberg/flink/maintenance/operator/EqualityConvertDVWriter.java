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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.IOException;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.DeletionVectorHelper.FilePositions;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keyed parallel resolver that buffers {@link DVPosition}s per data-file path, then writes Puffin
 * DV files directly via {@link BaseDVFileWriter}. Plan metadata arrives broadcast on input 2, so
 * every parallel task sees the cycle's metadata and can validate against the main snapshot.
 *
 * <p>Each buffered {@link DVPosition} carries the data file's {@code specId} + encoded partition,
 * so writing DVs needs no data-manifest scan. Existing DVs are folded into the rewrite (V3 allows
 * one DV per data file): delete manifests are pruned by partition summary to the cycle's affected
 * partitions, then filtered to entries referencing the affected data files. No cross-cycle state is
 * kept; reads are bounded by the pruned manifest set, not the table's full DV history.
 *
 * <p>Buffered positions are transient per-task. On failure recovery, upstream replay rebuilds them.
 */
@Internal
public class EqualityConvertDVWriter extends AbstractStreamOperator<DVWriteResult>
    implements TwoInputStreamOperator<DVPosition, EqualityConvertPlan, DVWriteResult> {

  private static final Logger LOG = LoggerFactory.getLogger(EqualityConvertDVWriter.class);

  private final String tableName;
  private final String taskName;
  private final TableLoader tableLoader;
  private final String targetBranch;

  private transient Table table;
  private transient OutputFileFactory fileFactory;
  private transient DeletionVectorHelper deletionVectorHelper;
  private transient Map<String, FilePositions> positionsByFile;
  private transient EqualityConvertPlan planResult;
  private transient boolean hasUpstreamError;

  public EqualityConvertDVWriter(
      String tableName, String taskName, TableLoader tableLoader, String targetBranch) {
    this.tableName = tableName;
    this.taskName = taskName;
    this.tableLoader = tableLoader;
    this.targetBranch = targetBranch;
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    table = tableLoader.loadTable();
    int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    fileFactory =
        OutputFileFactory.builderFor(table, subtaskIndex, 0L).format(FileFormat.PUFFIN).build();
    deletionVectorHelper = new DeletionVectorHelper(table);
    positionsByFile = Maps.newHashMap();
  }

  @Override
  public void processElement1(StreamRecord<DVPosition> record) {
    DVPosition pos = record.getValue();
    if (pos.isAbort()) {
      hasUpstreamError = true;
    }

    if (!hasUpstreamError) {
      positionsByFile
          .computeIfAbsent(
              pos.dataFilePath(), k -> new FilePositions(pos.specId(), pos.partition()))
          .add(pos.position());
    }
  }

  @Override
  public void processElement2(StreamRecord<EqualityConvertPlan> record) {
    planResult = record.getValue();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (planResult != null && mark.getTimestamp() >= planResult.doneTimestamp()) {
      if (hasUpstreamError) {
        output.collect(new StreamRecord<>(DVWriteResult.ABORT));
      } else {
        try {
          resolveAndWrite();
        } catch (Exception e) {
          LOG.error("Error writing DVs for table {} task {}", tableName, taskName, e);
          output.collect(TaskResultAggregator.ERROR_STREAM, new StreamRecord<>(e));
          output.collect(new StreamRecord<>(DVWriteResult.ABORT));
        }
      }

      positionsByFile.clear();
      hasUpstreamError = false;
      planResult = null;
    }

    super.processWatermark(mark);
  }

  private void resolveAndWrite() throws IOException {
    if (positionsByFile.isEmpty()) {
      return;
    }

    table.refresh();

    Snapshot mainSnapshot = table.snapshot(targetBranch);

    // Fail fast if the main branch changed since planning, to avoid writing DV files that the
    // committer would reject via validateFromSnapshot. The next cycle will reindex.
    if (mainSnapshot != null
        && planResult.mainSnapshotId() != null
        && mainSnapshot.snapshotId() != planResult.mainSnapshotId()) {
      throw new IllegalStateException(
          "Main branch snapshot changed since planning: expected "
              + planResult.mainSnapshotId()
              + " but found: "
              + mainSnapshot.snapshotId());
    }

    Map<String, DeleteFile> dvs =
        deletionVectorHelper.collectExistingDVs(mainSnapshot, positionsByFile);

    // Fold staging DVs into the rewrite so the writer emits one DV per data file (V3 rule). Flink
    // writes a staging DV only for a newly added data file, so it never collides with a distinct
    // existing DV: on a separate target branch collectExistingDVs has not seen it yet; on a shared
    // branch it IS that existing DV, so the put is idempotent.
    for (DeleteFile sd : planResult.stagingDVFiles()) {
      if (ContentFileUtil.isDV(sd) && sd.referencedDataFile() != null) {
        dvs.put(sd.referencedDataFile(), sd);
      }
    }

    DeleteWriteResult result = deletionVectorHelper.write(fileFactory, positionsByFile, dvs);
    LOG.info(
        "Wrote {} DV files (rewriting {}) for {} data files in table {} task {}.",
        result.deleteFiles().size(),
        result.rewrittenDeleteFiles().size(),
        positionsByFile.size(),
        tableName,
        taskName);

    output.collect(
        new StreamRecord<>(
            new DVWriteResult(
                Lists.newArrayList(result.deleteFiles()),
                Lists.newArrayList(result.rewrittenDeleteFiles()))));
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }

  @VisibleForTesting
  int manifestsReadLastCycle() {
    return deletionVectorHelper.manifestsReadLastLookup();
  }

  @VisibleForTesting
  int retainedStateSize() {
    return positionsByFile.size();
  }
}
