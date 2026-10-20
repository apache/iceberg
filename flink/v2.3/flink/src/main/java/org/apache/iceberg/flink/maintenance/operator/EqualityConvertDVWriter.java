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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.roaringbitmap.longlong.Roaring64Bitmap;
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
  private transient DeleteLoader deleteLoader;
  private transient Map<String, FilePositions> positionsByFile;
  private transient EqualityConvertPlan planResult;
  private transient boolean hasUpstreamError;
  private transient int manifestsRead;

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
    deleteLoader = new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile));
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
          .positions
          .addLong(pos.position());
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

    Map<String, DeleteFile> dvs = collectExistingDVs(mainSnapshot, positionsByFile.keySet());

    // Fold staging DVs into the rewrite so the writer emits one DV per data file (V3 rule). Flink
    // writes a staging DV only for a newly added data file, so it never collides with a distinct
    // existing DV: on a separate target branch collectExistingDVs has not seen it yet; on a shared
    // branch it IS that existing DV, so the put is idempotent.
    for (DeleteFile sd : planResult.stagingDVFiles()) {
      if (ContentFileUtil.isDV(sd) && sd.referencedDataFile() != null) {
        dvs.put(sd.referencedDataFile(), sd);
      }
    }

    BaseDVFileWriter dvWriter =
        new BaseDVFileWriter(fileFactory, path -> loadPreviousDV(path, dvs));
    try (dvWriter) {
      for (Map.Entry<String, FilePositions> entry : positionsByFile.entrySet()) {
        String dataFilePath = entry.getKey();
        FilePositions filePositions = entry.getValue();
        PartitionSpec spec = table.specs().get(filePositions.specId);
        StructLike partition = filePositions.partition(spec.partitionType());

        filePositions.positions.forEach(
            (long pos) -> dvWriter.delete(dataFilePath, pos, spec, partition));
      }
    }

    DeleteWriteResult result = dvWriter.result();
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

  private Map<String, DeleteFile> collectExistingDVs(
      Snapshot mainSnapshot, Set<String> affectedPaths) {
    manifestsRead = 0;
    Map<String, DeleteFile> dvs = Maps.newHashMap();
    if (mainSnapshot == null) {
      return dvs;
    }

    // Prune delete manifests whose partition summaries cannot cover the cycle's affected
    // partitions. A DV inherits its referenced data file's spec and partition, so partition pruning
    // works for DV manifests.
    Map<Integer, ManifestEvaluator> evaluators = partitionEvaluators(positionsByFile);
    for (ManifestFile manifest : mainSnapshot.deleteManifests(table.io())) {
      ManifestEvaluator evaluator = evaluators.get(manifest.partitionSpecId());
      if (evaluator == null || !evaluator.eval(manifest)) {
        continue;
      }

      readDVEntries(manifest, affectedPaths, dvs);
    }

    return dvs;
  }

  private Map<Integer, ManifestEvaluator> partitionEvaluators(
      Map<String, FilePositions> positions) {
    Map<Integer, StructLikeWrapper> templatesBySpec = Maps.newHashMap();
    Map<Integer, Set<StructLikeWrapper>> partitionsBySpec = Maps.newHashMap();
    for (FilePositions filePositions : positions.values()) {
      PartitionSpec spec = table.specs().get(filePositions.specId);
      StructLikeWrapper template =
          templatesBySpec.computeIfAbsent(
              filePositions.specId, id -> StructLikeWrapper.forType(spec.partitionType()));
      partitionsBySpec
          .computeIfAbsent(filePositions.specId, k -> Sets.newHashSet())
          .add(template.copyFor(filePositions.partition(spec.partitionType())));
    }

    Map<Integer, ManifestEvaluator> evaluators = Maps.newHashMap();
    for (Map.Entry<Integer, Set<StructLikeWrapper>> entry : partitionsBySpec.entrySet()) {
      PartitionSpec spec = table.specs().get(entry.getKey());
      Expression filter = partitionFilter(spec, entry.getValue());
      evaluators.put(entry.getKey(), ManifestEvaluator.forPartitionFilter(filter, spec, false));
    }

    return evaluators;
  }

  private static Expression partitionFilter(PartitionSpec spec, Set<StructLikeWrapper> partitions) {
    List<PartitionField> fields = spec.fields();
    Expression anyPartition = Expressions.alwaysFalse();
    for (StructLikeWrapper wrapper : partitions) {
      StructLike partition = wrapper.get();
      Expression onePartition = Expressions.alwaysTrue();
      for (int i = 0; i < fields.size(); i++) {
        String name = fields.get(i).name();
        Object value = partition.get(i, Object.class);
        Expression predicate =
            value == null ? Expressions.isNull(name) : Expressions.equal(name, value);
        onePartition = Expressions.and(onePartition, predicate);
      }

      anyPartition = Expressions.or(anyPartition, onePartition);
    }

    return anyPartition;
  }

  private void readDVEntries(
      ManifestFile manifest, Set<String> filterPaths, Map<String, DeleteFile> out) {
    manifestsRead++;
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
      for (DeleteFile deleteFile : reader) {
        if (ContentFileUtil.isDV(deleteFile)
            && deleteFile.referencedDataFile() != null
            && filterPaths.contains(deleteFile.referencedDataFile())) {
          out.put(deleteFile.referencedDataFile(), deleteFile);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read manifest: " + manifest.path(), e);
    }
  }

  @VisibleForTesting
  int manifestsReadLastCycle() {
    return manifestsRead;
  }

  @VisibleForTesting
  int retainedStateSize() {
    return positionsByFile.size();
  }

  private PositionDeleteIndex loadPreviousDV(String dataFilePath, Map<String, DeleteFile> dvs) {
    DeleteFile existingDV = dvs.get(dataFilePath);
    if (existingDV == null) {
      return null;
    }

    return deleteLoader.loadPositionDeletes(ImmutableList.of(existingDV), dataFilePath);
  }

  private static final class FilePositions {
    private final int specId;
    private final byte[] encodedPartition;
    private final Roaring64Bitmap positions = new Roaring64Bitmap();
    private StructLike decodedPartition;

    FilePositions(int specId, byte[] encodedPartition) {
      this.specId = specId;
      this.encodedPartition = encodedPartition;
    }

    StructLike partition(Types.StructType partitionType) {
      if (decodedPartition == null) {
        decodedPartition = StructLikeSerializer.decodePartition(encodedPartition, partitionType);
      }

      return decodedPartition;
    }
  }
}
