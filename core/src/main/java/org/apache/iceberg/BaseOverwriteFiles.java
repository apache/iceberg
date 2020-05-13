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

package org.apache.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;

public class BaseOverwriteFiles extends MergingSnapshotProducer<OverwriteFiles> implements OverwriteFiles {
  private boolean validateAddedFilesMatchOverwriteFilter = false;
  private Long readSnapshotId = null;
  private Expression conflictDetectionFilter = null;

  protected BaseOverwriteFiles(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected OverwriteFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.OVERWRITE;
  }

  @Override
  public OverwriteFiles overwriteByRowFilter(Expression expr) {
    deleteByRowFilter(expr);
    return this;
  }

  @Override
  public OverwriteFiles addFile(DataFile file) {
    add(file);
    return this;
  }

  @Override
  public OverwriteFiles deleteFile(DataFile file) {
    delete(file);
    return this;
  }

  @Override
  public OverwriteFiles validateAddedFilesMatchOverwriteFilter() {
    this.validateAddedFilesMatchOverwriteFilter = true;
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingAppends(Long newReadSnapshotId, Expression newConflictDetectionFilter) {
    Preconditions.checkArgument(newConflictDetectionFilter != null, "Conflict detection filter cannot be null");
    this.readSnapshotId = newReadSnapshotId;
    this.conflictDetectionFilter = newConflictDetectionFilter;
    failMissingDeletePaths();
    return this;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    if (validateAddedFilesMatchOverwriteFilter) {
      PartitionSpec spec = writeSpec();
      Expression rowFilter = rowFilter();

      Expression inclusiveExpr = Projections.inclusive(spec).project(rowFilter);
      Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);

      Expression strictExpr = Projections.strict(spec).project(rowFilter);
      Evaluator strict = new Evaluator(spec.partitionType(), strictExpr);

      StrictMetricsEvaluator metrics = new StrictMetricsEvaluator(
          base.schema(), rowFilter);

      for (DataFile file : addedFiles()) {
        // the real test is that the strict or metrics test matches the file, indicating that all
        // records in the file match the filter. inclusive is used to avoid testing the metrics,
        // which is more complicated
        ValidationException.check(
            inclusive.eval(file.partition()) &&
                (strict.eval(file.partition()) || metrics.eval(file)),
            "Cannot append file with rows that do not match filter: %s: %s",
            rowFilter, file.path());
      }
    }

    if (conflictDetectionFilter != null) {
      PartitionSpec spec = writeSpec();
      Expression inclusiveExpr = Projections.inclusive(spec).project(conflictDetectionFilter);
      Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);

      InclusiveMetricsEvaluator metrics = new InclusiveMetricsEvaluator(base.schema(), conflictDetectionFilter);

      List<DataFile> newFiles = collectNewFiles(base);
      for (DataFile newFile : newFiles) {
        ValidationException.check(
            !inclusive.eval(newFile.partition()) || !metrics.eval(newFile),
            "A file was appended that might contain data matching filter '%s': %s",
            conflictDetectionFilter, newFile.path());
      }
    }

    return super.apply(base);
  }

  private List<DataFile> collectNewFiles(TableMetadata meta) {
    List<DataFile> newFiles = new ArrayList<>();

    Long currentSnapshotId = meta.currentSnapshot() == null ? null : meta.currentSnapshot().snapshotId();
    while (currentSnapshotId != null && !currentSnapshotId.equals(readSnapshotId)) {
      Snapshot currentSnapshot = meta.snapshot(currentSnapshotId);

      if (currentSnapshot == null) {
        throw new ValidationException(
            "Cannot determine history between read snapshot %s and current %s",
            readSnapshotId, currentSnapshotId);
      }

      Iterables.addAll(newFiles, currentSnapshot.addedFiles());
      currentSnapshotId = currentSnapshot.parentId();
    }

    return newFiles;
  }
}
