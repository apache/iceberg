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

import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class BaseOverwriteFiles extends MergingSnapshotProducer<OverwriteFiles>
    implements OverwriteFiles {
  private final Set<DataFile> deletedDataFiles = Sets.newHashSet();
  private boolean validateAddedFilesMatchOverwriteFilter = false;
  private Long startingSnapshotId = null;
  private Expression conflictDetectionFilter = null;
  private boolean validateNewDataFiles = false;
  private boolean validateNewDeletes = false;

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
    deletedDataFiles.add(file);
    delete(file);
    return this;
  }

  @Override
  public OverwriteFiles validateAddedFilesMatchOverwriteFilter() {
    this.validateAddedFilesMatchOverwriteFilter = true;
    return this;
  }

  @Override
  public OverwriteFiles validateFromSnapshot(long snapshotId) {
    this.startingSnapshotId = snapshotId;
    return this;
  }

  @Override
  public OverwriteFiles conflictDetectionFilter(Expression newConflictDetectionFilter) {
    Preconditions.checkArgument(
        newConflictDetectionFilter != null, "Conflict detection filter cannot be null");
    this.conflictDetectionFilter = newConflictDetectionFilter;
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingData() {
    this.validateNewDataFiles = true;
    failMissingDeletePaths();
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingDeletes() {
    this.validateNewDeletes = true;
    failMissingDeletePaths();
    return this;
  }

  @Override
  public BaseOverwriteFiles toBranch(String branch) {
    targetBranch(branch);
    return this;
  }

  @Override
  protected void validate(TableMetadata base, Snapshot parent) {
    if (validateAddedFilesMatchOverwriteFilter) {
      PartitionSpec spec = dataSpec();
      Expression rowFilter = rowFilter();

      Expression inclusiveExpr = Projections.inclusive(spec).project(rowFilter);
      Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);

      Expression strictExpr = Projections.strict(spec).project(rowFilter);
      Evaluator strict = new Evaluator(spec.partitionType(), strictExpr);

      StrictMetricsEvaluator metrics =
          new StrictMetricsEvaluator(base.schema(), rowFilter, isCaseSensitive());

      for (DataFile file : addedDataFiles()) {
        // the real test is that the strict or metrics test matches the file, indicating that all
        // records in the file match the filter. inclusive is used to avoid testing the metrics,
        // which is more complicated
        ValidationException.check(
            inclusive.eval(file.partition())
                && (strict.eval(file.partition()) || metrics.eval(file)),
            "Cannot append file with rows that do not match filter: %s: %s",
            rowFilter,
            file.path());
      }
    }

    if (validateNewDataFiles) {
      validateAddedDataFiles(base, startingSnapshotId, dataConflictDetectionFilter(), parent);
    }

    if (validateNewDeletes) {
      if (rowFilter() != Expressions.alwaysFalse()) {
        Expression filter = conflictDetectionFilter != null ? conflictDetectionFilter : rowFilter();
        validateNoNewDeleteFiles(base, startingSnapshotId, filter, parent);
        validateDeletedDataFiles(base, startingSnapshotId, filter, parent);
      }

      if (!deletedDataFiles.isEmpty()) {
        validateNoNewDeletesForDataFiles(
            base, startingSnapshotId, conflictDetectionFilter, deletedDataFiles, parent);
      }
    }
  }

  private Expression dataConflictDetectionFilter() {
    if (conflictDetectionFilter != null) {
      return conflictDetectionFilter;
    } else if (rowFilter() != Expressions.alwaysFalse() && deletedDataFiles.isEmpty()) {
      return rowFilter();
    } else {
      return Expressions.alwaysTrue();
    }
  }
}
