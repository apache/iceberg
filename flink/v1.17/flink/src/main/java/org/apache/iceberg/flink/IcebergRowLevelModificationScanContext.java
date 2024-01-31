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
package org.apache.iceberg.flink;

import java.io.Serializable;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;

public abstract class IcebergRowLevelModificationScanContext
    implements RowLevelModificationScanContext, Serializable {
  private final SupportsRowLevelModificationScan.RowLevelModificationType type;
  private final RowLevelOperationMode mode;
  private final Long snapshotId;
  private final PartitionSpec spec;
  protected Expression conflictDetectionFilter;

  private IcebergRowLevelModificationScanContext(
      SupportsRowLevelModificationScan.RowLevelModificationType type,
      RowLevelOperationMode mode,
      Long snapshotId,
      PartitionSpec spec) {
    this.type = type;
    this.mode = mode;
    this.snapshotId = snapshotId;
    this.spec = spec;
    this.conflictDetectionFilter = Expressions.alwaysTrue();
  }

  public static IcebergRowLevelModificationScanContext of(
      SupportsRowLevelModificationScan.RowLevelModificationType type,
      RowLevelOperationMode mode,
      Long snapshotId,
      PartitionSpec spec) {
    if (mode == RowLevelOperationMode.COPY_ON_WRITE) {
      return new IcebergRowLevelModificationScanContext.IcebergRowLevelCowScanContext(
          type, mode, snapshotId, spec);
    } else {
      throw new UnsupportedOperationException("Unsupported row-level operation mode: " + mode);
    }
  }

  public long snapshotId() {
    return snapshotId;
  }

  public Expression conflictDetectionFilter() {
    return conflictDetectionFilter;
  }

  public SupportsRowLevelModificationScan.RowLevelModificationType type() {
    return type;
  }

  public RowLevelOperationMode mode() {
    return mode;
  }

  public PartitionSpec spec() {
    return spec;
  }

  public abstract Expression applyFilter(Expression filter);

  public void setConflictDetectionFilter(Expression filter) {
    this.conflictDetectionFilter = filter;
  }

  public IcebergRowLevelCowScanContext asCowContext() {
    return (IcebergRowLevelCowScanContext) this;
  }

  public static class IcebergRowLevelCowScanContext extends IcebergRowLevelModificationScanContext {

    private Expression overwritePartitions;

    private IcebergRowLevelCowScanContext(
        SupportsRowLevelModificationScan.RowLevelModificationType type,
        RowLevelOperationMode mode,
        Long snapshotId,
        PartitionSpec spec) {
      super(type, mode, snapshotId, spec);
      this.overwritePartitions = Expressions.alwaysTrue();
    }

    public Expression overwritePartitions() {
      return overwritePartitions;
    }

    public void setOverwritePartitions(Expression filter) {
      this.overwritePartitions = filter;
    }

    @Override
    public Expression applyFilter(Expression filter) {
      switch (type()) {
        case UPDATE:
          overwritePartitions = Projections.inclusive(spec()).project(filter);
          conflictDetectionFilter = filter;
          return filter;
        case DELETE:
          // In COW, the filter provided by Flink will match records that should not be deleted.
          // This can inefficiently match all partitions in the table,
          // so we need further partition pruning to avoid reading partitions that don't contain
          // records to delete.
          // Also, work need to be done on the sink side to ensure these pruned partitions don't
          // get overwritten.
          Expression prunePartitions = Projections.strict(spec()).project(filter).negate();
          conflictDetectionFilter = Expressions.and(filter, prunePartitions);
          overwritePartitions = prunePartitions;
          return Expressions.and(filter, prunePartitions);
        default:
          throw new UnsupportedOperationException(
              "Unsupported row level modification type: " + type());
      }
    }
  }
}
