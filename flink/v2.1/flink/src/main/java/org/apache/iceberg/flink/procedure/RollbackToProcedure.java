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
package org.apache.iceberg.flink.procedure;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * Rollback procedure. Usage:
 *
 * <pre><code>
 *  -- rollback to a snapshot
 *  CALL sys.rollback_to(`table` => 'tableId', snapshot_id => snapshotId)
 *
 *  -- rollback to a tag
 *  CALL sys.rollback_to(`table` => 'tableId', tag => 'tagName')
 * </code></pre>
 */
public class RollbackToProcedure extends ProcedureBase {
  public static final String PROCEDURE_NAME = "rollback_to";

  @ProcedureHint(
      arguments = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "tag", type = @DataTypeHint("STRING"), isOptional = true),
        @ArgumentHint(name = "snapshot_id", type = @DataTypeHint("BIGINT"), isOptional = true)
      })
  public @DataTypeHint("ROW<previous_snapshot_id BIGINT, current_snapshot_id BIGINT>") Row[] call(
      ProcedureContext procedureContext, String tableId, String tagName, Long snapshotId)
      throws Exception {
    Table table = table(tableId);
    table.refresh();

    Long requiredSnapshotId = snapshotId;

    if (requiredSnapshotId != null && tagName != null) {
      checkIfCompatible(table, tagName, requiredSnapshotId);
    }

    Long previousSnapshotId = table.currentSnapshot().snapshotId();
    if (requiredSnapshotId == null) {
      if (tagName == null) {
        throw new IllegalArgumentException(
            "No arguments to rollback to. Please specify a tag or a snapshot id");
      }
      Snapshot snapshot = table.snapshot(tagName);
      requiredSnapshotId = snapshot.snapshotId();
    }
    rollbackTo(table, requiredSnapshotId);
    return new Row[] {Row.of(previousSnapshotId, requiredSnapshotId)};
  }

  private void checkIfCompatible(Table table, String tagName, Long snapshotId) {
    Snapshot tagSnapshot = table.snapshot(tagName);
    if (snapshotId != tagSnapshot.snapshotId()) {
      throw new IllegalArgumentException(
          "Snapshot with provided snapshot id is not the same snapshot provided tag refers to. Please specify a tag or a snapshot id, or be sure both refer to the same snapshot");
    }
  }

  private void rollbackTo(Table table, Long snapshotId) {
    table.manageSnapshots().rollbackTo(snapshotId).commit();
  }

  @Override
  public String procedureName() {
    return PROCEDURE_NAME;
  }
}
