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
 * Rollback to timestamp procedure. Usage:
 *
 * <pre><code>
 *  -- rollback to the snapshot which earlier or equal than timestamp.
 *  CALL sys.rollback_to_timestamp(`table` => 'tableId', timestamp => timestamp)
 * </code></pre>
 */
public class RollbackToTimestampProcedure extends ProcedureBase {
  public static final String PROCEDURE_NAME = "rollback_to_timestamp";

  @ProcedureHint(
      arguments = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "timestamp", type = @DataTypeHint("BIGINT"))
      })
  public @DataTypeHint("ROW<previous_snapshot_id BIGINT, current_snapshot_id BIGINT>") Row[] call(
      ProcedureContext procedureContext, String tableId, Long timestamp) throws Exception {
    Table table = table(tableId);
    table.refresh();

    Long previousSnapshotId = table.currentSnapshot().snapshotId();
    Long snapshotId = rollbackToTimestamp(table, timestamp);

    return new Row[] {Row.of(previousSnapshotId, snapshotId)};
  }

  private Long rollbackToTimestamp(Table table, Long timestamp) {
    Snapshot snapshot = getLastSnapshotEarlierOrEqualThanTimestamp(table, timestamp);

    if (snapshot == null) {
      throw new IllegalArgumentException(
          "Could not find any snapshot whose commit-time earlier than " + timestamp);
    }

    long snapshotId = snapshot.snapshotId();

    table.manageSnapshots().rollbackTo(snapshotId).commit();

    return snapshotId;
  }

  private Snapshot getLastSnapshotEarlierOrEqualThanTimestamp(Table table, Long timestamp) {
    Iterable<Snapshot> snapshots = table.snapshots();
    Snapshot target = null;
    for (Snapshot snapshot : snapshots) {
      if (snapshot.timestampMillis() <= timestamp) {
        if (target == null || target.timestampMillis() <= snapshot.timestampMillis()) {
          target = snapshot;
        }
      }
    }

    return target;
  }

  @Override
  public String procedureName() {
    return PROCEDURE_NAME;
  }
}
