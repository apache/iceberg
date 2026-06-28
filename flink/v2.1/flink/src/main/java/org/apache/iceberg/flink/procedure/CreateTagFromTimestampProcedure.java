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

import java.time.Duration;
import javax.annotation.Nullable;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Create a tag for the first snapshot whose commit-time greater than the specified timestamp.
 * Usage:
 *
 * <pre><code>
 *  CALL sys.create_tag_from_timestamp('tableId', 'tagName', timestamp, 'timeRetained')
 * </code></pre>
 */
public class CreateTagFromTimestampProcedure extends ProcedureBase {
  public static final String PROCEDURE_NAME = "create_tag_from_timestamp";

  @ProcedureHint(
      arguments = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "tag", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "timestamp", type = @DataTypeHint("BIGINT")),
        @ArgumentHint(name = "time_retained", type = @DataTypeHint("STRING"), isOptional = true)
      })
  @DataTypeHint("ROW< tagName STRING, snapshot BIGINT, `commit_time` BIGINT>")
  public Row[] call(
      ProcedureContext procedureContext,
      String tableId,
      String tagName,
      Long timestamp,
      @Nullable String timeRetained)
      throws NoSuchTableException {
    Table table = table(tableId);
    table.refresh();
    Snapshot snapshot = createTagFromTimestamp(table, tagName, timestamp, toDuration(timeRetained));
    return new Row[] {Row.of(tagName, snapshot.snapshotId(), snapshot.timestampMillis())};
  }

  private Snapshot createTagFromTimestamp(
      Table table, String tagName, Long timestamp, Duration timeRetained) {
    Snapshot snapshot = getFirstSnapshotGreaterThanTimestamp(table, timestamp);

    if (snapshot == null) {
      throw new IllegalArgumentException(
          "Could not find any snapshot whose commit-time later than " + timestamp);
    }

    if (timeRetained == null) {
      table.manageSnapshots().createTag(tagName, snapshot.snapshotId()).commit();
    } else {
      table
          .manageSnapshots()
          .createTag(tagName, snapshot.snapshotId())
          .setMaxRefAgeMs(tagName, timeRetained.toMillis())
          .commit();
    }

    return snapshot;
  }

  private Snapshot getFirstSnapshotGreaterThanTimestamp(Table table, Long timestamp) {
    Iterable<Snapshot> snapshots = table.snapshots();
    Snapshot target = null;
    for (Snapshot snapshot : snapshots) {
      if (snapshot.timestampMillis() > timestamp) {
        if (target == null || target.timestampMillis() >= snapshot.timestampMillis()) {
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
