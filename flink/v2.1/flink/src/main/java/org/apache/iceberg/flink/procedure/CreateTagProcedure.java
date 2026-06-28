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
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Create tag procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.create_tag('tableId', 'tagName', snapshotId, 'timeRetained')
 * </code></pre>
 */
public class CreateTagProcedure extends ProcedureBase {
  public static final String PROCEDURE_NAME = "create_tag";

  @ProcedureHint(
      arguments = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "tag", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "snapshot_id", type = @DataTypeHint("BIGINT"), isOptional = true),
        @ArgumentHint(name = "time_retained", type = @DataTypeHint("STRING"), isOptional = true)
      })
  public String[] call(
      ProcedureContext procedureContext,
      String tableId,
      String tagName,
      @Nullable Long snapshotId,
      @Nullable String timeRetained)
      throws NoSuchTableException {
    Table table = table(tableId);
    table.refresh();
    createTag(table, tagName, snapshotId, toDuration(timeRetained));
    return new String[] {"Success"};
  }

  void createTag(Table table, String tagName, Long snapshotId, Duration timeRetained) {
    Long requiredSnapshotId = snapshotId;
    if (requiredSnapshotId == null) {
      requiredSnapshotId = table.currentSnapshot().snapshotId();
    }

    if (timeRetained == null) {
      table.manageSnapshots().createTag(tagName, requiredSnapshotId).commit();
    } else {
      table
          .manageSnapshots()
          .createTag(tagName, requiredSnapshotId)
          .setMaxRefAgeMs(tagName, timeRetained.toMillis())
          .commit();
    }
  }

  @Override
  public String procedureName() {
    return PROCEDURE_NAME;
  }
}
