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

import java.util.TimeZone;
import javax.annotation.Nullable;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Procedure for expiring snapshots. Usage:
 *
 * <pre><code>
 *  CALL sys.expire_snapshots('tableId', retainLast, 'olderThan')
 * </code></pre>
 */
public class ExpireSnapshotsProcedure extends ProcedureBase {
  public static final String PROCEDURE_NAME = "expire_snapshots";

  @ProcedureHint(
      arguments = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "retain_last", type = @DataTypeHint("INTEGER"), isOptional = true),
        @ArgumentHint(
            name = "older_than",
            type = @DataTypeHint(value = "STRING"),
            isOptional = true)
      })
  public String[] call(
      ProcedureContext procedureContext, String tableId, Integer retainLast, String olderThanStr)
      throws NoSuchTableException {
    Table table = table(tableId);
    table.refresh();
    expireSnapshots(table, retainLast, toMillis(olderThanStr));
    return new String[] {"Success"};
  }

  private void expireSnapshots(Table table, Integer retainLast, Long olderThan) {
    ExpireSnapshots expireSnapshots = table.expireSnapshots();
    if (retainLast != null) {
      expireSnapshots = expireSnapshots.retainLast(retainLast);
    }
    if (olderThan != null) {
      expireSnapshots = expireSnapshots.expireOlderThan(olderThan);
    }
    expireSnapshots.commit();
  }

  @Nullable
  private Long toMillis(String olderThanStr) {
    if (olderThanStr == null || olderThanStr.isEmpty()) {
      return null;
    }

    return DateTimeUtils.parseTimestampData(olderThanStr, 3, TimeZone.getDefault())
        .getMillisecond();
  }

  @Override
  public String procedureName() {
    return PROCEDURE_NAME;
  }
}
