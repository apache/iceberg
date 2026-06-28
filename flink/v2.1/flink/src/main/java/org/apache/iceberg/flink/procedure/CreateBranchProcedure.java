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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Create branch procedure for given tag. Usage:
 *
 * <p>Create branch from the current snapshot:
 *
 * <pre><code>
 *  CALL sys.create_branch('tableId', 'branchName')
 * </code></pre>
 *
 * Create branch from the tagged snapshot:
 *
 * <pre><code>
 *  CALL sys.create_branch('tableId', 'branchName', 'tagName')
 * </code></pre>
 */
public class CreateBranchProcedure extends ProcedureBase {
  public static final String PROCEDURE_NAME = "create_branch";

  @ProcedureHint(
      arguments = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "branch", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "tag", type = @DataTypeHint("STRING"), isOptional = true)
      })
  public String[] call(
      ProcedureContext procedureContext, String tableId, String branchName, String tagName)
      throws NoSuchTableException {
    Table table = table(tableId);
    table.refresh();

    createBranch(table, branchName, tagName);

    return new String[] {"Success"};
  }

  private void createBranch(Table table, String branchName, String tagName)
      throws NoSuchTableException {

    if (tagName == null || tagName.isEmpty()) {
      table.manageSnapshots().createBranch(branchName).commit();
    } else {
      Snapshot snapshot = table.snapshot(tagName);
      table.manageSnapshots().createBranch(branchName, snapshot.snapshotId()).commit();
    }
  }

  @Override
  public String procedureName() {
    return PROCEDURE_NAME;
  }
}
