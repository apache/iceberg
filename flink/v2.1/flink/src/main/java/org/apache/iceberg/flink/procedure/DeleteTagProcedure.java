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
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Delete tag procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.delete_tag('tableId', 'tagName')
 * </code></pre>
 */
public class DeleteTagProcedure extends ProcedureBase {
  public static final String PROCEDURE_NAME = "delete_tag";

  @ProcedureHint(
      arguments = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "tag", type = @DataTypeHint("STRING"))
      })
  public String[] call(ProcedureContext procedureContext, String tableId, String tagNameStr)
      throws NoSuchTableException {
    Table table = table(tableId);
    table.refresh();
    deleteTag(table, tagNameStr);
    return new String[] {"Success"};
  }

  private void deleteTag(Table table, String tagNameStr) {
    table.manageSnapshots().removeTag(tagNameStr).commit();
  }

  @Override
  public String procedureName() {
    return PROCEDURE_NAME;
  }
}
