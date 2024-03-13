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
package org.apache.iceberg.mr.hive;

import java.io.Serializable;
import java.util.stream.Stream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class HiveIcebergGenTokenHook implements ExecuteWithHookContext {
  private static final String ICEBERG_HIVE_METASTORE_TOKEN = "iceberg.hive.metastore.token";
  private static final String ICEBERG_SERDE = "org.apache.iceberg.mr.hive.HiveIcebergSerDe";

  @Override
  public void run(HookContext hookContext) {
    HiveConf conf = hookContext.getConf();
    if (!conf.get(ICEBERG_HIVE_METASTORE_TOKEN, "").isEmpty()
        || hookContext.isHiveServerQuery()
        || hookContext.getOperationName() == null) {
      return;
    }

    QueryPlan qp = hookContext.getQueryPlan();
    Stream<WriteEntity> writeEntirys =
        qp.getOutputs().stream()
            .filter(
                output -> {
                  return output.getTable() != null
                      && output
                          .getTable()
                          .getSd()
                          .getSerdeInfo()
                          .getSerializationLib()
                          .equals(ICEBERG_SERDE);
                });
    Stream<Task<? extends Serializable>> tasks =
        qp.getRootTasks().stream()
            .filter(
                task -> {
                  return MapRedTask.class.isInstance(task);
                });

    if (writeEntirys.count() == 0 || tasks.count() == 0) {
      return;
    }

    Hive db;
    String token;
    try {
      db = Hive.get(hookContext.getConf());
      token = db.getDelegationToken(hookContext.getUserName(), hookContext.getUgi().getUserName());
    } catch (HiveException e) {
      // ignore
      db = null;
      return;
    }
    if (token != null && !token.isEmpty()) {
      hookContext.getConf().set(ICEBERG_HIVE_METASTORE_TOKEN, token);
    }
  }
}
