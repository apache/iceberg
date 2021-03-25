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

package org.apache.iceberg.flink.source;

import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.flink.FlinkTestBase;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;

public class ChangeLogTableTestBase extends FlinkTestBase {
  private volatile TableEnvironment tEnv = null;

  @Rule
  public TestName name = new TestName();

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s", name.getMethodName());
    BoundedTableFactory.clearDataSets();
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings settings = EnvironmentSettings
              .newInstance()
              .useBlinkPlanner()
              .inStreamingMode()
              .build();

          StreamExecutionEnvironment env = StreamExecutionEnvironment
              .getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
              .enableCheckpointing(400)
              .setMaxParallelism(1)
              .setParallelism(1);

          tEnv = StreamTableEnvironment.create(env, settings);
        }
      }
    }
    return tEnv;
  }

  private static final Map<String, RowKind> ROW_KIND_MAP = ImmutableMap.of(
      "+I", RowKind.INSERT,
      "-D", RowKind.DELETE,
      "-U", RowKind.UPDATE_BEFORE,
      "+U", RowKind.UPDATE_AFTER);

  protected Row row(String rowKind, int id, String data) {
    RowKind kind = ROW_KIND_MAP.get(rowKind);
    if (kind == null) {
      throw new IllegalArgumentException("Unknown row kind: " + rowKind);
    }

    return Row.ofKind(kind, id, data);
  }

  protected static <T> List<T> listJoin(List<List<T>> lists) {
    List<T> result = Lists.newArrayList();
    for (List<T> list : lists) {
      result.addAll(list);
    }
    return result;
  }
}
