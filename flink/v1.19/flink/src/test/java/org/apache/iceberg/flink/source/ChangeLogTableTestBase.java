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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.TestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public class ChangeLogTableTestBase extends TestBase {
  private volatile TableEnvironment tEnv = null;

  protected String tableName;

  @BeforeEach
  public void setup(TestInfo testInfo) {
    assertThat(testInfo.getTestMethod()).isPresent();
    this.tableName = testInfo.getTestMethod().get().getName();
  }

  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    BoundedTableFactory.clearDataSets();
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings settings =
              EnvironmentSettings.newInstance().inStreamingMode().build();

          StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment(
                      MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
                  .enableCheckpointing(400)
                  .setMaxParallelism(1)
                  .setParallelism(1);

          tEnv = StreamTableEnvironment.create(env, settings);
        }
      }
    }
    return tEnv;
  }

  protected static Row insertRow(Object... values) {
    return Row.ofKind(RowKind.INSERT, values);
  }

  protected static Row deleteRow(Object... values) {
    return Row.ofKind(RowKind.DELETE, values);
  }

  protected static Row updateBeforeRow(Object... values) {
    return Row.ofKind(RowKind.UPDATE_BEFORE, values);
  }

  protected static Row updateAfterRow(Object... values) {
    return Row.ofKind(RowKind.UPDATE_AFTER, values);
  }

  protected static <T> List<T> listJoin(List<List<T>> lists) {
    return lists.stream().flatMap(List::stream).collect(Collectors.toList());
  }
}
