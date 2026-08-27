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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.FlinkReadOptions;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkSourceConfig extends TableSourceTestBase {
  private static final String TABLE = "test_table";

  @TestTemplate
  public void testFlinkSessionConfig() {
    getTableEnv().getConfig().set(FlinkReadOptions.STREAMING_OPTION, true);
    assertThatThrownBy(() -> sql("SELECT * FROM %s /*+ OPTIONS('as-of-timestamp'='1')*/", TABLE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set as-of-timestamp option for streaming reader");
  }

  @TestTemplate
  public void testFlinkHintConfig() {
    List<Row> result =
        sql(
            "SELECT * FROM %s /*+ OPTIONS('as-of-timestamp'='%d','streaming'='false')*/",
            TABLE, System.currentTimeMillis());
    assertThat(result).hasSize(3);
  }

  @TestTemplate
  public void testReadOptionHierarchy() {
    getTableEnv().getConfig().set(FlinkReadOptions.LIMIT_OPTION, 1L);
    List<Row> result = sql("SELECT * FROM %s", TABLE);
    // Note that this query doesn't have the limit clause in the SQL.
    // This assertions works because limit is pushed down to the reader and
    // reader parallelism is 1.
    assertThat(result).hasSize(1);

    result = sql("SELECT * FROM %s /*+ OPTIONS('limit'='3')*/", TABLE);
    assertThat(result).hasSize(3);
  }
}
