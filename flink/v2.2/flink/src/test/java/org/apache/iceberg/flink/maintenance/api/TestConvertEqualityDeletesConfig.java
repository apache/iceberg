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
package org.apache.iceberg.flink.maintenance.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.operator.OperatorTestBase;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestConvertEqualityDeletesConfig extends OperatorTestBase {
  private Table table;
  private final Map<String, String> input = Maps.newHashMap();

  @BeforeEach
  public void before() {
    this.table = createTable();
    input.put(ConvertEqualityDeletesConfig.TARGET_BRANCH, "myTarget");
    input.put(ConvertEqualityDeletesConfig.SCHEDULE_ON_COMMIT_COUNT, "5");
    input.put(ConvertEqualityDeletesConfig.SCHEDULE_ON_INTERVAL_SECOND, "120");
    input.put("other.config", "should-be-ignored");
  }

  @AfterEach
  public void after() {
    input.clear();
  }

  @Test
  void testConfigParsing() {
    ConvertEqualityDeletesConfig config =
        new ConvertEqualityDeletesConfig(table, input, new Configuration());

    assertThat(config.targetBranch()).isEqualTo("myTarget");
    assertThat(config.scheduleOnCommitCount()).isEqualTo(5);
    assertThat(config.scheduleOnIntervalSecond()).isEqualTo(120L);
  }

  @Test
  void testConfigDefaults() {
    ConvertEqualityDeletesConfig config =
        new ConvertEqualityDeletesConfig(table, Maps.newHashMap(), new Configuration());

    assertThat(config.targetBranch()).isNull();
    assertThat(config.scheduleOnCommitCount()).isEqualTo(1);
    assertThat(config.scheduleOnIntervalSecond()).isNull();
  }
}
