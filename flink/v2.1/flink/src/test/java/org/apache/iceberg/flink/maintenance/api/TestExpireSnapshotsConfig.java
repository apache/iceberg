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
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestExpireSnapshotsConfig extends OperatorTestBase {
  private Table table;
  private Map<String, String> input = Maps.newHashMap();

  @BeforeEach
  public void before() {
    this.table = createTable();
    input.put(ExpireSnapshotsConfig.SCHEDULE_ON_COMMIT_COUNT, "10");
    input.put(ExpireSnapshotsConfig.SCHEDULE_ON_INTERVAL_SECOND, "60");
    input.put(ExpireSnapshotsConfig.MAX_SNAPSHOT_AGE_SECONDS, "7200");
    input.put(ExpireSnapshotsConfig.RETAIN_LAST, "5");
    input.put(ExpireSnapshotsConfig.DELETE_BATCH_SIZE, "500");
    input.put(ExpireSnapshotsConfig.CLEAN_EXPIRED_METADATA, "true");
    input.put(ExpireSnapshotsConfig.PLANNING_WORKER_POOL_SIZE, "4");
    input.put("other.config", "should-be-ignored");
  }

  @AfterEach
  public void after() {
    input.clear();
  }

  @Test
  void testConfigParsing() {
    ExpireSnapshotsConfig config = new ExpireSnapshotsConfig(table, input, new Configuration());

    assertThat(config.scheduleOnCommitCount()).isEqualTo(10);
    assertThat(config.scheduleOnIntervalSecond()).isEqualTo(60);
    assertThat(config.maxSnapshotAgeSeconds()).isEqualTo(7200L);
    assertThat(config.retainLast()).isEqualTo(5);
    assertThat(config.deleteBatchSize()).isEqualTo(500);
    assertThat(config.cleanExpiredMetadata()).isTrue();
    assertThat(config.planningWorkerPoolSize()).isEqualTo(4);
  }

  @Test
  void testConfigDefaults() {
    ExpireSnapshotsConfig config =
        new ExpireSnapshotsConfig(table, Maps.newHashMap(), new Configuration());

    assertThat(config.scheduleOnCommitCount())
        .isEqualTo(ExpireSnapshotsConfig.SCHEDULE_ON_COMMIT_COUNT_OPTION.defaultValue());
    assertThat(config.scheduleOnIntervalSecond())
        .isEqualTo(ExpireSnapshotsConfig.SCHEDULE_ON_INTERVAL_SECOND_OPTION.defaultValue());
    assertThat(config.maxSnapshotAgeSeconds()).isNull();
    assertThat(config.retainLast()).isNull();
    assertThat(config.deleteBatchSize())
        .isEqualTo(ExpireSnapshotsConfig.DELETE_BATCH_SIZE_OPTION.defaultValue());
    assertThat(config.cleanExpiredMetadata()).isTrue();
    assertThat(config.planningWorkerPoolSize()).isEqualTo(ThreadPools.WORKER_THREAD_POOL_SIZE);
  }
}
