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
import org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode;
import org.apache.iceberg.flink.maintenance.operator.OperatorTestBase;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDeleteOrphanFilesConfig extends OperatorTestBase {
  private Table table;
  private Map<String, String> input = Maps.newHashMap();

  @BeforeEach
  public void before() {
    this.table = createTable();
    input.put(DeleteOrphanFilesConfig.SCHEDULE_ON_INTERVAL_SECOND, "60");
    input.put(DeleteOrphanFilesConfig.MIN_AGE_SECONDS, "86400");
    input.put(DeleteOrphanFilesConfig.DELETE_BATCH_SIZE, "500");
    input.put(DeleteOrphanFilesConfig.LOCATION, "/tmp/test-location");
    input.put(DeleteOrphanFilesConfig.USE_PREFIX_LISTING, "true");
    input.put(DeleteOrphanFilesConfig.PLANNING_WORKER_POOL_SIZE, "4");
    input.put(DeleteOrphanFilesConfig.EQUAL_SCHEMES, "s3n=s3,s3a=s3");
    input.put(DeleteOrphanFilesConfig.EQUAL_AUTHORITIES, "auth1=auth2");
    input.put(DeleteOrphanFilesConfig.PREFIX_MISMATCH_MODE, "IGNORE");
    input.put("other.config", "should-be-ignored");
  }

  @AfterEach
  public void after() {
    input.clear();
  }

  @Test
  void testConfigParsing() {
    DeleteOrphanFilesConfig config = new DeleteOrphanFilesConfig(table, input, new Configuration());

    assertThat(config.scheduleOnIntervalSecond()).isEqualTo(60);
    assertThat(config.minAgeSeconds()).isEqualTo(86400L);
    assertThat(config.deleteBatchSize()).isEqualTo(500);
    assertThat(config.location()).isEqualTo("/tmp/test-location");
    assertThat(config.usePrefixListing()).isTrue();
    assertThat(config.planningWorkerPoolSize()).isEqualTo(4);
    assertThat(config.equalSchemes()).containsEntry("s3n", "s3").containsEntry("s3a", "s3");
    assertThat(config.equalAuthorities()).containsEntry("auth1", "auth2");
    assertThat(config.prefixMismatchMode()).isEqualTo(PrefixMismatchMode.IGNORE);
  }

  @Test
  void testConfigDefaults() {
    DeleteOrphanFilesConfig config =
        new DeleteOrphanFilesConfig(table, Maps.newHashMap(), new Configuration());

    assertThat(config.scheduleOnIntervalSecond())
        .isEqualTo(DeleteOrphanFilesConfig.SCHEDULE_ON_INTERVAL_SECOND_OPTION.defaultValue());
    assertThat(config.minAgeSeconds())
        .isEqualTo(DeleteOrphanFilesConfig.MIN_AGE_SECONDS_OPTION.defaultValue());
    assertThat(config.deleteBatchSize())
        .isEqualTo(DeleteOrphanFilesConfig.DELETE_BATCH_SIZE_OPTION.defaultValue());
    assertThat(config.location()).isNull();
    assertThat(config.usePrefixListing()).isTrue();
    assertThat(config.planningWorkerPoolSize()).isEqualTo(ThreadPools.WORKER_THREAD_POOL_SIZE);
    assertThat(config.equalSchemes()).containsEntry("s3n", "s3").containsEntry("s3a", "s3");
    assertThat(config.equalAuthorities()).isEqualTo(Map.of());
    assertThat(config.prefixMismatchMode()).isEqualTo(PrefixMismatchMode.ERROR);
  }
}
