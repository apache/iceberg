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

import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRewriteDataFilesConfig {
  Map<String, String> input = Maps.newHashMap();

  @BeforeEach
  public void before() {
    input.put(
        RewriteDataFilesConfig.CONFIG_PREFIX + RewriteDataFilesConfig.PARTIAL_PROGRESS_ENABLE,
        "true");
    input.put(
        RewriteDataFilesConfig.CONFIG_PREFIX + RewriteDataFilesConfig.PARTIAL_PROGRESS_MAX_COMMITS,
        "5");
    input.put(RewriteDataFilesConfig.CONFIG_PREFIX + RewriteDataFilesConfig.MAX_BYTES, "1024");
    input.put(
        RewriteDataFilesConfig.CONFIG_PREFIX + RewriteDataFilesConfig.SCHEDULE_ON_COMMIT_COUNT,
        "10");
    input.put(
        RewriteDataFilesConfig.CONFIG_PREFIX + RewriteDataFilesConfig.SCHEDULE_ON_DATA_FILE_COUNT,
        "20");
    input.put(
        RewriteDataFilesConfig.CONFIG_PREFIX + RewriteDataFilesConfig.SCHEDULE_ON_DATA_FILE_SIZE,
        "30");
    input.put(
        RewriteDataFilesConfig.CONFIG_PREFIX + RewriteDataFilesConfig.SCHEDULE_ON_INTERVAL_SECOND,
        "60");
    input.put("other.config", "should-be-ignored");
  }

  @AfterEach
  public void after() {
    input.clear();
  }

  @Test
  void testConfigParsing() {
    RewriteDataFilesConfig config = new RewriteDataFilesConfig(input);

    assertThat(config.partialProgressEnable()).isTrue();
    assertThat(config.partialProgressMaxCommits()).isEqualTo(5);
    assertThat(config.maxRewriteBytes()).isEqualTo(1024L);
    assertThat(config.scheduleOnCommitCount()).isEqualTo(10);
    assertThat(config.scheduleOnDataFileCount()).isEqualTo(20);
    assertThat(config.scheduleOnDataFileSize()).isEqualTo(30);
    assertThat(config.scheduleOnIntervalSecond()).isEqualTo(60);
  }

  @Test
  void testEmptyConfig() {
    RewriteDataFilesConfig config = new RewriteDataFilesConfig(Maps.newHashMap());

    assertThat(config.partialProgressEnable())
        .isEqualTo(org.apache.iceberg.actions.RewriteDataFiles.PARTIAL_PROGRESS_ENABLED_DEFAULT);
    assertThat(config.partialProgressMaxCommits())
        .isEqualTo(
            org.apache.iceberg.actions.RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT);
    assertThat(config.maxRewriteBytes()).isEqualTo(Long.MAX_VALUE);
    assertThat(config.scheduleOnCommitCount())
        .isEqualTo(RewriteDataFilesConfig.SCHEDULE_ON_COMMIT_COUNT_DEFAULT);
    assertThat(config.scheduleOnDataFileCount())
        .isEqualTo(RewriteDataFilesConfig.SCHEDULE_ON_DATA_FILE_COUNT_DEFAULT);
    assertThat(config.scheduleOnDataFileSize())
        .isEqualTo(RewriteDataFilesConfig.SCHEDULE_ON_DATA_FILE_SIZE_DEFAULT);
    assertThat(config.scheduleOnIntervalSecond())
        .isEqualTo(RewriteDataFilesConfig.SCHEDULE_ON_INTERVAL_SECOND_DEFAULT);
  }

  @Test
  void testPropertiesMethodWithAllConfigs() {
    RewriteDataFiles.Builder builder = RewriteDataFiles.builder();
    builder.properties(input);

    // check the config about the rewriter
    assertThat(builder.partialProgressEnabled()).isTrue();
    assertThat(builder.partialProgressMaxCommits()).isEqualTo(5);
    assertThat(builder.maxRewriteBytes()).isEqualTo(1024L);

    // check the config about the schedule
    assertThat(builder.scheduleCommitCount()).isEqualTo(10);
    assertThat(builder.scheduleDataFileCount()).isEqualTo(20);
    assertThat(builder.scheduleDataFileSize()).isEqualTo(30);
    assertThat(builder.scheduleInterval()).isEqualTo(Duration.ofSeconds(60));

    assertThat(builder.rewriteOptions())
        .doesNotContainKey("custom.option")
        .containsEntry(RewriteDataFilesConfig.PARTIAL_PROGRESS_ENABLE, "true")
        .containsEntry(RewriteDataFilesConfig.PARTIAL_PROGRESS_MAX_COMMITS, "5")
        .containsEntry(RewriteDataFilesConfig.MAX_BYTES, "1024")
        .containsEntry(RewriteDataFilesConfig.SCHEDULE_ON_COMMIT_COUNT, "10")
        .containsEntry(RewriteDataFilesConfig.SCHEDULE_ON_DATA_FILE_COUNT, "20")
        .containsEntry(RewriteDataFilesConfig.SCHEDULE_ON_DATA_FILE_SIZE, "30")
        .containsEntry(RewriteDataFilesConfig.SCHEDULE_ON_INTERVAL_SECOND, "60");
  }

  @Test
  void testPropertiesWithDefaultConfig() {
    RewriteDataFiles.Builder builder = RewriteDataFiles.builder();
    builder.properties(Maps.newHashMap());

    // check the config about the rewriter
    assertThat(builder.partialProgressEnabled()).isFalse();
    assertThat(builder.partialProgressMaxCommits())
        .isEqualTo(
            org.apache.iceberg.actions.RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT);
    assertThat(builder.maxRewriteBytes()).isEqualTo(Long.MAX_VALUE);

    // check the config about the schedule
    assertThat(builder.scheduleCommitCount())
        .isEqualTo(RewriteDataFilesConfig.SCHEDULE_ON_COMMIT_COUNT_DEFAULT);
    assertThat(builder.scheduleDataFileCount())
        .isEqualTo(RewriteDataFilesConfig.SCHEDULE_ON_DATA_FILE_COUNT_DEFAULT);
    assertThat(builder.scheduleDataFileSize())
        .isEqualTo(RewriteDataFilesConfig.SCHEDULE_ON_DATA_FILE_SIZE_DEFAULT);
    assertThat(builder.scheduleInterval().toSeconds())
        .isEqualTo(RewriteDataFilesConfig.SCHEDULE_ON_INTERVAL_SECOND_DEFAULT);
  }
}
