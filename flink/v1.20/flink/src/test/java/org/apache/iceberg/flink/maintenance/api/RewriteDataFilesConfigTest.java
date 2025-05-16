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

public class RewriteDataFilesConfigTest {
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

    assertThat(config.getPartialProgressEnable()).isTrue();
    assertThat(config.getPartialProgressMaxCommits()).isEqualTo(5);
    assertThat(config.getMaxRewriteBytes()).isEqualTo(1024L);
    assertThat(config.getScheduleOnCommitCount()).isEqualTo(10);
    assertThat(config.getScheduleOnDataFileCount()).isEqualTo(20);
    assertThat(config.getScheduleOnDataFileSize()).isEqualTo(30);
    assertThat(config.getScheduleOnIntervalSecond()).isEqualTo(60);
  }

  @Test
  void testEmptyConfig() {
    RewriteDataFilesConfig config = new RewriteDataFilesConfig(Maps.newHashMap());

    assertThat(config.getPartialProgressEnable()).isNull();
    assertThat(config.getPartialProgressMaxCommits()).isNull();
    assertThat(config.getMaxRewriteBytes()).isNull();
    assertThat(config.getScheduleOnCommitCount()).isNull();
    assertThat(config.getScheduleOnDataFileCount()).isNull();
    assertThat(config.getScheduleOnDataFileSize()).isNull();
    assertThat(config.getScheduleOnIntervalSecond()).isNull();
  }

  @Test
  void testPropertiesMethodWithAllConfigs() {
    RewriteDataFiles.Builder builder = RewriteDataFiles.builder();
    builder.properties(input);

    // check the config about the rewriter
    assertThat(builder.partialProgressEnabled()).isTrue();
    assertThat(builder.getPartialProgressMaxCommits()).isEqualTo(5);
    assertThat(builder.getMaxRewriteBytes()).isEqualTo(1024L);

    // check the config about the schedule
    assertThat(builder.getScheduleCommitCount()).isEqualTo(10);
    assertThat(builder.getScheduleDataFileCount()).isEqualTo(20);
    assertThat(builder.getScheduleDataFileSize()).isEqualTo(30);
    assertThat(builder.getScheduleInterval()).isEqualTo(Duration.ofSeconds(60));

    assertThat(builder.getRewriteOptions())
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
    assertThat(builder.getPartialProgressMaxCommits()).isEqualTo(10);
    assertThat(builder.getMaxRewriteBytes()).isEqualTo(Long.MAX_VALUE);

    // check the config about the schedule
    assertThat(builder.getScheduleCommitCount()).isNull();
    assertThat(builder.getScheduleDataFileCount()).isNull();
    assertThat(builder.getScheduleDataFileSize()).isNull();
    assertThat(builder.getScheduleInterval()).isNull();
  }
}
