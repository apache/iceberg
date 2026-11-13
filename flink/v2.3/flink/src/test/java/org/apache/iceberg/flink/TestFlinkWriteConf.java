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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFlinkWriteConf {

  @TempDir private Path temporaryFolder;

  private Table table;
  private Table fullOverwriteTable;

  @BeforeEach
  void before() throws IOException {
    table = createTable(ImmutableMap.of());
    fullOverwriteTable =
        createTable(ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY_TABLE_PROPERTY, "static"));
  }

  @Test
  void fullOverwriteIsOffByDefault() {
    FlinkWriteConf conf = new FlinkWriteConf(table, ImmutableMap.of(), new Configuration());
    assertThat(conf.fullOverwrite()).isFalse();
  }

  @Test
  void fullOverwriteFromWriteOption() {
    Map<String, String> options =
        ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY.key(), "STATIC");
    FlinkWriteConf conf = new FlinkWriteConf(table, options, new Configuration());
    assertThat(conf.fullOverwrite()).isTrue();
  }

  @Test
  void fullOverwriteFromFlinkConfig() {
    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkWriteOptions.OVERWRITE_STRATEGY, "static");
    FlinkWriteConf conf = new FlinkWriteConf(table, ImmutableMap.of(), flinkConf);
    assertThat(conf.fullOverwrite()).isTrue();
  }

  @Test
  void fullOverwriteFromTableProperty() {
    FlinkWriteConf conf =
        new FlinkWriteConf(fullOverwriteTable, ImmutableMap.of(), new Configuration());
    assertThat(conf.fullOverwrite()).isTrue();
  }

  @Test
  void fullOverwriteWithoutTable() {
    Map<String, String> options =
        ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY.key(), "static");
    assertThat(new FlinkWriteConf(options, new Configuration()).fullOverwrite()).isTrue();
    assertThat(new FlinkWriteConf(ImmutableMap.of(), new Configuration()).fullOverwrite())
        .isFalse();
  }

  @Test
  void writeOptionOverridesDynamicTableProperty() throws IOException {
    Table dynamicTable =
        createTable(
            ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY_TABLE_PROPERTY, "dynamic"));
    Map<String, String> options =
        ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY.key(), "static");
    FlinkWriteConf conf = new FlinkWriteConf(dynamicTable, options, new Configuration());
    assertThat(conf.fullOverwrite()).isTrue();
  }

  @Test
  void writeOptionOverridesStaticTableProperty() {
    Map<String, String> options =
        ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY.key(), "dynamic");
    FlinkWriteConf conf = new FlinkWriteConf(fullOverwriteTable, options, new Configuration());
    assertThat(conf.fullOverwrite()).isFalse();
  }

  @Test
  void flinkConfigOverridesStaticTableProperty() {
    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkWriteOptions.OVERWRITE_STRATEGY, "dynamic");
    FlinkWriteConf conf = new FlinkWriteConf(fullOverwriteTable, ImmutableMap.of(), flinkConf);
    assertThat(conf.fullOverwrite()).isFalse();
  }

  @Test
  void writeOptionOverridesFlinkConfig() {
    Map<String, String> options =
        ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY.key(), "dynamic");
    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkWriteOptions.OVERWRITE_STRATEGY, "static");
    FlinkWriteConf conf = new FlinkWriteConf(table, options, flinkConf);
    assertThat(conf.fullOverwrite()).isFalse();
  }

  @Test
  void unknownOverwriteModeIsRejected() {
    Map<String, String> options =
        ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY.key(), "unknown");
    FlinkWriteConf conf = new FlinkWriteConf(table, options, new Configuration());
    assertThatThrownBy(conf::fullOverwrite)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid overwrite mode: unknown, expected one of: dynamic, static");
  }

  private Table createTable(Map<String, String> properties) throws IOException {
    File folder = Files.createTempDirectory(temporaryFolder, "junit").toFile();
    return SimpleDataUtil.createTable(folder.getAbsolutePath(), properties, false);
  }
}
