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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFlinkReadConf {

  @TempDir private Path temporaryFolder;

  private Table table;
  private Table tableWithoutProperties;

  @BeforeEach
  void before() throws IOException {
    table =
        createTable(
            ImmutableMap.of(
                TableProperties.SPLIT_SIZE, "111",
                TableProperties.SPLIT_LOOKBACK, "5",
                TableProperties.SPLIT_OPEN_FILE_COST, "333"));
    tableWithoutProperties = createTable(ImmutableMap.of());
  }

  @Test
  void splitSizePrecedence() {
    // table property is used when the flink config does not set the option
    FlinkReadConf conf = new FlinkReadConf(table, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitSize()).isEqualTo(111L);

    // flink config takes precedence over the table property
    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkReadOptions.SPLIT_SIZE_OPTION, 222L);
    conf = new FlinkReadConf(table, ImmutableMap.of(), flinkConf);
    assertThat(conf.splitSize()).isEqualTo(222L);

    // read options take precedence over the flink config and the table property
    conf = new FlinkReadConf(table, ImmutableMap.of(FlinkReadOptions.SPLIT_SIZE, "999"), flinkConf);
    assertThat(conf.splitSize()).isEqualTo(999L);

    // default is used when neither is set
    conf = new FlinkReadConf(tableWithoutProperties, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitSize()).isEqualTo(TableProperties.SPLIT_SIZE_DEFAULT);
  }

  @Test
  void splitLookbackPrecedence() {
    FlinkReadConf conf = new FlinkReadConf(table, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitLookback()).isEqualTo(5);

    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkReadOptions.SPLIT_LOOKBACK_OPTION, 7);
    conf = new FlinkReadConf(table, ImmutableMap.of(), flinkConf);
    assertThat(conf.splitLookback()).isEqualTo(7);

    conf =
        new FlinkReadConf(table, ImmutableMap.of(FlinkReadOptions.SPLIT_LOOKBACK, "9"), flinkConf);
    assertThat(conf.splitLookback()).isEqualTo(9);

    conf = new FlinkReadConf(tableWithoutProperties, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitLookback()).isEqualTo(TableProperties.SPLIT_LOOKBACK_DEFAULT);
  }

  @Test
  void splitOpenFileCostPrecedence() {
    FlinkReadConf conf = new FlinkReadConf(table, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitFileOpenCost()).isEqualTo(333L);

    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkReadOptions.SPLIT_FILE_OPEN_COST_OPTION, 444L);
    conf = new FlinkReadConf(table, ImmutableMap.of(), flinkConf);
    assertThat(conf.splitFileOpenCost()).isEqualTo(444L);

    conf =
        new FlinkReadConf(
            table, ImmutableMap.of(FlinkReadOptions.SPLIT_FILE_OPEN_COST, "999"), flinkConf);
    assertThat(conf.splitFileOpenCost()).isEqualTo(999L);

    conf = new FlinkReadConf(tableWithoutProperties, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitFileOpenCost()).isEqualTo(TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
  }

  private Table createTable(Map<String, String> properties) throws IOException {
    File folder = Files.createTempDirectory(temporaryFolder, "junit").toFile();
    return SimpleDataUtil.createTable(folder.getAbsolutePath(), properties, false);
  }
}
