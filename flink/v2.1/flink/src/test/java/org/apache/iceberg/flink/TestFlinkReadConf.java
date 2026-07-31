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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestFlinkReadConf {

  @Test
  void splitSizePrecedence() {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(ImmutableMap.of(TableProperties.SPLIT_SIZE, "111"));

    // table property is used when the flink config does not set the option
    FlinkReadConf conf = new FlinkReadConf(table, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitSize()).isEqualTo(111L);

    // flink config takes precedence over the table property
    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkReadOptions.SPLIT_SIZE_OPTION, 222L);
    conf = new FlinkReadConf(table, ImmutableMap.of(), flinkConf);
    assertThat(conf.splitSize()).isEqualTo(222L);

    // default is used when neither is set
    when(table.properties()).thenReturn(ImmutableMap.of());
    conf = new FlinkReadConf(table, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitSize()).isEqualTo(TableProperties.SPLIT_SIZE_DEFAULT);
  }

  @Test
  void splitLookbackPrecedence() {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(ImmutableMap.of(TableProperties.SPLIT_LOOKBACK, "5"));

    FlinkReadConf conf = new FlinkReadConf(table, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitLookback()).isEqualTo(5);

    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkReadOptions.SPLIT_LOOKBACK_OPTION, 7);
    conf = new FlinkReadConf(table, ImmutableMap.of(), flinkConf);
    assertThat(conf.splitLookback()).isEqualTo(7);

    when(table.properties()).thenReturn(ImmutableMap.of());
    conf = new FlinkReadConf(table, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitLookback()).isEqualTo(TableProperties.SPLIT_LOOKBACK_DEFAULT);
  }

  @Test
  void splitOpenFileCostPrecedence() {
    Table table = mock(Table.class);
    when(table.properties())
        .thenReturn(ImmutableMap.of(TableProperties.SPLIT_OPEN_FILE_COST, "333"));

    FlinkReadConf conf = new FlinkReadConf(table, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitFileOpenCost()).isEqualTo(333L);

    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkReadOptions.SPLIT_FILE_OPEN_COST_OPTION, 444L);
    conf = new FlinkReadConf(table, ImmutableMap.of(), flinkConf);
    assertThat(conf.splitFileOpenCost()).isEqualTo(444L);

    when(table.properties()).thenReturn(ImmutableMap.of());
    conf = new FlinkReadConf(table, ImmutableMap.of(), new Configuration());
    assertThat(conf.splitFileOpenCost()).isEqualTo(TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
  }
}
