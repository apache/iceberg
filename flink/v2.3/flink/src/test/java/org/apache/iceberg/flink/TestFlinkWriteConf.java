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

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestFlinkWriteConf {

  @Test
  void staticOverwriteIsOffByDefault() {
    FlinkWriteConf conf = new FlinkWriteConf(ImmutableMap.of(), new Configuration());
    assertThat(conf.staticOverwrite()).isFalse();
  }

  @Test
  void staticOverwriteFromWriteOption() {
    Map<String, String> options =
        ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY.key(), "STATIC");
    FlinkWriteConf conf = new FlinkWriteConf(options, new Configuration());
    assertThat(conf.staticOverwrite()).isTrue();
  }

  @Test
  void staticOverwriteFromFlinkConfig() {
    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkWriteOptions.OVERWRITE_STRATEGY, "static");
    FlinkWriteConf conf = new FlinkWriteConf(ImmutableMap.of(), flinkConf);
    assertThat(conf.staticOverwrite()).isTrue();
  }

  @Test
  void writeOptionOverridesFlinkConfig() {
    Map<String, String> options =
        ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY.key(), "dynamic");
    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkWriteOptions.OVERWRITE_STRATEGY, "static");
    FlinkWriteConf conf = new FlinkWriteConf(options, flinkConf);
    assertThat(conf.staticOverwrite()).isFalse();
  }

  @Test
  void unknownOverwriteModeIsRejected() {
    Map<String, String> options =
        ImmutableMap.of(FlinkWriteOptions.OVERWRITE_STRATEGY.key(), "unknown");
    FlinkWriteConf conf = new FlinkWriteConf(options, new Configuration());
    assertThatThrownBy(conf::staticOverwrite)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid overwrite mode: unknown, expected one of: dynamic, static");
  }
}
