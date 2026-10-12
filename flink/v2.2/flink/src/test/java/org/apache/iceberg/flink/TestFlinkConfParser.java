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

import java.time.Duration;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestFlinkConfParser {

  @Test
  public void testDurationConf() {
    Map<String, String> writeOptions = ImmutableMap.of("write-prop", "111s");

    ConfigOption<Duration> configOption =
        ConfigOptions.key("conf-prop").durationType().noDefaultValue();
    Configuration flinkConf = new Configuration();
    flinkConf.setString(configOption.key(), "222s");

    Table table = mock(Table.class);
    when(table.properties()).thenReturn(ImmutableMap.of("table-prop", "333s"));

    FlinkConfParser confParser = new FlinkConfParser(table, writeOptions, flinkConf);
    Duration defaultVal = Duration.ofMillis(999);

    Duration result =
        confParser.durationConf().option("write-prop").defaultValue(defaultVal).parse();
    assertThat(result).isEqualTo(Duration.ofSeconds(111));

    result = confParser.durationConf().flinkConfig(configOption).defaultValue(defaultVal).parse();
    assertThat(result).isEqualTo(Duration.ofSeconds(222));

    result = confParser.durationConf().tableProperty("table-prop").defaultValue(defaultVal).parse();
    assertThat(result).isEqualTo(Duration.ofSeconds(333));
  }
}
