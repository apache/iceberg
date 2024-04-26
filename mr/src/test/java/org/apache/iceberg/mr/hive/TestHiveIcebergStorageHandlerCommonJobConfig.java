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
package org.apache.iceberg.mr.hive;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.iceberg.ParameterizedTestExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestHiveIcebergStorageHandlerCommonJobConfig {
  private static TestHiveShell shell;
  private static final String TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES =
      "tez.mrreader.config.update.properties";
  private static final String FAKE_CUSTOM_CONFIG_VALUE = "fake_custom_config_value";

  @BeforeEach
  public void before() throws IOException {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterEach
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
    shell.stop();
  }

  @TestTemplate
  public void testWithoutCustomConfigValue() {
    String configValue = shell.getHiveConf().get(TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES, null);
    assertThat(configValue).isNot(null);
    assertThat(configValue).contains("hive.io.file.readcolumn.names");
    assertThat(configValue).contains("hive.io.file.readcolumn.ids");
    assertThat(configValue).contains(",");
    assertThat(configValue).doesNotEndWith(",");
    assertThat(configValue).doesNotStartWith(",");
  }

  @TestTemplate
  public void testWithCustomConfigValue() {
    shell.setHiveConfValue(TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES, FAKE_CUSTOM_CONFIG_VALUE);
    String configValue = shell.getHiveConf().get(TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES, null);
    assertThat(configValue).isNot(null);
    assertThat(configValue).contains("hive.io.file.readcolumn.names");
    assertThat(configValue).contains("hive.io.file.readcolumn.ids");
    assertThat(configValue).contains(FAKE_CUSTOM_CONFIG_VALUE);
    assertThat(configValue).contains(",");
    assertThat(configValue).doesNotEndWith(",");
    assertThat(configValue).doesNotStartWith(",");
  }
}
