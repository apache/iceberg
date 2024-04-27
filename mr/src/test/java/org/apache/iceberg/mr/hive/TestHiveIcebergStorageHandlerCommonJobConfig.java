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
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestHiveIcebergStorageHandlerCommonJobConfig {
  private static TestHiveShell shell;
  private static final String TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES =
      "tez.mrreader.config.update.properties";

  @Parameter(index = 0)
  private String fakeCustomConfigValue;

  private TestTables testTables;

  @TempDir private Path temp;

  @Parameters(name = "fakeCustomConfigValue={0}")
  public static Collection<Object[]> parameters() {
    return ImmutableList.of(
        new String[] {" fake_custom_config_value3 , fake_custom_config_value4 "});
  }

  @BeforeAll
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterAll
  public static void afterClass() throws Exception {
    shell.stop();
  }

  @BeforeEach
  public void before() throws IOException {
    testTables =
        HiveIcebergStorageHandlerTestUtils.testTables(
            shell, TestTables.TestTableType.HIVE_CATALOG, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, "tez");
  }

  @AfterEach
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @TestTemplate
  public void testWithoutCustomConfigValue() throws IOException {
    // execute sql to inject the config parameters
    TableIdentifier identifier = TableIdentifier.of("default", "customers1");
    testTables.createTableWithGeneratedRecords(
        shell,
        identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        FileFormat.ORC,
        10);
    shell.executeStatement("SELECT * FROM default.customers1 ORDER BY customer_id LIMIT 1");
    String configValue = shell.getHiveSessionValue(TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES, null);
    assertThat(configValue).isNotNull();
    List<String> configValueList = Lists.newArrayList(configValue.split(","));
    configValueList.sort(String::compareTo);
    String sortedConfigValue = String.join(",", configValueList);
    assertThat(sortedConfigValue)
        .isEqualTo("hive.io.file.readcolumn.ids,hive.io.file.readcolumn.names");
  }
}
