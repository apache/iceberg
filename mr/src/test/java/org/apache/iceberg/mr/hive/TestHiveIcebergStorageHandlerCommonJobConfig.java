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
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

  private void executeSql() throws IOException {
    createAndAddRecords(
        FileFormat.ORC,
        TableIdentifier.of("default", "customers1"),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    shell.executeStatement("SELECT count(1) FROM default.customers1 limit 1");
  }

  @Parameters(name = "fakeCustomConfigValue={0}")
  public static Collection<Object[]> parameters() {
    return ImmutableList.of(
        new String[] {"fake_custom_config_value"},
        new String[] {"fake_custom_config_value "},
        new String[] {" fake_custom_config_value"},
        new String[] {" fake_custom_config_value1 , fake_custom_config_value2 "},
        new String[] {" fake_custom_config_value1 , fake_custom_config_value1 "});
  }

  @BeforeEach
  public void before() throws IOException {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
    testTables =
        HiveIcebergStorageHandlerTestUtils.testTables(
            shell, TestTables.TestTableType.HIVE_CATALOG, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, "tez");
  }

  @AfterEach
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
    shell.stop();
  }

  @Test
  public void testWithoutCustomConfigValue() throws IOException {
    String configValue = shell.getHiveSessionValue(TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES, null);
    assertThat(configValue).isNull();
    // execute sql to inject the config parameters
    executeSql();
    configValue = shell.getHiveSessionValue(TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES, null);
    assertThat(configValue).isNotNull();
    List<String> configValueList = Lists.newArrayList(configValue.split(","));
    configValueList.sort(String::compareTo);
    String sortedConfigValue = String.join(",", configValueList);
    assertThat(sortedConfigValue)
        .isEqualTo("hive.io.file.readcolumn.ids,hive.io.file.readcolumn.names");
  }

  private void createAndAddRecords(
      FileFormat fileFormat, TableIdentifier identifier, List<Record> records) throws IOException {
    String createSql =
        "CREATE EXTERNAL TABLE "
            + identifier
            + " (customer_id BIGINT, first_name STRING, last_name STRING)"
            + " STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' "
            + testTables.locationForCreateTableSQL(identifier)
            + " TBLPROPERTIES ('"
            + InputFormatConfig.CATALOG_NAME
            + "'='"
            + testTables.catalogName()
            + "')";
    shell.executeStatement(createSql);
    Table icebergTable = testTables.loadTable(identifier);
    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, null, records);
  }
}
