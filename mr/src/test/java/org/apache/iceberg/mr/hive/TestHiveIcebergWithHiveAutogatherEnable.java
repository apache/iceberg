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
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestHiveIcebergWithHiveAutogatherEnable {

  @Parameters(name = "fileFormat={0}, catalog={1}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = Lists.newArrayList();
    // Run tests with every FileFormat for a single Catalog (HiveCatalog)
    for (FileFormat fileFormat : HiveIcebergStorageHandlerTestUtils.FILE_FORMATS) {
      testParams.add(new Object[] {fileFormat, TestTables.TestTableType.HIVE_CATALOG});
    }
    return testParams;
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter(index = 0)
  private FileFormat fileFormat;

  @Parameter(index = 1)
  private TestTables.TestTableType testTableType;

  @TempDir private Path temp;

  @BeforeAll
  public static void beforeClass() {
    // The hive configuration HIVESTATSAUTOGATHER must be set to true from hive engine
    shell =
        HiveIcebergStorageHandlerTestUtils.shell(
            ImmutableMap.of(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, "true"));
  }

  @AfterAll
  public static void afterClass() throws Exception {
    shell.stop();
  }

  @BeforeEach
  public void before() throws IOException {
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, "mr");
  }

  @AfterEach
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @TestTemplate
  public void testHiveStatsAutogatherWhenCreateNewTable() throws Exception {
    // Create a Catalog where the KEEP_HIVE_STATS is false
    shell.metastore().hiveConf().set(ConfigProperties.KEEP_HIVE_STATS, StatsSetupConst.FALSE);
    TestTables hiveStatsDisabledTestTables =
        HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);

    TableIdentifier identifierWithoutStats =
        TableIdentifier.of("default", "customers_without_stats");

    // To validate the stats augother is disabled from Hive engine, the creation of iceberg table
    // cannot have any records. Otherwise, the table parameters TOTAL_SIZE and NUM_FILES are
    // added by Iceberg when inserting records.
    hiveStatsDisabledTestTables.createTable(
        shell,
        identifierWithoutStats.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat,
        ImmutableList.of());

    // The table parameter TOTAL_SIZE is removed from hive engine
    String totalSize =
        shell
            .metastore()
            .getTable(identifierWithoutStats)
            .getParameters()
            .get(StatsSetupConst.TOTAL_SIZE);
    assertThat(totalSize).isNull();

    // The table parameter NUM_FILES is removed from hive engine
    String numFiles =
        shell
            .metastore()
            .getTable(identifierWithoutStats)
            .getParameters()
            .get(StatsSetupConst.NUM_FILES);
    assertThat(numFiles).isNull();

    // The table parameter DO_NOT_UPDATE_STATS is removed from hive engine
    String stats =
        shell
            .metastore()
            .getTable(identifierWithoutStats)
            .getParameters()
            .get(StatsSetupConst.DO_NOT_UPDATE_STATS);
    assertThat(stats).isNull();

    // Create a Catalog where the KEEP_HIVE_STATS is true
    shell.metastore().hiveConf().set(ConfigProperties.KEEP_HIVE_STATS, StatsSetupConst.TRUE);
    TestTables keepHiveStatsTestTables =
        HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);

    TableIdentifier identifierWithStats = TableIdentifier.of("default", "customers_with_stats");

    // To validate the stats augother is enabled from Hive engine, the creation of iceberg table
    // cannot have any records. Otherwise, the table parameters TOTAL_SIZE and NUM_FILES are
    // added by Iceberg when inserting records.
    keepHiveStatsTestTables.createTable(
        shell,
        identifierWithStats.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat,
        ImmutableList.of());

    // The table parameter DO_NOT_UPDATE_STATS doesn't exist
    stats =
        shell
            .metastore()
            .getTable(identifierWithStats)
            .getParameters()
            .get(StatsSetupConst.DO_NOT_UPDATE_STATS);
    assertThat(stats).isNull();

    // The table parameter NUM_FILES is gathered from hive engine
    numFiles =
        shell
            .metastore()
            .getTable(identifierWithStats)
            .getParameters()
            .get(StatsSetupConst.NUM_FILES);
    assertThat(numFiles).isEqualTo("1");

    // The table parameter TOTAL_SIZE is gathered from hive engine
    numFiles =
        shell
            .metastore()
            .getTable(identifierWithStats)
            .getParameters()
            .get(StatsSetupConst.TOTAL_SIZE);
    assertThat(numFiles).isNotNull();
  }
}
