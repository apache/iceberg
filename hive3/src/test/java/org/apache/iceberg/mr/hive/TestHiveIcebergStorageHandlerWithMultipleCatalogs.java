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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestHiveIcebergStorageHandlerWithMultipleCatalogs {

  private static final String[] EXECUTION_ENGINES = new String[] {"tez", "mr"};
  private static final String HIVECATALOGNAME = "table1_catalog";
  private static final String OTHERCATALOGNAME = "table2_catalog";
  private static TestHiveShell shell;

  @Parameter(index = 0)
  private FileFormat fileFormat1;

  @Parameter(index = 1)
  private FileFormat fileFormat2;

  @Parameter(index = 2)
  private String executionEngine;

  @Parameter(index = 3)
  private TestTables.TestTableType testTableType1;

  @Parameter(index = 4)
  private String table1CatalogName;

  @Parameter(index = 5)
  private TestTables.TestTableType testTableType2;

  @Parameter(index = 6)
  private String table2CatalogName;

  @TempDir private Path temp;
  private TestTables testTables1;
  private TestTables testTables2;

  @Parameters(
      name =
          "fileFormat1={0}, fileFormat2={1}, engine={2}, tableType1={3}, catalogName1={4}, "
              + "tableType2={5}, catalogName2={6}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = Lists.newArrayList();
    String javaVersion = System.getProperty("java.specification.version");

    // Run tests with PARQUET and ORC file formats for a two Catalogs
    for (String engine : EXECUTION_ENGINES) {
      // include Tez tests only for Java 8
      if (javaVersion.equals("1.8") || "mr".equals(engine)) {
        for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
          if (!TestTables.TestTableType.HIVE_CATALOG.equals(testTableType)) {
            testParams.add(
                new Object[] {
                  FileFormat.PARQUET,
                  FileFormat.ORC,
                  engine,
                  TestTables.TestTableType.HIVE_CATALOG,
                  HIVECATALOGNAME,
                  testTableType,
                  OTHERCATALOGNAME
                });
          }
        }
      }
    }
    return testParams;
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
    testTables1 =
        HiveIcebergStorageHandlerTestUtils.testTables(
            shell, testTableType1, temp, table1CatalogName);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables1, temp, executionEngine);
    testTables1
        .properties()
        .entrySet()
        .forEach(e -> shell.setHiveSessionValue(e.getKey(), e.getValue()));

    testTables2 =
        HiveIcebergStorageHandlerTestUtils.testTables(
            shell, testTableType2, temp, table2CatalogName);
    testTables2
        .properties()
        .entrySet()
        .forEach(e -> shell.setHiveSessionValue(e.getKey(), e.getValue()));
  }

  @AfterEach
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @TestTemplate
  public void testJoinTablesFromDifferentCatalogs() throws IOException {
    createAndAddRecords(
        testTables1,
        fileFormat1,
        TableIdentifier.of("default", "customers1"),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    createAndAddRecords(
        testTables2,
        fileFormat2,
        TableIdentifier.of("default", "customers2"),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    List<Object[]> rows =
        shell.executeStatement(
            "SELECT c2.customer_id, c2.first_name, c2.last_name "
                + "FROM default.customers2 c2 JOIN default.customers1 c1 ON c2.customer_id = c1.customer_id "
                + "ORDER BY c2.customer_id");
    assertThat(rows).hasSameSizeAs(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    HiveIcebergTestUtils.validateData(
        Lists.newArrayList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS),
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, rows),
        0);
  }

  private void createAndAddRecords(
      TestTables testTables,
      FileFormat fileFormat,
      TableIdentifier identifier,
      List<Record> records)
      throws IOException {
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
