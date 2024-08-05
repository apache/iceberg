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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TableSourceTestBase extends TestBase {
  @Parameters(name = "useFlip27Source = {0}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {false}, {true},
    };
  }

  @Parameter(index = 0)
  protected boolean useFlip27Source;

  protected static final String CATALOG_NAME = "test_catalog";
  protected static final String DATABASE_NAME = "test_db";
  protected static final String TABLE_NAME = "test_table";
  protected final FileFormat format = FileFormat.AVRO;
  protected int scanEventCount = 0;
  protected ScanEvent lastScanEvent = null;

  @Override
  protected TableEnvironment getTableEnv() {
    super.getTableEnv().getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);
    super.getTableEnv()
        .getConfig()
        .getConfiguration()
        .setBoolean(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE.key(), useFlip27Source);
    return super.getTableEnv();
  }

  @BeforeEach
  public void before() throws IOException {
    // register a scan event listener to validate pushdown
    Listeners.register(
        event -> {
          scanEventCount += 1;
          lastScanEvent = event;
        },
        ScanEvent.class);

    File warehouseFile = File.createTempFile("junit", null, temporaryDirectory.toFile());
    assertThat(warehouseFile.delete()).isTrue();
    String warehouse = String.format("file:%s", warehouseFile);

    sql(
        "CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        CATALOG_NAME, warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql(
        "INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)",
        TABLE_NAME);

    this.scanEventCount = 0;
    this.lastScanEvent = null;
  }

  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, TABLE_NAME);
    dropDatabase(DATABASE_NAME, true);
    dropCatalog(CATALOG_NAME, true);
  }
}
