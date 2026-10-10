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
package org.apache.iceberg.spark.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.actions.SparkActions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestUniqueTableLocation extends CatalogTestBase {

  private String renamedTableName;
  private TableIdentifier renamedIdent;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE_WITH_UNIQUE_LOCATION.catalogName(),
        SparkCatalogConfig.HIVE_WITH_UNIQUE_LOCATION.implementation(),
        SparkCatalogConfig.HIVE_WITH_UNIQUE_LOCATION.properties()
      },
      {
        SparkCatalogConfig.SPARK_SESSION_WITH_UNIQUE_LOCATION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION_WITH_UNIQUE_LOCATION.implementation(),
        SparkCatalogConfig.SPARK_SESSION_WITH_UNIQUE_LOCATION.properties()
      },
    };
  }

  @BeforeEach
  public void initTableName() {
    renamedTableName = tableName("table_2");
    renamedIdent = TableIdentifier.of(Namespace.of("default"), "table_2");
  }

  @AfterEach
  public void dropTestTable() {
    try {
      sql("DROP TABLE IF EXISTS %s", tableName);
      sql("DROP TABLE IF EXISTS %s", renamedTableName);
    } catch (NotFoundException ignore) {
      // Swallow FNF exception in case of corrupted table so test failure reason is clearer
    }
  }

  @TestTemplate
  public void noCollisionAfterRename() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("%s should not exist", tableIdent)
        .isFalse();
    assertThat(validationCatalog.tableExists(renamedIdent))
        .as("%s should not exist", renamedIdent)
        .isFalse();

    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tableName);

    sql("ALTER TABLE %s RENAME TO %s", tableName, renamedTableName);

    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Table renamedTable = validationCatalog.loadTable(renamedIdent);

    assertThat(table.location())
        .as(
            "After rename+recreate, %s and %s must have different locations",
            tableName, renamedTableName)
        .isNotEqualTo(renamedTable.location());
  }

  @TestTemplate
  public void orphanCleanupDoesntCorruptTable() {
    SparkActions actions = SparkActions.get();

    assertThat(validationCatalog.tableExists(tableIdent))
        .as("%s should not exist", tableIdent)
        .isFalse();
    assertThat(validationCatalog.tableExists(renamedIdent))
        .as("%s should not exist", renamedIdent)
        .isFalse();

    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES(0, '%s')", tableName, UUID.randomUUID().toString());

    sql("ALTER TABLE %s RENAME TO %s", tableName, renamedTableName);

    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES(1, '%s')", tableName, UUID.randomUUID().toString());

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table).as("Should load %s", table).isNotNull();

    long cutoff = System.currentTimeMillis() + 1;
    DeleteOrphanFiles.Result result = actions.deleteOrphanFiles(table).olderThan(cutoff).execute();
    assertThat(result.orphanFileLocations()).as("Should not touch any files").isEmpty();

    assertThat(scalarSql("SELECT count(*) FROM %s", renamedTableName))
        .as("Table %s should remain unaffected by %s table cleanup", renamedTableName, tableName)
        .isEqualTo(1L);
  }
}
