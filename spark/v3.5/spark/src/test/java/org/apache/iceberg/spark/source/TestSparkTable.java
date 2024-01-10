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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSparkTable extends CatalogTestBase {

  @BeforeEach
  public void createTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testTableEquality() throws NoSuchTableException {
    CatalogManager catalogManager = spark.sessionState().catalogManager();
    TableCatalog catalog = (TableCatalog) catalogManager.catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    SparkTable table1 = (SparkTable) catalog.loadTable(identifier);
    SparkTable table2 = (SparkTable) catalog.loadTable(identifier);

    // different instances pointing to the same table must be equivalent
    assertThat(table1).as("References must be different").isNotSameAs(table2);
    assertThat(table1).as("Tables must be equivalent").isEqualTo(table2);
  }

  @TestTemplate
  public void testEffectiveSnapshotIdEquality() throws NoSuchTableException {
    CatalogManager catalogManager = spark.sessionState().catalogManager();
    TableCatalog catalog = (TableCatalog) catalogManager.catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    SparkTable table = (SparkTable) catalog.loadTable(identifier);
    final long version1Snapshot = table.effectiveSnapshotId();
    final String version1 = "VERSION_1";
    table.table().manageSnapshots().createTag(version1, version1Snapshot).commit();

    SparkTable firstSnapshotTable = table.copyWithSnapshotId(version1Snapshot);
    SparkTable firstTagTable = table.copyWithBranch(version1);

    sql("UPDATE %s SET data = 'b'", tableName);

    final String version2 = "VERSION_2";
    table.table().manageSnapshots().createTag(version2, table.effectiveSnapshotId()).commit();

    SparkTable secondTagTable = table.copyWithBranch(version2);

    assertThat(firstSnapshotTable)
        .as("References for two different SparkTables must be different")
        .isNotSameAs(firstTagTable);
    assertThat(firstSnapshotTable)
        .as("The different snapshots with same id must be equal")
        .isEqualTo(firstTagTable);
    assertThat(firstTagTable)
        .as("The different snapshots should not match")
        .isNotEqualTo(secondTagTable);
    assertThat(table)
        .as("The SparkTable should points to latest snapshot")
        .isEqualTo(secondTagTable);
  }
}
