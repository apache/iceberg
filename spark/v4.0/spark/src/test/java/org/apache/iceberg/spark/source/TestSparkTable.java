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

import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
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
  public void testTableInequalityWithDifferentSnapshots() throws NoSuchTableException {
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    CatalogManager catalogManager = spark.sessionState().catalogManager();
    TableCatalog catalog = (TableCatalog) catalogManager.catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    SparkTable table = (SparkTable) catalog.loadTable(identifier);

    Table icebergTable = validationCatalog.loadTable(tableIdent);
    long[] snapshotIds =
        icebergTable.history().stream().mapToLong(HistoryEntry::snapshotId).toArray();

    SparkTable tableAtSnapshot1 = table.copyWithSnapshotId(snapshotIds[0]);
    SparkTable tableAtSnapshot2 = table.copyWithSnapshotId(snapshotIds[1]);

    assertThat(tableAtSnapshot1)
        .as("Tables at different snapshots must not be equal")
        .isNotEqualTo(tableAtSnapshot2);
    assertThat(tableAtSnapshot1.hashCode())
        .as("Hash codes should differ for different snapshots")
        .isNotEqualTo(tableAtSnapshot2.hashCode());
  }

  @TestTemplate
  public void testTableInequalityWithDifferentBranches() throws NoSuchTableException {
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);

    CatalogManager catalogManager = spark.sessionState().catalogManager();
    TableCatalog catalog = (TableCatalog) catalogManager.catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());

    Table icebergTable = validationCatalog.loadTable(tableIdent);
    icebergTable
        .manageSnapshots()
        .createBranch("testBranch", icebergTable.currentSnapshot().snapshotId())
        .commit();

    // reload after branch creation so the table sees the new ref
    SparkTable table = (SparkTable) catalog.loadTable(identifier);
    table.table().refresh();

    SparkTable tableOnMain = table.copyWithBranch("main");
    SparkTable tableOnBranch = table.copyWithBranch("testBranch");

    assertThat(tableOnMain)
        .as("Tables on different branches must not be equal")
        .isNotEqualTo(tableOnBranch);
    assertThat(tableOnMain.hashCode())
        .as("Hash codes should differ for different branches")
        .isNotEqualTo(tableOnBranch.hashCode());
  }
}
