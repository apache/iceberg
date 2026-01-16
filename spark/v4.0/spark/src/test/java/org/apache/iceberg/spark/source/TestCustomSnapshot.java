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

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCustomSnapshot extends TestBaseWithCatalog {

  @BeforeEach
  public void setupTable() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
  }

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testDeleteWithSessionConfig() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    spark.conf().set("spark.sql.iceberg.snapshot-property.custom-key", "custom-value");

    sql("DELETE FROM %s WHERE id = 1", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot deleteSnapshot = table.currentSnapshot();

    assertThat(deleteSnapshot.summary())
        .as("DELETE should apply session snapshot properties")
        .containsEntry("custom-key", "custom-value");
  }

  @TestTemplate
  public void testInsertVsDeleteWithSessionConfig_InconsistentBehavior() {
    // Set session configuration
    spark.conf().set("spark.sql.iceberg.snapshot-property.custom-key", "custom-value");

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot insertSnapshot = table.currentSnapshot();

    assertThat(insertSnapshot.summary())
        .as("INSERT should have custom-key from session config")
        .containsEntry("custom-key", "custom-value");

    sql("DELETE FROM %s WHERE id = 1", tableName);
    table.refresh();
    Snapshot deleteSnapshot = table.currentSnapshot();

    assertThat(deleteSnapshot.summary())
        .as("DELETE should also have custom-key from session config")
        .containsEntry("custom-key", "custom-value");
  }

  @TestTemplate
  public void testCustomPropertyTrackingWithDelete() {
    spark.conf().set("spark.sql.iceberg.snapshot-property.custom-key", "custom-value");

    // Execute multiple operations
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    sql("DELETE FROM %s WHERE id = 2", tableName);

    // Verify all operations have custom-key information
    Table table = validationCatalog.loadTable(tableIdent);
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());

    assertThat(snapshots).hasSize(3);

    // INSERT: Should have custom-key
    assertThat(snapshots.get(0).summary())
        .as("INSERT should have custom-key")
        .containsEntry("custom-key", "custom-value");

    // UPDATE: Should have custom-key
    assertThat(snapshots.get(1).summary())
        .as("UPDATE should have custom-key")
        .containsEntry("custom-key", "custom-value");

    // DELETE: Should have custom-key
    assertThat(snapshots.get(2).summary())
        .as("DELETE should have custom-key metadata")
        .containsEntry("custom-key", "custom-value");
  }

  @TestTemplate
  public void testTruncateWithSessionConfig() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    spark.conf().set("spark.sql.iceberg.snapshot-property.custom-key", "custom-value");

    sql("TRUNCATE TABLE %s", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot truncateSnapshot = table.currentSnapshot();

    assertThat(truncateSnapshot.summary())
        .as("TRUNCATE should apply session snapshot properties")
        .containsEntry("custom-key", "custom-value");
  }
}
