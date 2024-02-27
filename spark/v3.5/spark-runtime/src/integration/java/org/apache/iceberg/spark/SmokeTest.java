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
package org.apache.iceberg.spark;

import java.io.IOException;
import java.nio.file.Files;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.extensions.ExtensionsTestBase;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class SmokeTest extends ExtensionsTestBase {
  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  // Run through our Doc's Getting Started Example
  // TODO Update doc example so that it can actually be run, modifications were required for this
  // test suite to run
  @TestTemplate
  public void testGettingStarted() throws IOException {
    // Creating a table
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    // Writing
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    Assertions.assertThat(scalarSql("SELECT COUNT(*) FROM %s", tableName))
        .as("Should have inserted 3 rows")
        .isEqualTo(3L);

    sql("DROP TABLE IF EXISTS source PURGE");
    sql(
        "CREATE TABLE source (id bigint, data string) USING parquet LOCATION '%s'",
        Files.createTempDirectory(temp, "junit"));
    sql("INSERT INTO source VALUES (10, 'd'), (11, 'ee')");

    sql("INSERT INTO %s SELECT id, data FROM source WHERE length(data) = 1", tableName);
    Assertions.assertThat(scalarSql("SELECT COUNT(*) FROM %s", tableName))
        .as("Table should now have 4 rows")
        .isEqualTo(4L);

    sql("DROP TABLE IF EXISTS updates PURGE");
    sql(
        "CREATE TABLE updates (id bigint, data string) USING parquet LOCATION '%s'",
        Files.createTempDirectory(temp, "junit"));
    sql("INSERT INTO updates VALUES (1, 'x'), (2, 'x'), (4, 'z')");

    sql(
        "MERGE INTO %s t USING (SELECT * FROM updates) u ON t.id = u.id\n"
            + "WHEN MATCHED THEN UPDATE SET t.data = u.data\n"
            + "WHEN NOT MATCHED THEN INSERT *",
        tableName);
    Assertions.assertThat(scalarSql("SELECT COUNT(*) FROM %s", tableName))
        .as("Table should now have 5 rows")
        .isEqualTo(5L);
    Assertions.assertThat(scalarSql("SELECT data FROM %s WHERE id = 1", tableName))
        .as("Record 1 should now have data x")
        .isEqualTo("x");

    // Reading
    Assertions.assertThat(
            scalarSql(
                "SELECT count(1) as count FROM %s WHERE data = 'x' GROUP BY data ", tableName))
        .as("There should be 2 records with data x")
        .isEqualTo(2L);

    // Not supported because of Spark limitation
    if (!catalogName.equals("spark_catalog")) {
      Assertions.assertThat(scalarSql("SELECT COUNT(*) FROM %s.snapshots", tableName))
          .as("There should be 3 snapshots")
          .isEqualTo(3L);
    }
  }

  // From Spark DDL Docs section
  @TestTemplate
  public void testAlterTable() {
    sql(
        "CREATE TABLE %s (category int, id bigint, data string, ts timestamp) USING iceberg",
        tableName);
    Table table = getTable();
    // Add examples
    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, id)", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD truncate(data, 4)", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD years(ts)", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, category) AS shard", tableName);
    table = getTable();
    Assertions.assertThat(table.spec().fields())
        .as("Table should have 4 partition fields")
        .hasSize(4);

    // Drop Examples
    sql("ALTER TABLE %s DROP PARTITION FIELD bucket(16, id)", tableName);
    sql("ALTER TABLE %s DROP PARTITION FIELD truncate(data, 4)", tableName);
    sql("ALTER TABLE %s DROP PARTITION FIELD years(ts)", tableName);
    sql("ALTER TABLE %s DROP PARTITION FIELD shard", tableName);

    table = getTable();
    Assertions.assertThat(table.spec().isUnpartitioned())
        .as("Table should be unpartitioned")
        .isTrue();

    // Sort order examples
    sql("ALTER TABLE %s WRITE ORDERED BY category, id", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY category ASC, id DESC", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST", tableName);
    table = getTable();
    Assertions.assertThat(table.sortOrder().fields())
        .as("Table should be sorted on 2 fields")
        .hasSize(2);
  }

  @TestTemplate
  public void testCreateTable() {
    sql("DROP TABLE IF EXISTS %s", tableName("first"));
    sql("DROP TABLE IF EXISTS %s", tableName("second"));
    sql("DROP TABLE IF EXISTS %s", tableName("third"));

    sql(
        "CREATE TABLE %s (\n"
            + "    id bigint COMMENT 'unique id',\n"
            + "    data string)\n"
            + "USING iceberg",
        tableName("first"));
    getTable("first"); // Table should exist

    sql(
        "CREATE TABLE %s (\n"
            + "    id bigint,\n"
            + "    data string,\n"
            + "    category string)\n"
            + "USING iceberg\n"
            + "PARTITIONED BY (category)",
        tableName("second"));
    Table second = getTable("second");
    Assertions.assertThat(second.spec().fields())
        .as("Should be partitioned on 1 column")
        .hasSize(1);

    sql(
        "CREATE TABLE %s (\n"
            + "    id bigint,\n"
            + "    data string,\n"
            + "    category string,\n"
            + "    ts timestamp)\n"
            + "USING iceberg\n"
            + "PARTITIONED BY (bucket(16, id), days(ts), category)",
        tableName("third"));
    Table third = getTable("third");
    Assertions.assertThat(third.spec().fields())
        .as("Should be partitioned on 3 columns")
        .hasSize(3);
  }

  private Table getTable(String name) {
    return validationCatalog.loadTable(TableIdentifier.of("default", name));
  }

  private Table getTable() {
    return validationCatalog.loadTable(tableIdent);
  }
}
