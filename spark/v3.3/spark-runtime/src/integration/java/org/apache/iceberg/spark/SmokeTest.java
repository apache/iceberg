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
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.extensions.SparkExtensionsTestBase;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SmokeTest extends SparkExtensionsTestBase {

  public SmokeTest(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  // Run through our Doc's Getting Started Example
  // TODO Update doc example so that it can actually be run, modifications were required for this
  // test suite to run
  @Test
  public void testGettingStarted() throws IOException {
    // Creating a table
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    // Writing
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    Assert.assertEquals(
        "Should have inserted 3 rows", 3L, scalarSql("SELECT COUNT(*) FROM %s", tableName));

    sql("DROP TABLE IF EXISTS source");
    sql(
        "CREATE TABLE source (id bigint, data string) USING parquet LOCATION '%s'",
        temp.newFolder());
    sql("INSERT INTO source VALUES (10, 'd'), (11, 'ee')");

    sql("INSERT INTO %s SELECT id, data FROM source WHERE length(data) = 1", tableName);
    Assert.assertEquals(
        "Table should now have 4 rows", 4L, scalarSql("SELECT COUNT(*) FROM %s", tableName));

    sql("DROP TABLE IF EXISTS updates");
    sql(
        "CREATE TABLE updates (id bigint, data string) USING parquet LOCATION '%s'",
        temp.newFolder());
    sql("INSERT INTO updates VALUES (1, 'x'), (2, 'x'), (4, 'z')");

    sql(
        "MERGE INTO %s t USING (SELECT * FROM updates) u ON t.id = u.id\n"
            + "WHEN MATCHED THEN UPDATE SET t.data = u.data\n"
            + "WHEN NOT MATCHED THEN INSERT *",
        tableName);
    Assert.assertEquals(
        "Table should now have 5 rows", 5L, scalarSql("SELECT COUNT(*) FROM %s", tableName));
    Assert.assertEquals(
        "Record 1 should now have data x",
        "x",
        scalarSql("SELECT data FROM %s WHERE id = 1", tableName));

    // Reading
    Assert.assertEquals(
        "There should be 2 records with data x",
        2L,
        scalarSql("SELECT count(1) as count FROM %s WHERE data = 'x' GROUP BY data ", tableName));

    // Not supported because of Spark limitation
    if (!catalogName.equals("spark_catalog")) {
      Assert.assertEquals(
          "There should be 3 snapshots",
          3L,
          scalarSql("SELECT COUNT(*) FROM %s.snapshots", tableName));
    }
  }

  // From Spark DDL Docs section
  @Test
  public void testAlterTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (category int, id bigint, data string, ts timestamp) USING iceberg",
        tableName);
    Table table;
    // Add examples
    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, id)", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD truncate(data, 4)", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD years(ts)", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, category) AS shard", tableName);
    table = getTable();
    Assert.assertEquals("Table should have 4 partition fields", 4, table.spec().fields().size());

    // Drop Examples
    sql("ALTER TABLE %s DROP PARTITION FIELD bucket(16, id)", tableName);
    sql("ALTER TABLE %s DROP PARTITION FIELD truncate(data, 4)", tableName);
    sql("ALTER TABLE %s DROP PARTITION FIELD years(ts)", tableName);
    sql("ALTER TABLE %s DROP PARTITION FIELD shard", tableName);

    table = getTable();
    Assert.assertTrue("Table should be unpartitioned", table.spec().isUnpartitioned());

    // Sort order examples
    sql("ALTER TABLE %s WRITE ORDERED BY category, id", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY category ASC, id DESC", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST", tableName);
    table = getTable();
    Assert.assertEquals("Table should be sorted on 2 fields", 2, table.sortOrder().fields().size());
  }

  @Test
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
    Assert.assertEquals("Should be partitioned on 1 column", 1, second.spec().fields().size());

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
    Assert.assertEquals("Should be partitioned on 3 columns", 3, third.spec().fields().size());
  }

  private Table getTable(String name) {
    return validationCatalog.loadTable(TableIdentifier.of("default", name));
  }

  private Table getTable() {
    return validationCatalog.loadTable(tableIdent);
  }
}
