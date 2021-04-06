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

import com.google.errorprone.annotations.FormatMethod;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SmokeTest {

  private static SparkSession spark;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void beforeSuite() throws IOException {
    String warehouse = temp.newFolder().getPath();
    spark = SparkSession
        .builder()
        .appName("IcebergSpark3Smoke")
        .master("local")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate();
  }

  // Run through our Doc's Getting Started Example
  // TODO Update example so that it can actually be run, modifications were required for this test suite to run
  @Test
  public void testGettingStarted() throws IOException {
    // Creating a table
    sql("CREATE TABLE local.db.table (id bigint, data string) USING iceberg");

    // Writing
    sql("INSERT INTO local.db.table VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    Assert.assertEquals("Should have inserted 3 rows",
        3L, scalarSql("SELECT COUNT(*) FROM local.db.table"));

    sql("CREATE TABLE source (id bigint, data string) USING parquet LOCATION '%s'", temp.newFolder());
    sql("INSERT INTO source VALUES (10, 'd'), (11, 'ee')");

    sql("INSERT INTO local.db.table SELECT id, data FROM source WHERE length(data) = 1");
    Assert.assertEquals("Table should now have 4 rows",
        4L, scalarSql("SELECT COUNT(*) FROM local.db.table"));

    sql("CREATE TABLE updates (id bigint, data string) USING parquet LOCATION '%s'", temp.newFolder());
    sql("INSERT INTO updates VALUES (1, 'x'), (2, 'x'), (4, 'z')");

    sql("MERGE INTO local.db.table t USING (SELECT * FROM updates) u ON t.id = u.id\n" +
        "WHEN MATCHED THEN UPDATE SET t.data = u.data\n" +
        "WHEN NOT MATCHED THEN INSERT *");
    Assert.assertEquals("Table should now have 5 rows",
        5L, scalarSql("SELECT COUNT(*) FROM local.db.table"));
    Assert.assertEquals("Record 1 should now have data x",
        "x", scalarSql("SELECT data FROM local.db.table WHERE id = 1"));

    // Reading
    Assert.assertEquals("There should be 2 records with data x",
        2L, scalarSql(
            "SELECT count(1) as count FROM local.db.table WHERE data = 'x' GROUP BY data "));

    Assert.assertEquals("There should be 3 snapshots",
        3L, scalarSql("SELECT COUNT(*) FROM local.db.table.snapshots"));
  }

  @FormatMethod
  private List<Row> sql(String str, Object... args) {
    return spark.sql(String.format(str, args)).collectAsList();
  }

  @FormatMethod
  private Object scalarSql(String str) {
    return sql(str).get(0).get(0);
  }

}
