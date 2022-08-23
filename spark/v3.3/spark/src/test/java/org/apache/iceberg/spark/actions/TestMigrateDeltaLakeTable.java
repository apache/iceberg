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
package org.apache.iceberg.spark.actions;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.iceberg.actions.MigrateDeltaLakeTable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

public class TestMigrateDeltaLakeTable extends SparkCatalogTestBase {
  private static final String NAMESPACE = "default";

  private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private String partitionedIdentifier;
  private String unpartitionedIdentifier;

  @Parameterized.Parameters(name = "Catalog Name {0} - Options {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {
        "delta",
        DeltaCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "parquet-enabled", "true",
            "cache-enabled",
                "false" // Spark will delete tables using v1, leaving the cache out of sync
            )
      }
    };
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  @Rule public TemporaryFolder other = new TemporaryFolder();

  private final String partitionedTableName = "partitioned_table";
  private final String unpartitionedTableName = "unpartitioned_table";

  private final String defaultSparkCatalog = "spark_catalog";
  private String partitionedLocation;
  private String unpartitionedLocation;
  private final String type;
  private TableCatalog catalog;

  private String catalogName;

  public TestMigrateDeltaLakeTable(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    spark
        .conf()
        .set("spark.sql.catalog." + defaultSparkCatalog, SparkSessionCatalog.class.getName());
    this.catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    this.type = config.get("type");
    this.catalogName = catalogName;
  }

  @Before
  public void before() {
    try {
      File partitionedFolder = temp.newFolder();
      File unpartitionedFolder = other.newFolder();
      partitionedLocation = partitionedFolder.toURI().toString();
      unpartitionedLocation = unpartitionedFolder.toURI().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    partitionedIdentifier = destName(partitionedTableName);
    unpartitionedIdentifier = destName(unpartitionedTableName);

    CatalogExtension delta =
        (CatalogExtension) spark.sessionState().catalogManager().catalog("delta");
    // This needs to be set, otherwise Delta operations fail as the catalog is designed to override
    // the default catalog (spark_catalog).
    delta.setDelegateCatalog(spark.sessionState().catalogManager().currentCatalog());

    spark.sql(String.format("DROP TABLE IF EXISTS %s", partitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", unpartitionedIdentifier));

    // Create a partitioned and unpartitioned table, doing a few inserts on each
    IntStream.range(0, 3)
        .forEach(
            i -> {
              List<SimpleRecord> record =
                  Lists.newArrayList(new SimpleRecord(i, ALPHABET.substring(i, i + 1)));

              Dataset<Row> df = spark.createDataFrame(record, SimpleRecord.class);

              df.write()
                  .format("delta")
                  .mode(i == 0 ? SaveMode.Overwrite : SaveMode.Append)
                  .partitionBy("id")
                  .option("path", partitionedLocation)
                  .saveAsTable(partitionedIdentifier);

              df.write()
                  .format("delta")
                  .mode(i == 0 ? SaveMode.Overwrite : SaveMode.Append)
                  .option("path", unpartitionedLocation)
                  .saveAsTable(unpartitionedIdentifier);
            });

    // Delete a record from the table
    spark.sql("DELETE FROM " + partitionedIdentifier + " WHERE id=0");
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=0");

    // Update a record
    spark.sql("UPDATE " + partitionedIdentifier + " SET id=3 WHERE id=1");
    spark.sql("UPDATE " + unpartitionedIdentifier + " SET id=3 WHERE id=1");
  }

  @After
  public void after() throws IOException {
    // Drop the hive table.
    spark.sql(String.format("DROP TABLE IF EXISTS %s", destName(partitionedTableName)));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", destName(unpartitionedTableName)));
  }

  @Test
  public void testMigratePartitioned() {
    // This will test the scenario that the user switches the configuration and sets the default
    // catalog to be Iceberg
    // AFTER they had made it Delta and written a delta table there
    spark.sessionState().catalogManager().setCurrentCatalog(defaultSparkCatalog);

    catalogName = defaultSparkCatalog;
    String newTableIdentifier = destName("iceberg_table");
    MigrateDeltaLakeTable.Result result =
        SparkActions.get().migrateDeltaLakeTable(newTableIdentifier, partitionedLocation).execute();

    // Compare the results
    List<Row> oldResults = spark.sql("SELECT * FROM " + partitionedIdentifier).collectAsList();
    List<Row> newResults = spark.sql("SELECT * FROM " + newTableIdentifier).collectAsList();

    Assert.assertEquals(oldResults.size(), newResults.size());
    Assert.assertTrue(newResults.containsAll(oldResults));
    Assert.assertTrue(oldResults.containsAll(newResults));
  }

  @Test
  public void testMigrateUnpartitioned() {
    // This will test the scenario that the user switches the configuration and sets the default
    // catalog to be Iceberg
    // AFTER they had made it Delta and written a delta table there
    spark.sessionState().catalogManager().setCurrentCatalog(defaultSparkCatalog);

    catalogName = defaultSparkCatalog;
    String newTableIdentifier = destName("iceberg_table_unpartitioned");
    MigrateDeltaLakeTable.Result result =
        SparkActions.get()
            .migrateDeltaLakeTable(newTableIdentifier, unpartitionedLocation)
            .execute();

    // Compare the results
    List<Row> oldResults = spark.sql("SELECT * FROM " + unpartitionedIdentifier).collectAsList();
    List<Row> newResults = spark.sql("SELECT * FROM " + newTableIdentifier).collectAsList();

    Assert.assertEquals(oldResults.size(), newResults.size());
    Assert.assertTrue(newResults.containsAll(oldResults));
    Assert.assertTrue(oldResults.containsAll(newResults));
  }

  private String destName(String dest) {
    if (catalogName.equals("spark_catalog")) {
      return NAMESPACE + "." + catalogName + "_" + type + "_" + dest;
    } else {
      return catalogName + "." + NAMESPACE + "." + catalogName + "_" + type + "_" + dest;
    }
  }
}
