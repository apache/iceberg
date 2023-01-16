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
package org.apache.iceberg.spark.extensions;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestAlterTablePartitionFields extends SparkExtensionsTestBase {
  public TestAlterTablePartitionFields(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testAddIdentityPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD category", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).identity("category").build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testAddBucketPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, id)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .bucket("id", 16, "id_bucket_16")
            .build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testAddTruncatePartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD truncate(data, 4)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .truncate("data", 4, "data_trunc_4")
            .build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testAddYearsPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD years(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).year("ts").build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testAddMonthsPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD months(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).month("ts").build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testAddDaysPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts").build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testAddHoursPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD hours(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).hour("ts").build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testAddNamedPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, id) AS shard", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).bucket("id", 16, "shard").build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testDropIdentityPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, data string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Table should start with 1 partition field", 1, table.spec().fields().size());

    sql("ALTER TABLE %s DROP PARTITION FIELD category", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .alwaysNull("category", "category")
            .build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testDropDaysPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, ts timestamp, data string) USING iceberg PARTITIONED BY (days(ts))",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Table should start with 1 partition field", 1, table.spec().fields().size());

    sql("ALTER TABLE %s DROP PARTITION FIELD days(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).alwaysNull("ts", "ts_day").build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testDropBucketPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (bucket(16, id))",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Table should start with 1 partition field", 1, table.spec().fields().size());

    sql("ALTER TABLE %s DROP PARTITION FIELD bucket(16, id)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .alwaysNull("id", "id_bucket")
            .build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testDropPartitionByName() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, id) AS shard", tableName);

    table.refresh();

    Assert.assertEquals("Table should have 1 partition field", 1, table.spec().fields().size());

    // Should be recognized as iceberg command even with extra white spaces
    sql("ALTER TABLE %s DROP  PARTITION \n FIELD shard", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(2).alwaysNull("id", "shard").build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testReplacePartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts)", tableName);
    table.refresh();
    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts").build();
    Assert.assertEquals("Should have new spec field", expected, table.spec());

    sql("ALTER TABLE %s REPLACE PARTITION FIELD days(ts) WITH hours(ts)", tableName);
    table.refresh();
    expected =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .alwaysNull("ts", "ts_day")
            .hour("ts")
            .build();
    Assert.assertEquals(
        "Should changed from daily to hourly partitioned field", expected, table.spec());
  }

  @Test
  public void testReplacePartitionAndRename() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts)", tableName);
    table.refresh();
    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts").build();
    Assert.assertEquals("Should have new spec field", expected, table.spec());

    sql("ALTER TABLE %s REPLACE PARTITION FIELD days(ts) WITH hours(ts) AS hour_col", tableName);
    table.refresh();
    expected =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .alwaysNull("ts", "ts_day")
            .hour("ts", "hour_col")
            .build();
    Assert.assertEquals(
        "Should changed from daily to hourly partitioned field", expected, table.spec());
  }

  @Test
  public void testReplaceNamedPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts) AS day_col", tableName);
    table.refresh();
    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts", "day_col").build();
    Assert.assertEquals("Should have new spec field", expected, table.spec());

    sql("ALTER TABLE %s REPLACE PARTITION FIELD day_col WITH hours(ts)", tableName);
    table.refresh();
    expected =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .alwaysNull("ts", "day_col")
            .hour("ts")
            .build();
    Assert.assertEquals(
        "Should changed from daily to hourly partitioned field", expected, table.spec());
  }

  @Test
  public void testReplaceNamedPartitionAndRenameDifferently() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts) AS day_col", tableName);
    table.refresh();
    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts", "day_col").build();
    Assert.assertEquals("Should have new spec field", expected, table.spec());

    sql("ALTER TABLE %s REPLACE PARTITION FIELD day_col WITH hours(ts) AS hour_col", tableName);
    table.refresh();
    expected =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .alwaysNull("ts", "day_col")
            .hour("ts", "hour_col")
            .build();
    Assert.assertEquals(
        "Should changed from daily to hourly partitioned field", expected, table.spec());
  }

  @Test
  public void testSparkTableAddDropPartitions() throws Exception {
    sql("CREATE TABLE %s (id bigint NOT NULL, ts timestamp, data string) USING iceberg", tableName);
    Assert.assertEquals(
        "spark table partition should be empty", 0, sparkTable().partitioning().length);

    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, id) AS shard", tableName);
    assertPartitioningEquals(sparkTable(), 1, "bucket(16, id)");

    sql("ALTER TABLE %s ADD PARTITION FIELD truncate(data, 4)", tableName);
    assertPartitioningEquals(sparkTable(), 2, "truncate(data, 4)");

    sql("ALTER TABLE %s ADD PARTITION FIELD years(ts)", tableName);
    assertPartitioningEquals(sparkTable(), 3, "years(ts)");

    sql("ALTER TABLE %s DROP PARTITION FIELD years(ts)", tableName);
    assertPartitioningEquals(sparkTable(), 2, "truncate(data, 4)");

    sql("ALTER TABLE %s DROP PARTITION FIELD truncate(data, 4)", tableName);
    assertPartitioningEquals(sparkTable(), 1, "bucket(16, id)");

    sql("ALTER TABLE %s DROP PARTITION FIELD shard", tableName);
    sql("DESCRIBE %s", tableName);
    Assert.assertEquals(
        "spark table partition should be empty", 0, sparkTable().partitioning().length);
  }

  @Test
  public void testDropColumnOfOldPartitionFieldV1() {
    // default table created in v1 format
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, ts timestamp, day_of_ts date) USING iceberg PARTITIONED BY (day_of_ts) TBLPROPERTIES('format-version' = '1')",
        tableName);

    sql("ALTER TABLE %s REPLACE PARTITION FIELD day_of_ts WITH days(ts)", tableName);

    sql("ALTER TABLE %s DROP COLUMN day_of_ts", tableName);
  }

  @Test
  public void testDropColumnOfOldPartitionFieldV2() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, ts timestamp, day_of_ts date) USING iceberg PARTITIONED BY (day_of_ts) TBLPROPERTIES('format-version' = '2')",
        tableName);

    sql("ALTER TABLE %s REPLACE PARTITION FIELD day_of_ts WITH days(ts)", tableName);

    sql("ALTER TABLE %s DROP COLUMN day_of_ts", tableName);
  }

  private void assertPartitioningEquals(SparkTable table, int len, String transform) {
    Assert.assertEquals("spark table partition should be " + len, len, table.partitioning().length);
    Assert.assertEquals(
        "latest spark table partition transform should match",
        transform,
        table.partitioning()[len - 1].toString());
  }

  private SparkTable sparkTable() throws Exception {
    validationCatalog.loadTable(tableIdent).refresh();
    CatalogManager catalogManager = spark.sessionState().catalogManager();
    TableCatalog catalog = (TableCatalog) catalogManager.catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    return (SparkTable) catalog.loadTable(identifier);
  }
}
