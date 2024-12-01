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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestAlterTablePartitionFields extends ExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, formatVersion = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        1
      },
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties(),
        2
      }
    };
  }

  @Parameter(index = 3)
  private int formatVersion;

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testAddIdentityPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD category", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).identity("category").build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddBucketPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, id)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .bucket("id", 16, "id_bucket_16")
            .build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddTruncatePartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD truncate(data, 4)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .truncate("data", 4, "data_trunc_4")
            .build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddYearsPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD years(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).year("ts").build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddMonthsPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD months(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).month("ts").build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddDaysPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts").build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddHoursPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD hours(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).hour("ts").build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddYearPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD year(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).year("ts").build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddMonthPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD month(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).month("ts").build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddDayPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD day(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts").build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddHourPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD hour(ts)", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).hour("ts").build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testAddNamedPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, id) AS shard", tableName);

    table.refresh();

    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).bucket("id", 16, "shard").build();

    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
  }

  @TestTemplate
  public void testDropIdentityPartition() {
    createTable("id bigint NOT NULL, category string, data string", "category");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().fields()).as("Table should start with 1 partition field").hasSize(1);

    sql("ALTER TABLE %s DROP PARTITION FIELD category", tableName);

    table.refresh();

    if (formatVersion == 1) {
      PartitionSpec expected =
          PartitionSpec.builderFor(table.schema())
              .withSpecId(1)
              .alwaysNull("category", "category")
              .build();
      assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
    } else {
      assertThat(table.spec().isUnpartitioned()).as("New spec must be unpartitioned").isTrue();
    }
  }

  @TestTemplate
  public void testDropDaysPartition() {
    createTable("id bigint NOT NULL, ts timestamp, data string", "days(ts)");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().fields()).as("Table should start with 1 partition field").hasSize(1);

    sql("ALTER TABLE %s DROP PARTITION FIELD days(ts)", tableName);

    table.refresh();

    if (formatVersion == 1) {
      PartitionSpec expected =
          PartitionSpec.builderFor(table.schema()).withSpecId(1).alwaysNull("ts", "ts_day").build();
      assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
    } else {
      assertThat(table.spec().isUnpartitioned()).as("New spec must be unpartitioned").isTrue();
    }
  }

  @TestTemplate
  public void testDropBucketPartition() {
    createTable("id bigint NOT NULL, data string", "bucket(16, id)");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().fields()).as("Table should start with 1 partition field").hasSize(1);

    sql("ALTER TABLE %s DROP PARTITION FIELD bucket(16, id)", tableName);

    table.refresh();

    if (formatVersion == 1) {
      PartitionSpec expected =
          PartitionSpec.builderFor(table.schema())
              .withSpecId(1)
              .alwaysNull("id", "id_bucket")
              .build();
      assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
    } else {
      assertThat(table.spec().isUnpartitioned()).as("New spec must be unpartitioned").isTrue();
    }
  }

  @TestTemplate
  public void testDropPartitionByName() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, id) AS shard", tableName);

    table.refresh();

    assertThat(table.spec().fields()).as("Table should have 1 partition field").hasSize(1);

    // Should be recognized as iceberg command even with extra white spaces
    sql("ALTER TABLE %s DROP  PARTITION \n FIELD shard", tableName);

    table.refresh();

    if (formatVersion == 1) {
      PartitionSpec expected =
          PartitionSpec.builderFor(table.schema()).withSpecId(2).alwaysNull("id", "shard").build();
      assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);
    } else {
      assertThat(table.spec().isUnpartitioned()).as("New spec must be unpartitioned").isTrue();
    }
  }

  @TestTemplate
  public void testReplacePartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts)", tableName);
    table.refresh();
    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts").build();
    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);

    sql("ALTER TABLE %s REPLACE PARTITION FIELD days(ts) WITH hours(ts)", tableName);
    table.refresh();
    if (formatVersion == 1) {
      expected =
          PartitionSpec.builderFor(table.schema())
              .withSpecId(2)
              .alwaysNull("ts", "ts_day")
              .hour("ts")
              .build();
    } else {
      expected =
          TestHelpers.newExpectedSpecBuilder()
              .withSchema(table.schema())
              .withSpecId(2)
              .addField("hour", 3, 1001, "ts_hour")
              .build();
    }
    assertThat(table.spec())
        .as("Should changed from daily to hourly partitioned field")
        .isEqualTo(expected);
  }

  @TestTemplate
  public void testReplacePartitionAndRename() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts)", tableName);
    table.refresh();
    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts").build();
    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);

    sql("ALTER TABLE %s REPLACE PARTITION FIELD days(ts) WITH hours(ts) AS hour_col", tableName);
    table.refresh();
    if (formatVersion == 1) {
      expected =
          PartitionSpec.builderFor(table.schema())
              .withSpecId(2)
              .alwaysNull("ts", "ts_day")
              .hour("ts", "hour_col")
              .build();
    } else {
      expected =
          TestHelpers.newExpectedSpecBuilder()
              .withSchema(table.schema())
              .withSpecId(2)
              .addField("hour", 3, 1001, "hour_col")
              .build();
    }
    assertThat(table.spec())
        .as("Should changed from daily to hourly partitioned field")
        .isEqualTo(expected);
  }

  @TestTemplate
  public void testReplaceNamedPartition() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts) AS day_col", tableName);
    table.refresh();
    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts", "day_col").build();
    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);

    sql("ALTER TABLE %s REPLACE PARTITION FIELD day_col WITH hours(ts)", tableName);
    table.refresh();
    if (formatVersion == 1) {
      expected =
          PartitionSpec.builderFor(table.schema())
              .withSpecId(2)
              .alwaysNull("ts", "day_col")
              .hour("ts")
              .build();
    } else {
      expected =
          TestHelpers.newExpectedSpecBuilder()
              .withSchema(table.schema())
              .withSpecId(2)
              .addField("hour", 3, 1001, "ts_hour")
              .build();
    }
    assertThat(table.spec())
        .as("Should changed from daily to hourly partitioned field")
        .isEqualTo(expected);
  }

  @TestTemplate
  public void testReplaceNamedPartitionAndRenameDifferently() {
    createTable("id bigint NOT NULL, category string, ts timestamp, data string");
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.spec().isUnpartitioned()).as("Table should start unpartitioned").isTrue();

    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts) AS day_col", tableName);
    table.refresh();
    PartitionSpec expected =
        PartitionSpec.builderFor(table.schema()).withSpecId(1).day("ts", "day_col").build();
    assertThat(table.spec()).as("Should have new spec field").isEqualTo(expected);

    sql("ALTER TABLE %s REPLACE PARTITION FIELD day_col WITH hours(ts) AS hour_col", tableName);
    table.refresh();
    if (formatVersion == 1) {
      expected =
          PartitionSpec.builderFor(table.schema())
              .withSpecId(2)
              .alwaysNull("ts", "day_col")
              .hour("ts", "hour_col")
              .build();
    } else {
      expected =
          TestHelpers.newExpectedSpecBuilder()
              .withSchema(table.schema())
              .withSpecId(2)
              .addField("hour", 3, 1001, "hour_col")
              .build();
    }
    assertThat(table.spec())
        .as("Should changed from daily to hourly partitioned field")
        .isEqualTo(expected);
  }

  @TestTemplate
  public void testSparkTableAddDropPartitions() throws Exception {
    createTable("id bigint NOT NULL, ts timestamp, data string");
    assertThat(sparkTable().partitioning()).as("spark table partition should be empty").hasSize(0);

    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, id) AS shard", tableName);
    assertPartitioningEquals(sparkTable(), 1, "bucket(16, id)");

    sql("ALTER TABLE %s ADD PARTITION FIELD truncate(data, 4)", tableName);
    assertPartitioningEquals(sparkTable(), 2, "truncate(4, data)");

    sql("ALTER TABLE %s ADD PARTITION FIELD years(ts)", tableName);
    assertPartitioningEquals(sparkTable(), 3, "years(ts)");

    sql("ALTER TABLE %s DROP PARTITION FIELD years(ts)", tableName);
    assertPartitioningEquals(sparkTable(), 2, "truncate(4, data)");

    sql("ALTER TABLE %s DROP PARTITION FIELD truncate(4, data)", tableName);
    assertPartitioningEquals(sparkTable(), 1, "bucket(16, id)");

    sql("ALTER TABLE %s DROP PARTITION FIELD shard", tableName);
    sql("DESCRIBE %s", tableName);
    assertThat(sparkTable().partitioning()).as("spark table partition should be empty").hasSize(0);
  }

  @TestTemplate
  public void testDropColumnOfOldPartitionFieldV1() {
    // default table created in v1 format
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, ts timestamp, day_of_ts date) USING iceberg PARTITIONED BY (day_of_ts) TBLPROPERTIES('format-version' = '1')",
        tableName);

    sql("ALTER TABLE %s REPLACE PARTITION FIELD day_of_ts WITH days(ts)", tableName);

    sql("ALTER TABLE %s DROP COLUMN day_of_ts", tableName);
  }

  @TestTemplate
  public void testDropColumnOfOldPartitionFieldV2() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, ts timestamp, day_of_ts date) USING iceberg PARTITIONED BY (day_of_ts) TBLPROPERTIES('format-version' = '2')",
        tableName);

    sql("ALTER TABLE %s REPLACE PARTITION FIELD day_of_ts WITH days(ts)", tableName);

    sql("ALTER TABLE %s DROP COLUMN day_of_ts", tableName);
  }

  private void assertPartitioningEquals(SparkTable table, int len, String transform) {
    assertThat(table.partitioning()).as("spark table partition should be " + len).hasSize(len);
    assertThat(table.partitioning()[len - 1].toString())
        .as("latest spark table partition transform should match")
        .isEqualTo(transform);
  }

  private SparkTable sparkTable() throws Exception {
    validationCatalog.loadTable(tableIdent).refresh();
    CatalogManager catalogManager = spark.sessionState().catalogManager();
    TableCatalog catalog = (TableCatalog) catalogManager.catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    return (SparkTable) catalog.loadTable(identifier);
  }

  private void createTable(String schema) {
    createTable(schema, null);
  }

  private void createTable(String schema, String spec) {
    if (spec == null) {
      sql(
          "CREATE TABLE %s (%s) USING iceberg TBLPROPERTIES ('%s' '%d')",
          tableName, schema, TableProperties.FORMAT_VERSION, formatVersion);
    } else {
      sql(
          "CREATE TABLE %s (%s) USING iceberg PARTITIONED BY (%s) TBLPROPERTIES ('%s' '%d')",
          tableName, schema, spec, TableProperties.FORMAT_VERSION, formatVersion);
    }
  }
}
