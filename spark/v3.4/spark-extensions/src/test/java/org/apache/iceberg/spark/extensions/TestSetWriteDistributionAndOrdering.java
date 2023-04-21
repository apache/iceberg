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

import static org.apache.iceberg.expressions.Expressions.bucket;

import java.util.Map;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.internal.SQLConf;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestSetWriteDistributionAndOrdering extends SparkExtensionsTestBase {
  public TestSetWriteDistributionAndOrdering(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSetWriteOrderByColumn() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY category, id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "range", distributionMode);

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .asc("category", NullOrder.NULLS_FIRST)
            .asc("id", NullOrder.NULLS_FIRST)
            .build();
    Assert.assertEquals("Should have expected order", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteOrderWithCaseSensitiveColumnNames() {
    sql(
        "CREATE TABLE %s (Id bigint NOT NULL, Category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());
    sql("SET %s=true", SQLConf.CASE_SENSITIVE().key());
    Assertions.assertThatThrownBy(
            () -> {
              sql("ALTER TABLE %s WRITE ORDERED BY category, id", tableName);
            })
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'category' in struct");

    sql("SET %s=false", SQLConf.CASE_SENSITIVE().key());
    sql("ALTER TABLE %s WRITE ORDERED BY category, id", tableName);
    table = validationCatalog.loadTable(tableIdent);
    SortOrder expected =
        SortOrder.builderFor(table.schema()).withOrderId(1).asc("Category").asc("Id").build();
    Assert.assertEquals("Should have expected order", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteOrderByColumnWithDirection() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY category ASC, id DESC", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "range", distributionMode);

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .asc("category", NullOrder.NULLS_FIRST)
            .desc("id", NullOrder.NULLS_LAST)
            .build();
    Assert.assertEquals("Should have expected order", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteOrderByColumnWithDirectionAndNullOrder() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "range", distributionMode);

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .asc("category", NullOrder.NULLS_LAST)
            .desc("id", NullOrder.NULLS_FIRST)
            .build();
    Assert.assertEquals("Should have expected order", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteOrderByTransform() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY category DESC, bucket(16, id), id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "range", distributionMode);

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .desc("category")
            .asc(bucket("id", 16))
            .asc("id")
            .build();
    Assert.assertEquals("Should have expected order", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteUnordered() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY category DESC, bucket(16, id), id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "range", distributionMode);

    Assert.assertNotEquals("Table must be sorted", SortOrder.unsorted(), table.sortOrder());

    sql("ALTER TABLE %s WRITE UNORDERED", tableName);

    table.refresh();

    String newDistributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("New distribution mode must match", "none", newDistributionMode);

    Assert.assertEquals("New sort order must match", SortOrder.unsorted(), table.sortOrder());
  }

  @Test
  public void testSetWriteLocallyOrdered() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE LOCALLY ORDERED BY category DESC, bucket(16, id), id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "none", distributionMode);

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .desc("category")
            .asc(bucket("id", 16))
            .asc("id")
            .build();
    Assert.assertEquals("Sort order must match", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteDistributedByWithSort() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION ORDERED BY id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    SortOrder expected = SortOrder.builderFor(table.schema()).withOrderId(1).asc("id").build();
    Assert.assertEquals("Sort order must match", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteDistributedByWithLocalSort() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    SortOrder expected = SortOrder.builderFor(table.schema()).withOrderId(1).asc("id").build();
    Assert.assertEquals("Sort order must match", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteDistributedByAndUnordered() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION UNORDERED", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    Assert.assertEquals("Sort order must match", SortOrder.unsorted(), table.sortOrder());
  }

  @Test
  public void testSetWriteDistributedByOnly() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION UNORDERED", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    Assert.assertEquals("Sort order must match", SortOrder.unsorted(), table.sortOrder());
  }

  @Test
  public void testSetWriteDistributedAndUnorderedInverted() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    Assert.assertEquals("Sort order must match", SortOrder.unsorted(), table.sortOrder());
  }

  @Test
  public void testSetWriteDistributedAndLocallyOrderedInverted() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY id DISTRIBUTED BY PARTITION", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    SortOrder expected = SortOrder.builderFor(table.schema()).withOrderId(1).asc("id").build();
    Assert.assertEquals("Sort order must match", expected, table.sortOrder());
  }
}
