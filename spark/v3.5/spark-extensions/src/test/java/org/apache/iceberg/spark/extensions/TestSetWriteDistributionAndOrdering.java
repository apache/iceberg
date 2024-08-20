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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSetWriteDistributionAndOrdering extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testSetWriteOrderByColumn() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE ORDERED BY category, id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("range");

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .asc("category", NullOrder.NULLS_FIRST)
            .asc("id", NullOrder.NULLS_FIRST)
            .build();
    assertThat(table.sortOrder()).as("Should have expected order").isEqualTo(expected);
  }

  @TestTemplate
  public void testSetWriteOrderWithCaseSensitiveColumnNames() {
    sql(
        "CREATE TABLE %s (Id bigint NOT NULL, Category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();
    sql("SET %s=true", SQLConf.CASE_SENSITIVE().key());
    assertThatThrownBy(
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
    assertThat(table.sortOrder()).as("Should have expected order").isEqualTo(expected);
  }

  @TestTemplate
  public void testSetWriteOrderByColumnWithDirection() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE ORDERED BY category ASC, id DESC", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("range");

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .asc("category", NullOrder.NULLS_FIRST)
            .desc("id", NullOrder.NULLS_LAST)
            .build();
    assertThat(table.sortOrder()).as("Should have expected order").isEqualTo(expected);
  }

  @TestTemplate
  public void testSetWriteOrderByColumnWithDirectionAndNullOrder() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("range");

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .asc("category", NullOrder.NULLS_LAST)
            .desc("id", NullOrder.NULLS_FIRST)
            .build();
    assertThat(table.sortOrder()).as("Should have expected order").isEqualTo(expected);
  }

  @TestTemplate
  public void testSetWriteOrderByTransform() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).isTrue();

    sql("ALTER TABLE %s WRITE ORDERED BY category DESC, bucket(16, id), id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("range");

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .desc("category")
            .asc(bucket("id", 16))
            .asc("id")
            .build();
    assertThat(table.sortOrder()).as("Should have expected order").isEqualTo(expected);
  }

  @TestTemplate
  public void testSetWriteUnordered() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE ORDERED BY category DESC, bucket(16, id), id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("range");

    assertThat(table.sortOrder()).as("Table must be sorted").isNotEqualTo(SortOrder.unsorted());

    sql("ALTER TABLE %s WRITE UNORDERED", tableName);

    table.refresh();

    String newDistributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(newDistributionMode).as("New distribution mode must match").isEqualTo("none");

    assertThat(table.sortOrder()).as("New sort order must match").isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testSetWriteLocallyOrdered() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE LOCALLY ORDERED BY category DESC, bucket(16, id), id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("none");

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .desc("category")
            .asc(bucket("id", 16))
            .asc("id")
            .build();
    assertThat(table.sortOrder()).as("Sort order must match").isEqualTo(expected);
  }

  @TestTemplate
  public void testSetWriteDistributedByWithSort() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION ORDERED BY id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("hash");

    SortOrder expected = SortOrder.builderFor(table.schema()).withOrderId(1).asc("id").build();
    assertThat(table.sortOrder()).as("Sort order must match").isEqualTo(expected);
  }

  @TestTemplate
  public void testSetWriteDistributedByWithLocalSort() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY id", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("hash");

    SortOrder expected = SortOrder.builderFor(table.schema()).withOrderId(1).asc("id").build();
    assertThat(table.sortOrder()).as("Sort order must match").isEqualTo(expected);
  }

  @TestTemplate
  public void testSetWriteDistributedByAndUnordered() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION UNORDERED", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("hash");

    assertThat(table.sortOrder()).as("Sort order must match").isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testSetWriteDistributedByOnly() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION UNORDERED", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("hash");

    assertThat(table.sortOrder()).as("Sort order must match").isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testSetWriteDistributedAndUnorderedInverted() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("hash");

    assertThat(table.sortOrder()).as("Sort order must match").isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testSetWriteDistributedAndLocallyOrderedInverted() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string) USING iceberg PARTITIONED BY (category)",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.sortOrder().isUnsorted()).as("Table should start unsorted").isTrue();

    sql("ALTER TABLE %s WRITE ORDERED BY id DISTRIBUTED BY PARTITION", tableName);

    table.refresh();

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    assertThat(distributionMode).as("Distribution mode must match").isEqualTo("hash");

    SortOrder expected = SortOrder.builderFor(table.schema()).withOrderId(1).asc("id").build();
    assertThat(table.sortOrder()).as("Sort order must match").isEqualTo(expected);
  }
}
