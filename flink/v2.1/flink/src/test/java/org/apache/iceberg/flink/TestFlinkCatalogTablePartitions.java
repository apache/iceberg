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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.types.Row;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkCatalogTablePartitions extends CatalogTestBase {

  private final String tableName = "test_table";

  @Parameter(index = 2)
  private FileFormat format;

  @Parameter(index = 3)
  private Boolean cacheEnabled;

  @Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}, cacheEnabled={3}")
  protected static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Boolean cacheEnabled : new Boolean[] {true, false}) {
        for (Object[] catalogParams : CatalogTestBase.parameters()) {
          String catalogName = (String) catalogParams[0];
          Namespace baseNamespace = (Namespace) catalogParams[1];
          parameters.add(new Object[] {catalogName, baseNamespace, format, cacheEnabled});
        }
      }
    }
    return parameters;
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    config.put(CatalogProperties.CACHE_ENABLED, String.valueOf(cacheEnabled));
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @AfterEach
  public void cleanNamespaces() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    dropDatabase(flinkDatabase, true);
    super.clean();
  }

  @TestTemplate
  public void testListPartitionsWithUnpartitionedTable() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);

    ObjectPath objectPath = new ObjectPath(DATABASE, tableName);
    FlinkCatalog flinkCatalog = (FlinkCatalog) getTableEnv().getCatalog(catalogName).get();
    assertThatThrownBy(() -> flinkCatalog.listPartitions(objectPath))
        .isInstanceOf(TableNotPartitionedException.class)
        .hasMessageStartingWith("Table db.test_table in catalog")
        .hasMessageEndingWith("is not partitioned.");
  }

  @TestTemplate
  public void testListPartitionsWithPartitionedTable()
      throws TableNotExistException, TableNotPartitionedException {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);
    sql("INSERT INTO %s SELECT 2,'b'", tableName);

    ObjectPath objectPath = new ObjectPath(DATABASE, tableName);
    FlinkCatalog flinkCatalog = (FlinkCatalog) getTableEnv().getCatalog(catalogName).get();
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath);
    assertThat(list).hasSize(2);
    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("data", "a"));
    CatalogPartitionSpec partitionSpec2 = new CatalogPartitionSpec(ImmutableMap.of("data", "b"));
    expected.add(partitionSpec1);
    expected.add(partitionSpec2);
    assertThat(list).as("Should produce the expected catalog partition specs.").isEqualTo(expected);
  }

  @TestTemplate
  public void testDropPartitionIdentity() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);
    sql("INSERT INTO %s SELECT 2,'b'", tableName);
    sql("INSERT INTO %s SELECT 3,'c'", tableName);

    sql("ALTER TABLE %s DROP PARTITION (data='b')", tableName);

    List<Row> remaining = sql("SELECT id, data FROM %s ORDER BY id", tableName);
    assertThat(remaining).containsExactly(Row.of(1, "a"), Row.of(3, "c"));
  }

  @TestTemplate
  public void testDropPartitionMultipleColumns() {
    sql(
        "CREATE TABLE %s (id INT, region VARCHAR, dt VARCHAR) PARTITIONED BY (region, dt) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql(
        "INSERT INTO %s VALUES (1, 'us', '2024-01-01'), (2, 'us', '2024-01-02'), (3, 'eu', '2024-01-01')",
        tableName);

    sql("ALTER TABLE %s DROP PARTITION (region='us', dt='2024-01-01')", tableName);

    List<Row> remaining = sql("SELECT id, region, dt FROM %s ORDER BY id", tableName);
    assertThat(remaining)
        .containsExactly(Row.of(2, "us", "2024-01-02"), Row.of(3, "eu", "2024-01-01"));
  }

  @TestTemplate
  public void testDropPartitionIgnoreIfNotExistsOnMissingPartition() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);

    sql("ALTER TABLE %s DROP IF EXISTS PARTITION (data='z')", tableName);

    List<Row> remaining = sql("SELECT id, data FROM %s", tableName);
    assertThat(remaining).containsExactly(Row.of(1, "a"));
  }

  @TestTemplate
  public void testDropPartitionMissingPartitionFails() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);

    assertThatThrownBy(() -> sql("ALTER TABLE %s DROP PARTITION (data='z')", tableName))
        .rootCause()
        .isInstanceOf(PartitionNotExistException.class)
        .hasMessageContaining(
            "Partition CatalogPartitionSpec{{data=z}} of table db.test_table in catalog");

    List<Row> remaining = sql("SELECT id, data FROM %s", tableName);
    assertThat(remaining).containsExactly(Row.of(1, "a"));
  }

  @TestTemplate
  public void testDropPartitionRejectsPartialKey() {
    sql(
        "CREATE TABLE %s (id INT, region VARCHAR, dt VARCHAR) PARTITIONED BY (region, dt) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s VALUES (1, 'us', '2024-01-01')", tableName);

    assertThatThrownBy(() -> sql("ALTER TABLE %s DROP PARTITION (region='us')", tableName))
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "DROP PARTITION requires values for partition columns [region, dt] but got [region]");
  }

  @TestTemplate
  public void testDropPartitionRejectsUnknownColumn() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);

    assertThatThrownBy(() -> sql("ALTER TABLE %s DROP PARTITION (id='1')", tableName))
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "DROP PARTITION requires values for partition columns [data] but got [id]");
  }

  @TestTemplate
  public void testDropPartitionUnpartitionedTable() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);

    assertThatThrownBy(() -> sql("ALTER TABLE %s DROP PARTITION (data='a')", tableName))
        .rootCause()
        .isInstanceOf(PartitionNotExistException.class)
        .hasMessageContaining(
            "Partition CatalogPartitionSpec{{data=a}} of table db.test_table in catalog");
  }
}
