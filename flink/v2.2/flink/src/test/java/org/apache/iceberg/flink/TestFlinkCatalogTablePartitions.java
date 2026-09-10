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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkCatalogTablePartitions extends CatalogTestBase {

  private final String tableName = "test_table";

  @Parameter(index = 2)
  private FileFormat format;

  @Parameter(index = 3)
  private Boolean cacheEnabled;

  @Parameters(name = "catalogType={0}, baseNamespace={1}, format={2}, cacheEnabled={3}")
  protected static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Boolean cacheEnabled : new Boolean[] {true, false}) {
        for (Object[] catalogParams : CatalogTestBase.parameters()) {
          CatalogType catalogType = (CatalogType) catalogParams[0];
          Namespace baseNamespace = (Namespace) catalogParams[1];
          parameters.add(new Object[] {catalogType, baseNamespace, format, cacheEnabled});
        }
      }
    }
    return parameters;
  }

  @Override
  @BeforeEach
  public void before() {
    config.put(CatalogProperties.CACHE_ENABLED, String.valueOf(cacheEnabled));
    super.before();
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

    ObjectPath tablePath = new ObjectPath(DATABASE, tableName);
    FlinkCatalog flinkCatalog = (FlinkCatalog) getTableEnv().getCatalog(catalogName).get();
    assertThatThrownBy(() -> flinkCatalog.listPartitions(tablePath))
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

    ObjectPath tablePath = new ObjectPath(DATABASE, tableName);
    FlinkCatalog flinkCatalog = (FlinkCatalog) getTableEnv().getCatalog(catalogName).get();
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(tablePath);
    assertThat(list).hasSize(2);
    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("data", "a"));
    CatalogPartitionSpec partitionSpec2 = new CatalogPartitionSpec(ImmutableMap.of("data", "b"));
    expected.add(partitionSpec1);
    expected.add(partitionSpec2);
    assertThat(list).as("Should produce the expected catalog partition specs.").isEqualTo(expected);
  }

  @TestTemplate
  void dropPartitionThroughSql() throws TableNotExistException, TableNotPartitionedException {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);
    sql("INSERT INTO %s SELECT 2,'b'", tableName);

    sql("ALTER TABLE %s DROP PARTITION (data = 'a')", tableName);

    FlinkCatalog flinkCatalog = flinkCatalog();
    assertThat(flinkCatalog.listPartitions(objectPath))
        .containsExactly(new CatalogPartitionSpec(ImmutableMap.of("data", "b")));
  }

  @TestTemplate
  void dropPartitionWithMultipleFields()
      throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (id, data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);
    sql("INSERT INTO %s SELECT 1,'b'", tableName);
    sql("INSERT INTO %s SELECT 2,'a'", tableName);

    FlinkCatalog flinkCatalog = flinkCatalog();
    flinkCatalog.dropPartition(
        objectPath,
        new CatalogPartitionSpec(ImmutableMap.of("id", "1", "data", "a")),
        false);

    assertThat(flinkCatalog.listPartitions(objectPath))
        .containsExactlyInAnyOrder(
            new CatalogPartitionSpec(ImmutableMap.of("id", "1", "data", "b")),
            new CatalogPartitionSpec(ImmutableMap.of("id", "2", "data", "a")));
  }

  @TestTemplate
  void dropPartitionWithDatePartition()
      throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
    sql(
        "CREATE TABLE %s (id INT, dt DATE) PARTITIONED BY (dt) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1, DATE '2024-01-01'", tableName);
    sql("INSERT INTO %s SELECT 2, DATE '2024-01-02'", tableName);

    FlinkCatalog flinkCatalog = flinkCatalog();
    flinkCatalog.dropPartition(
        objectPath, new CatalogPartitionSpec(ImmutableMap.of("dt", "2024-01-01")), false);

    assertThat(flinkCatalog.listPartitions(objectPath))
        .containsExactly(new CatalogPartitionSpec(ImmutableMap.of("dt", "2024-01-02")));
  }

  @TestTemplate
  void dropPartitionWithNullValue()
      throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1, CAST(NULL AS VARCHAR)", tableName);
    sql("INSERT INTO %s SELECT 2, 'null'", tableName);

    FlinkCatalog flinkCatalog = flinkCatalog();
    flinkCatalog.dropPartition(
        objectPath,
        new CatalogPartitionSpec(Collections.singletonMap("data", null)),
        false);

    assertThat(flinkCatalog.listPartitions(objectPath))
        .containsExactly(new CatalogPartitionSpec(ImmutableMap.of("data", "null")));
  }

  @TestTemplate
  void listPartitionsPreservesNullValue()
      throws TableNotExistException, TableNotPartitionedException {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1, CAST(NULL AS VARCHAR)", tableName);

    assertThat(flinkCatalog().listPartitions(objectPath))
        .containsExactly(new CatalogPartitionSpec(Collections.singletonMap("data", null)));
  }

  @TestTemplate
  void dropPartitionRejectsNonIdentityTransform() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, tableName),
        schema,
        PartitionSpec.builderFor(schema).bucket("id", 2).build());

    assertThatThrownBy(
            () ->
                flinkCatalog()
                    .dropPartition(
                        objectPath,
                        new CatalogPartitionSpec(ImmutableMap.of("id_bucket", "0")),
                        false))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Invalid partition spec");
  }

  @TestTemplate
  void dropPartitionIfNotExistsDoesNotCreateSnapshot()
      throws TableNotExistException, TableNotPartitionedException {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);

    FlinkCatalog flinkCatalog = flinkCatalog();
    long snapshotId = currentSnapshotId();
    sql("ALTER TABLE %s DROP IF EXISTS PARTITION (data = 'missing')", tableName);

    assertThat(currentSnapshotId()).isEqualTo(snapshotId);
    assertThat(flinkCatalog.listPartitions(objectPath))
        .containsExactly(new CatalogPartitionSpec(ImmutableMap.of("data", "a")));
  }

  @TestTemplate
  void dropPartitionThrowsWhenPartitionDoesNotExist() throws PartitionNotExistException {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) "
            + "with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);

    CatalogPartitionSpec partitionSpec =
        new CatalogPartitionSpec(ImmutableMap.of("data", "missing"));

    assertThatThrownBy(() -> flinkCatalog().dropPartition(objectPath, partitionSpec, false))
        .isInstanceOf(PartitionNotExistException.class)
        .hasMessageContaining("does not exist");
  }

  @TestTemplate
  void dropPartitionWithUnpartitionedTable() throws PartitionNotExistException {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);

    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(ImmutableMap.of("data", "a"));

    assertThatThrownBy(() -> flinkCatalog().dropPartition(objectPath, partitionSpec, false))
        .isInstanceOf(PartitionNotExistException.class)
        .hasMessageContaining("does not exist");
  }

  @TestTemplate
  void dropPartitionWithIgnoreOnUnpartitionedTable() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) with ('write.format.default'='%s')",
        tableName, format.name());

    assertThatCode(
            () ->
                flinkCatalog()
                    .dropPartition(
                        objectPath,
                        new CatalogPartitionSpec(ImmutableMap.of("data", "a")),
                        true))
        .doesNotThrowAnyException();
  }

  @TestTemplate
  void dropPartitionWithIgnoreOnMissingTable() {
    assertThatCode(
            () ->
                flinkCatalog()
                    .dropPartition(
                        new ObjectPath(DATABASE, "missing_table"),
                        new CatalogPartitionSpec(ImmutableMap.of("data", "a")),
                        true))
        .doesNotThrowAnyException();
  }

  @TestTemplate
  void dropPartitionThrowsForMissingTable() {
    assertThatThrownBy(
            () ->
                flinkCatalog()
                    .dropPartition(
                        new ObjectPath(DATABASE, "missing_table"),
                        new CatalogPartitionSpec(ImmutableMap.of("data", "a")),
                        false))
        .isInstanceOf(PartitionNotExistException.class)
        .hasMessageContaining("does not exist");
  }

  private FlinkCatalog flinkCatalog() {
    return (FlinkCatalog) getTableEnv().getCatalog(catalogName).get();
  }

  private long currentSnapshotId() {
    return validationCatalog
        .loadTable(TableIdentifier.of(icebergNamespace, tableName))
        .currentSnapshot()
        .snapshotId();
  }

  private final ObjectPath objectPath = new ObjectPath(DATABASE, tableName);
}
