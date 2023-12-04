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

import java.util.List;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestFlinkCatalogTablePartitions extends FlinkCatalogTestBase {

  private String tableName = "test_table";

  private final FileFormat format;

  @Parameterized.Parameters(
      name = "catalogName={0}, baseNamespace={1}, format={2}, cacheEnabled={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Boolean cacheEnabled : new Boolean[] {true, false}) {
        for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
          String catalogName = (String) catalogParams[0];
          Namespace baseNamespace = (Namespace) catalogParams[1];
          parameters.add(new Object[] {catalogName, baseNamespace, format, cacheEnabled});
        }
      }
    }
    return parameters;
  }

  public TestFlinkCatalogTablePartitions(
      String catalogName, Namespace baseNamespace, FileFormat format, boolean cacheEnabled) {
    super(catalogName, baseNamespace);
    this.format = format;
    config.put(CatalogProperties.CACHE_ENABLED, String.valueOf(cacheEnabled));
  }

  @Override
  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @After
  public void cleanNamespaces() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testListPartitionsWithUnpartitionedTable() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) with ('write.format.default'='%s')",
        tableName, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", tableName);

    ObjectPath objectPath = new ObjectPath(DATABASE, tableName);
    FlinkCatalog flinkCatalog = (FlinkCatalog) getTableEnv().getCatalog(catalogName).get();
    Assertions.assertThatThrownBy(() -> flinkCatalog.listPartitions(objectPath))
        .isInstanceOf(TableNotPartitionedException.class)
        .hasMessageStartingWith("Table db.test_table in catalog")
        .hasMessageEndingWith("is not partitioned.");
  }

  @Test
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
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("data", "a"));
    CatalogPartitionSpec partitionSpec2 = new CatalogPartitionSpec(ImmutableMap.of("data", "b"));
    expected.add(partitionSpec1);
    expected.add(partitionSpec2);
    Assert.assertEquals("Should produce the expected catalog partition specs.", list, expected);
  }
}
