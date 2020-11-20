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

import java.util.HashMap;
import java.util.List;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFlinkCatalogTablePartitions extends FlinkCatalogTestBase {

  private String tableName = "partition_table";

  public TestFlinkCatalogTablePartitions(String catalogName, String[] baseNamespace) {
    super(catalogName, baseNamespace);
  }

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
  public void testListPartitionsEmpty() throws TableNotExistException {
    sql("CREATE TABLE %s (id int, data varchar)", tableName);
    sql("insert into %s select 1,'a'", tableName);

    ObjectPath objectPath = new ObjectPath(DATABASE, tableName);
    FlinkCatalog flinkCatalog = (FlinkCatalog) getTableEnv().getCatalog(catalogName).get();
    flinkCatalog.loadIcebergTable(objectPath).refresh();
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath);
    Assert.assertEquals("Should have empty partition", 0, list.size());
  }


  @Test
  public void testListPartitions() throws TableNotExistException {
    sql("CREATE TABLE %s (id int, data varchar) PARTITIONED BY (data)", tableName);
    sql("insert into %s select 1,'a'", tableName);
    sql("insert into %s select 2,'b'", tableName);

    ObjectPath objectPath = new ObjectPath(DATABASE, tableName);
    FlinkCatalog flinkCatalog = (FlinkCatalog) getTableEnv().getCatalog(catalogName).get();
    flinkCatalog.loadIcebergTable(objectPath).refresh();
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(new HashMap<String, String>() {{
        put("data", "a");
      }}
    );
    CatalogPartitionSpec partitionSpec2 = new CatalogPartitionSpec(new HashMap<String, String>() {{
        put("data", "b");
      }}
    );
    expected.add(partitionSpec1);
    expected.add(partitionSpec2);
    Assert.assertEquals("Should produce the expected record", list, expected);
  }
}
