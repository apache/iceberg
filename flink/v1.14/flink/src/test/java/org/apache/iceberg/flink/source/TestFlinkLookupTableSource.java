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

package org.apache.iceberg.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkTestBase;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFlinkLookupTableSource extends FlinkTestBase {
  private static final String CATALOG_NAME = "test_catalog";
  private static final String CATALOG_TYPE = "hadoop";
  private static final String DATABASE_NAME = "test_db";
  private static final String STREAM_TABLE_NAME = "stream_test_table";
  private static final String LOOKUP_TABLE_NAME = "lookup_test_table";
  private static final FileFormat format = FileFormat.PARQUET;
  private static String warehouse;

  private volatile TableEnvironment tEnv = null;

  @BeforeClass
  public static void createWarehouse() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue("The warehouse should be deleted", warehouseFile.delete());
    warehouse = "file:" + warehouseFile;
  }

  @Before
  public void before() {
    sql("CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='%s', 'warehouse'='%s')",
        CATALOG_NAME, CATALOG_TYPE, warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);
    sql("CREATE TABLE %s (id INT, name VARCHAR, tag_id INT) " +
        "WITH ('write.format.default'='%s', 'streaming'='true')", STREAM_TABLE_NAME, format.name());
    sql("CREATE TABLE %s (id INT, tag VARCHAR) " +
            "WITH ('write.format.default'='%s', 'streaming'='false', 'lookup.cache.max-rows'='100')",
        LOOKUP_TABLE_NAME, format.name());
    sql("CREATE TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "name VARCHAR," +
            "tag_id INT," +
            "proc_time AS PROCTIME()" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'streaming'='true'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s')",
        STREAM_TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, STREAM_TABLE_NAME);
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, STREAM_TABLE_NAME);
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, LOOKUP_TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", DATABASE_NAME);
    sql("DROP CATALOG IF EXISTS %s", CATALOG_NAME);
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings settings = EnvironmentSettings.newInstance()
              .inStreamingMode()
              .useBlinkPlanner()
              .build();
          tEnv = TableEnvironment.create(settings);
        }
      }
    }
    return tEnv;
  }

  @Test
  public void testLookupTableFunctionDisable() throws Exception {
    sql("INSERT INTO %s(id,tag) VALUES (1002,'tag_x')", LOOKUP_TABLE_NAME);

    TableLoader tableLoader = new TestTableLoader(TableIdentifier.of(DATABASE_NAME, LOOKUP_TABLE_NAME));
    FlinkLookupOptions options = FlinkLookupOptions.valueOf(Collections.emptyMap());
    Schema lookupTableSchema = tableLoader.loadTable().schema();
    String[] fieldNames = lookupTableSchema.identifierFieldNames().toArray(new String[0]);
    String[] lookupFieldNames = new String[] {"id"};

    FlinkLookupFunction lookupFunction = new FlinkLookupFunction(tableLoader,
        options, lookupTableSchema, fieldNames, lookupFieldNames);
    List<RowData> collector = Lists.newArrayList();
    lookupFunction.setCollector(new ListCollector(collector));
    lookupFunction.open(null);
    lookupFunction.eval(1002);
    Assert.assertNull(lookupFunction.getCache());
    Assert.assertEquals(collector, Lists.newArrayList(
        GenericRowData.of(1002, BinaryStringData.fromString("tag_x"))));

    lookupFunction.close();
  }

  @Test
  public void testLookupTableFunctionEnable() throws Exception {
    TableLoader tableLoader = new TestTableLoader(TableIdentifier.of(DATABASE_NAME, LOOKUP_TABLE_NAME));
    FlinkLookupOptions options = FlinkLookupOptions.valueOf(
        ImmutableMap.of(FlinkLookupOptions.LOOKUP_CACHE_MAX_ROWS.key(), "100"));
    Schema lookupTableSchema = tableLoader.loadTable().schema();
    String[] fieldNames = lookupTableSchema.identifierFieldNames().toArray(new String[0]);
    String[] lookupFieldNames = new String[] {"id"};

    TableLoader mockTableLoader = Mockito.mock(TableLoader.class);
    FlinkLookupFunction lookupFunction = new FlinkLookupFunction(mockTableLoader,
        options, lookupTableSchema, fieldNames, lookupFieldNames);
    List<RowData> collector = Lists.newArrayList();
    lookupFunction.setCollector(new ListCollector(collector));
    RowData rowData1002 = GenericRowData.of(1002, "test");
    lookupFunction.getCache().put(
        GenericRowData.of(1002),
        Lists.newArrayList(rowData1002));
    lookupFunction.eval(1002);
    Assert.assertEquals(collector, Lists.newArrayList(rowData1002));

    lookupFunction.close();
  }

  @Test
  public void testLookupTableFunctionCacheIgnoreEmpty() throws Exception {
    TableLoader tableLoader = new TestTableLoader(TableIdentifier.of(DATABASE_NAME, LOOKUP_TABLE_NAME));
    FlinkLookupOptions options = FlinkLookupOptions.valueOf(
        ImmutableMap.of(FlinkLookupOptions.LOOKUP_CACHE_MAX_ROWS.key(), "100",
            FlinkLookupOptions.LOOKUP_CACHE_IGNORE_EMPTY.key(), "true"));
    Schema lookupTableSchema = tableLoader.loadTable().schema();
    String[] fieldNames = lookupTableSchema.identifierFieldNames().toArray(new String[0]);
    String[] lookupFieldNames = new String[] {"id"};

    FlinkLookupFunction lookupFunction = new FlinkLookupFunction(tableLoader,
        options, lookupTableSchema, fieldNames, lookupFieldNames);
    lookupFunction.setCollector(new ListCollector(Lists.newArrayList()));
    lookupFunction.open(null);
    lookupFunction.eval(1001);
    Assert.assertEquals(lookupFunction.getCache().asMap().size(), 0);
    lookupFunction.close();

    options = FlinkLookupOptions.valueOf(
        ImmutableMap.of(FlinkLookupOptions.LOOKUP_CACHE_MAX_ROWS.key(), "100",
            FlinkLookupOptions.LOOKUP_CACHE_IGNORE_EMPTY.key(), "false"));
    lookupFunction = new FlinkLookupFunction(tableLoader,
        options, lookupTableSchema, fieldNames, lookupFieldNames);
    lookupFunction.open(null);
    List<RowData> collector = Lists.newArrayList();
    lookupFunction.setCollector(new ListCollector(collector));
    lookupFunction.eval(1001);
    Assert.assertEquals(lookupFunction.getCache().asMap().size(), 1);
    lookupFunction.close();
  }
  @Test
  public void testLookupTableFunctionRetries() throws Exception {
    int maxRetries = 10;
    TableLoader tableLoader = new TestTableLoader(TableIdentifier.of(DATABASE_NAME, LOOKUP_TABLE_NAME));
    FlinkLookupOptions options = FlinkLookupOptions.valueOf(
        ImmutableMap.of(FlinkLookupOptions.LOOKUP_MAX_RETRIES.key(), String.valueOf(maxRetries)));
    Schema lookupTableSchema = tableLoader.loadTable().schema();
    String[] fieldNames = lookupTableSchema.identifierFieldNames().toArray(new String[0]);
    String[] lookupFieldNames = new String[] {"id"};

    TableLoader mockTableLoader = Mockito.mock(TableLoader.class);
    Table mockTable = Mockito.mock(Table.class);
    Mockito.doNothing().when(mockTable).refresh();
    Mockito.when(mockTableLoader.loadTable()).thenReturn(mockTable);

    FlinkLookupFunction lookupFunction = new FlinkLookupFunction(mockTableLoader,
        options, lookupTableSchema, fieldNames, lookupFieldNames);
    lookupFunction.open(null);

    try {
      lookupFunction.eval(1001);
      Assert.fail();
    } catch (Exception e) {
      Mockito.verify(mockTable, Mockito.times(maxRetries + 1)).newScan();
    } finally {
      lookupFunction.close();
    }
  }

  @Test
  public void testLookupTableFunctionCache() throws Exception {
    sql("INSERT INTO %s(id,tag) VALUES (1002,'tag_x'), (1003,'tag_y'), (1004,'tag_z')",
        LOOKUP_TABLE_NAME);

    TableLoader tableLoader = new TestTableLoader(TableIdentifier.of(DATABASE_NAME, LOOKUP_TABLE_NAME));
    FlinkLookupOptions options = FlinkLookupOptions.valueOf(
        ImmutableMap.of(FlinkLookupOptions.LOOKUP_CACHE_MAX_ROWS.key(), "2"));
    Schema lookupTableSchema = tableLoader.loadTable().schema();
    String[] fieldNames = lookupTableSchema.identifierFieldNames().toArray(new String[0]);
    String[] lookupFieldNames = new String[] {"id"};

    FlinkLookupFunction lookupFunction = new FlinkLookupFunction(tableLoader,
        options, lookupTableSchema, fieldNames, lookupFieldNames);
    List<RowData> collector = Lists.newArrayList();
    lookupFunction.setCollector(new ListCollector<>(collector));
    lookupFunction.open(null);

    lookupFunction.eval(1002);
    RowData rowData1002 = GenericRowData.of(1002, BinaryStringData.fromString("tag_x"));
    Assert.assertEquals(
        lookupFunction.getCache().asMap(),
        Collections.singletonMap(GenericRowData.of(1002), Lists.newArrayList(rowData1002)));

    lookupFunction.eval(1003);
    RowData rowData1003 = GenericRowData.of(1003, BinaryStringData.fromString("tag_y"));
    Assert.assertEquals(
        lookupFunction.getCache().asMap(),
        ImmutableMap.of(
            GenericRowData.of(1002),
            Lists.newArrayList(rowData1002),
            GenericRowData.of(1003),
            Lists.newArrayList(rowData1003)));
    Assert.assertEquals(
        collector,
        Lists.newArrayList(rowData1002, rowData1003));

    lookupFunction.eval(1004);
    lookupFunction.getCache().cleanUp();
    Assert.assertEquals(lookupFunction.getCache().asMap().size(), 2);
    Assertions.assertThat(lookupFunction.getCache().asMap())
        .containsEntry(GenericRowData.of(1004),
            Lists.newArrayList(GenericRowData.of(1004, BinaryStringData.fromString("tag_z"))));

    lookupFunction.close();
  }

  @Test
  public void testStreamTableInnerJoinLookupTable() {
    sql("INSERT INTO %s(id,name,tag_id) " +
        "VALUES (1,'a',1001), (2,'b',1002), (3,'c',1002), (4,'d',1004)",
        STREAM_TABLE_NAME);
    sql("INSERT INTO %s(id,tag) " +
        "VALUES (1002,'tag_x'), (1003,'tag_y'), (1004,'tag_z')",
        LOOKUP_TABLE_NAME);
    String sqlLimitExceed = String.format(
        "SELECT a.id, b.tag FROM default_catalog.default_database.%s AS a " +
            "INNER JOIN %s FOR SYSTEM_TIME AS OF a.proc_time AS b " +
            "ON a.tag_id = b.id", STREAM_TABLE_NAME, LOOKUP_TABLE_NAME);
    TableResult tableResult = exec(sqlLimitExceed);
    List<Row> results = Streams.stream(tableResult.collect())
        .limit(3)
        .collect(Collectors.toList());

    TestHelpers.assertRows(results, Lists.newArrayList(
        Row.of(2, "tag_x"),
        Row.of(3, "tag_x"),
        Row.of(4, "tag_z")));
  }

  @Test
  public void testStreamTableLeftJoinLookupTable() {
    sql(
        "INSERT INTO %s(id,name,tag_id) " +
            "VALUES (1,'a',1001), (2,'b',1002), (3,'c',1002), (4,'d',1004)",
        STREAM_TABLE_NAME);
    sql(
        "INSERT INTO %s(id,tag) " +
            "VALUES (1002,'tag_x'), (1003,'tag_y'), (1004,'tag_z')",
        LOOKUP_TABLE_NAME);
    String sqlLimitExceed = String.format(
        "SELECT a.id, b.tag FROM default_catalog.default_database.%s AS a " +
            "LEFT JOIN %s FOR SYSTEM_TIME AS OF a.proc_time AS b " +
            "ON a.tag_id = b.id", STREAM_TABLE_NAME, LOOKUP_TABLE_NAME);
    TableResult tableResult = exec(sqlLimitExceed);
    List<Row> results = Streams.stream(tableResult.collect())
        .limit(4)
        .collect(Collectors.toList());

    TestHelpers.assertRows(results, Lists.newArrayList(
        Row.of(1, null),
        Row.of(2, "tag_x"),
        Row.of(3, "tag_x"),
        Row.of(4, "tag_z")));
  }

  private static class TestTableLoader implements TableLoader {
    private FlinkCatalog flinkCatalog;
    private final TableIdentifier tableIdentifier;

    TestTableLoader(TableIdentifier tableIdentifier) {
      this.tableIdentifier = tableIdentifier;
    }

    @Override
    public Table loadTable() {
      open();
      return this.flinkCatalog.catalog().loadTable(this.tableIdentifier);
    }

    @Override
    public void open() {
      if (this.flinkCatalog == null) {
        FlinkCatalogFactory catalogFactory = new FlinkCatalogFactory();
        this.flinkCatalog = (FlinkCatalog) catalogFactory.createCatalog(CATALOG_NAME,
                ImmutableMap.of(
                        "catalog-type", CATALOG_TYPE,
                        "catalog-name", CATALOG_NAME,
                        "warehouse", warehouse,
                        "write.format.default", format.name()));
      }
    }

    @Override
    public void close() throws IOException {
      this.flinkCatalog.close();
    }
  }
}
