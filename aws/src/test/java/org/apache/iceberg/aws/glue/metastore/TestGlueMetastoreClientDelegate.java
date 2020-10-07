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

package org.apache.iceberg.aws.glue.metastore;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeletePartitionResult;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.InternalServiceException;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.OperationTimeoutException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdatePartitionResult;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.iceberg.aws.glue.converters.CatalogToHiveConverter;
import org.apache.iceberg.aws.glue.converters.GlueInputConverter;
import org.apache.iceberg.aws.glue.lock.LockManager;
import org.apache.iceberg.aws.glue.util.ObjectTestUtils;
import org.apache.iceberg.aws.glue.util.TestExecutorServiceFactory;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.iceberg.aws.glue.util.ObjectTestUtils.getTestDatabase;
import static org.apache.iceberg.aws.glue.util.ObjectTestUtils.getTestPartition;
import static org.apache.iceberg.aws.glue.util.ObjectTestUtils.getTestTable;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGlueMetastoreClientDelegate {

  private GlueMetastoreClientDelegate metastoreClientDelegate;
  private GlueMetastoreClientDelegate metastoreClientDelegateCatalogId;

  private HiveConf conf;
  HiveConf hiveConfCatalogId; // conf with CatalogId
  private AWSGlue glueClient;
  private Warehouse wh;

  private Database testDb;
  private Table testTbl;
  private LockManager lockManager;

  private static final int BATCH_CREATE_PARTITIONS_MAX_REQUEST_SIZE = 100;
  private static final int BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE = 1000;
  private static final String CATALOG_ID = "12345";

  @Before
  public void before() throws Exception {
    conf = new HiveConf();
    glueClient = mock(AWSGlue.class);
    wh = mock(Warehouse.class);
    lockManager = mock(LockManager.class);
    metastoreClientDelegate = new GlueMetastoreClientDelegate(
        conf, new DefaultAWSGlueMetastore(conf, glueClient), wh, lockManager);

    // Create a client delegate with CatalogId
    hiveConfCatalogId = new HiveConf();
    hiveConfCatalogId.set(GlueMetastoreClientDelegate.CATALOG_ID_CONF, CATALOG_ID);
    metastoreClientDelegateCatalogId = new GlueMetastoreClientDelegate(
        hiveConfCatalogId, new DefaultAWSGlueMetastore(hiveConfCatalogId, glueClient), wh, lockManager);

    testDb = getTestDatabase();
    testTbl = getTestTable(testDb.getName());
    setupMockWarehouseForPath(new Path(
        testTbl.getStorageDescriptor().getLocation().toString()), false, true);
  }

  private void setupMockWarehouseForPath(Path path, boolean isDir, boolean mkDir) throws Exception {
    when(wh.getDnsPath(path)).thenReturn(path);
    when(wh.isDir(path)).thenReturn(isDir);
    when(wh.mkdirs(path, true)).thenReturn(mkDir);
  }

  // ===================== Thread Executor =====================

  @Test
  public void testExecutorService() throws Exception {
    Object defaultExecutorService = new DefaultExecutorServiceFactory().getExecutorService(conf);
    assertEquals("Default executor service should be used",
        metastoreClientDelegate.getExecutorService(), defaultExecutorService);
    HiveConf customConf = new HiveConf();
    customConf.set(GlueMetastoreClientDelegate.CATALOG_ID_CONF, CATALOG_ID);
    customConf.setClass(GlueMetastoreClientDelegate.CUSTOM_EXECUTOR_FACTORY_CONF,
        TestExecutorServiceFactory.class, ExecutorServiceFactory.class);
    GlueMetastoreClientDelegate customDelegate = new GlueMetastoreClientDelegate(
        customConf, mock(AWSGlueMetastore.class), mock(Warehouse.class), mock(LockManager.class));
    Object customExecutorService = new TestExecutorServiceFactory().getExecutorService(customConf);

    assertEquals("Custom executor service should be used",
        customDelegate.getExecutorService(), customExecutorService);
  }

  // ===================== Database =====================

  @Test
  public void testCreateDatabaseWithExistingDir() throws Exception {
    Path dbPath = new Path(testDb.getLocationUri());
    setupMockWarehouseForPath(dbPath, true, true);

    metastoreClientDelegate.createDatabase(CatalogToHiveConverter.convertDatabase(testDb));
    verify(glueClient, times(1)).createDatabase(any(CreateDatabaseRequest.class));
    verify(wh, times(1)).isDir(dbPath);
    verify(wh, never()).mkdirs(dbPath, true);
  }

  @Test
  public void testCreateDatabaseWithExistingDirWthCatalogId() throws Exception {
    Path dbPath = new Path(testDb.getLocationUri());
    setupMockWarehouseForPath(dbPath, true, true);

    metastoreClientDelegateCatalogId.createDatabase(CatalogToHiveConverter.convertDatabase(testDb));
    ArgumentCaptor<CreateDatabaseRequest> captor = ArgumentCaptor.forClass(CreateDatabaseRequest.class);
    verify(glueClient, times(1)).createDatabase(captor.capture());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
    verify(wh, times(1)).isDir(dbPath);
    verify(wh, never()).mkdirs(dbPath, true);
  }

  @Test
  public void testCreateDatabaseWithoutExistingDir() throws Exception {
    Path dbPath = new Path(testDb.getLocationUri());
    setupMockWarehouseForPath(dbPath, false, true);

    metastoreClientDelegate.createDatabase(CatalogToHiveConverter.convertDatabase(testDb));
    verify(glueClient, times(1)).createDatabase(any(CreateDatabaseRequest.class));
    verify(wh, times(1)).isDir(dbPath);
    verify(wh, times(1)).mkdirs(dbPath, true);
  }

  @Test
  public void testGetDatabases() throws Exception {
    when(glueClient.getDatabases(any(GetDatabasesRequest.class))).thenReturn(
        new GetDatabasesResult().withDatabaseList(testDb));

    List<String> dbs = metastoreClientDelegate.getDatabases("*");
    assertEquals(testDb.getName(), Iterables.getOnlyElement(dbs));
  }

  @Test
  public void testGetDatabasesWithCatalogId() throws Exception {
    when(glueClient.getDatabases(any(GetDatabasesRequest.class))).thenReturn(
        new GetDatabasesResult().withDatabaseList(testDb));

    List<String> dbs = metastoreClientDelegateCatalogId.getDatabases("*");
    ArgumentCaptor<GetDatabasesRequest> captor = ArgumentCaptor.forClass(GetDatabasesRequest.class);
    verify(glueClient, times(1)).getDatabases(captor.capture());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
    assertEquals(testDb.getName(), Iterables.getOnlyElement(dbs));
  }

  @Test
  public void testGetDatabasesNullPattern() throws Exception {
    when(glueClient.getDatabases(any(GetDatabasesRequest.class))).thenReturn(
        new GetDatabasesResult().withDatabaseList(testDb));

    List<String> dbs = metastoreClientDelegate.getDatabases(null);
    assertEquals(testDb.getName(), Iterables.getOnlyElement(dbs));
  }

  @Test
  public void testGetDatabase() throws Exception {
    when(glueClient.getDatabase(any(GetDatabaseRequest.class))).thenReturn(
        new GetDatabaseResult().withDatabase(getTestDatabase()));
    metastoreClientDelegate.getDatabase("db");
    verify(glueClient, atLeastOnce()).getDatabase(any(GetDatabaseRequest.class));
  }

  @Test
  public void testGetDatabaseWithCatalogId() throws Exception {
    when(glueClient.getDatabase(any(GetDatabaseRequest.class))).thenReturn(
        new GetDatabaseResult().withDatabase(getTestDatabase()));
    metastoreClientDelegateCatalogId.getDatabase("db");
    ArgumentCaptor<GetDatabaseRequest> captor = ArgumentCaptor.forClass(GetDatabaseRequest.class);
    verify(glueClient, atLeastOnce()).getDatabase(captor.capture());
    GetDatabaseRequest request = captor.getValue();
    assertEquals(CATALOG_ID, request.getCatalogId());
    assertEquals("db", request.getName());
  }

  @Test
  public void testGetAllDatabases() throws Exception {
    when(glueClient.getDatabases(any(GetDatabasesRequest.class))).thenReturn(
        new GetDatabasesResult().withDatabaseList(getTestDatabase()));
    metastoreClientDelegate.getDatabases("*");
    // Ensure this gets invoked
    verify(glueClient, atLeastOnce()).getDatabases(any(GetDatabasesRequest.class));
  }

  @Test
  public void testGetAllDatabasesPaginated() throws Exception {
    when(glueClient.getDatabases(any(GetDatabasesRequest.class)))
        .thenReturn(new GetDatabasesResult().withDatabaseList(testDb).withNextToken("token"))
        .thenReturn(new GetDatabasesResult().withDatabaseList(getTestDatabase()));
    List<String> databases = metastoreClientDelegate.getDatabases(".*");

    assertEquals(2, databases.size());
    verify(glueClient, times(2)).getDatabases(any(GetDatabasesRequest.class));
  }

  @Test
  public void testAlterDatabase() throws Exception {
    metastoreClientDelegate.alterDatabase("db", CatalogToHiveConverter.convertDatabase(testDb));
    verify(glueClient, times(1)).updateDatabase(any(UpdateDatabaseRequest.class));
  }

  @Test
  public void testAlterDatabaseWithCatalogId() throws Exception {
    metastoreClientDelegateCatalogId.alterDatabase("db", CatalogToHiveConverter.convertDatabase(testDb));
    ArgumentCaptor<UpdateDatabaseRequest> captor = ArgumentCaptor.forClass(UpdateDatabaseRequest.class);
    verify(glueClient, times(1)).updateDatabase(any(UpdateDatabaseRequest.class));
    verify(glueClient).updateDatabase(captor.capture());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
  }

  @Test
  public void testDropDatabaseDeleteData() throws Exception {
    when(glueClient.getDatabase(any(GetDatabaseRequest.class))).thenReturn(
        new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTables(any(GetTablesRequest.class))).thenReturn(
        new GetTablesResult().withTableList(ImmutableList.<Table>of()));
    Path dbPath = new Path(testDb.getLocationUri());
    when(wh.deleteDir(dbPath, true)).thenReturn(true);

    metastoreClientDelegate.dropDatabase(testDb.getName(), true, false, false);
    verify(glueClient, times(1)).deleteDatabase(any(DeleteDatabaseRequest.class));
    verify(wh, times(1)).deleteDir(dbPath, true);
  }

  @Test
  public void testDropDatabaseDeleteDataWithCatalogId() throws Exception {
    when(glueClient.getDatabase(any(GetDatabaseRequest.class))).thenReturn(
        new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTables(any(GetTablesRequest.class))).thenReturn(
        new GetTablesResult().withTableList(ImmutableList.<Table>of()));
    Path dbPath = new Path(testDb.getLocationUri());
    when(wh.deleteDir(dbPath, true)).thenReturn(true);

    metastoreClientDelegateCatalogId.dropDatabase(
        testDb.getName(), true, false, false);
    ArgumentCaptor<DeleteDatabaseRequest> captor = ArgumentCaptor.forClass(DeleteDatabaseRequest.class);
    verify(glueClient, times(1)).deleteDatabase(captor.capture());
    DeleteDatabaseRequest request = captor.getValue();
    verify(wh, times(1)).deleteDir(dbPath, true);
    assertEquals(CATALOG_ID, request.getCatalogId());
    assertEquals(testDb.getName(), request.getName());
  }

  @Test
  public void testDropDatabaseKeepData() throws Exception {
    when(glueClient.getDatabase(any(GetDatabaseRequest.class))).thenReturn(
        new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTables(any(GetTablesRequest.class))).thenReturn(
        new GetTablesResult().withTableList(ImmutableList.<Table>of()));
    Path dbPath = new Path(testDb.getLocationUri());
    when(wh.deleteDir(dbPath, true)).thenReturn(true);

    metastoreClientDelegate.dropDatabase(testDb.getName(), false, false, false);
    verify(glueClient, times(1)).deleteDatabase(any(DeleteDatabaseRequest.class));
    verify(wh, never()).deleteDir(dbPath, true);
  }

  // ======================= Table ======================

  @Test(expected = InvalidObjectException.class)
  public void testGetTableInvalidGlueTable() throws Exception {
    Table tbl = getTestTable().withTableType(null);
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(new GetTableResult().withTable(tbl));
    metastoreClientDelegate.getTable(testDb.getName(), tbl.getName());
  }

  @Test
  public void testGetTables() throws Exception {
    Table tbl2 = getTestTable();
    List<String> tableNames = ImmutableList.of(testTbl.getName(), tbl2.getName());
    List<Table> tableList = ImmutableList.of(testTbl, tbl2);

    when(glueClient.getTables(new GetTablesRequest().withDatabaseName(testDb.getName()).withExpression("*")))
        .thenReturn(new GetTablesResult().withTableList(tableList));
    List<String> result = metastoreClientDelegate.getTables(testDb.getName(), "*");

    verify(glueClient).getTables(new GetTablesRequest().withDatabaseName(testDb.getName()).withExpression("*"));
    assertThat(result, is(tableNames));
  }

  @Test
  public void testGetTableWithCatalogId() throws Exception {
    Table tbl2 = getTestTable();
    List<String> tableNames = ImmutableList.of(testTbl.getName(), tbl2.getName());
    List<Table> tableList = ImmutableList.of(testTbl, tbl2);

    when(glueClient.getTables(new GetTablesRequest()
        .withDatabaseName(testDb.getName())
        .withExpression("*")
        .withCatalogId(CATALOG_ID)))
        .thenReturn(new GetTablesResult().withTableList(tableList));
    List<String> result = metastoreClientDelegateCatalogId.getTables(testDb.getName(), "*");

    assertThat(result, is(tableNames));
    ArgumentCaptor<GetTablesRequest> captor = ArgumentCaptor.forClass(GetTablesRequest.class);
    verify(glueClient, times(1)).getTables(captor.capture());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
    assertEquals(testDb.getName(), captor.getValue().getDatabaseName());
    assertEquals("*", captor.getValue().getExpression());
  }

  @Test
  public void testGetTablesWithPagination() throws Exception {
    Table tbl2 = getTestTable();
    List<String> tableNames = ImmutableList.of(testTbl.getName(), tbl2.getName());
    List<Table> tableList1 = ImmutableList.of(testTbl);
    List<Table> tableList2 = ImmutableList.of(tbl2);

    String nextToken = "1";
    when(glueClient.getTables(any(GetTablesRequest.class)))
        .thenReturn(new GetTablesResult().withTableList(tableList1).withNextToken(nextToken))
        .thenReturn(new GetTablesResult().withTableList(tableList2));
    List<String> result = metastoreClientDelegate.getTables(testDb.getName(), "*");

    verify(glueClient, times(2)).getTables(any(GetTablesRequest.class));
    assertThat(result, is(tableNames));
  }

  @Test
  public void testGetTableMeta() throws Exception {
    List<Table> tables = Lists.newArrayList(testTbl);
    List<String> tableTypes = Lists.newArrayList(TableType.MANAGED_TABLE.name());

    when(glueClient.getDatabases(any(GetDatabasesRequest.class))).thenReturn(
        new GetDatabasesResult().withDatabaseList(testDb));
    when(glueClient.getTables(any(GetTablesRequest.class))).thenReturn(
        new GetTablesResult().withTableList(tables));

    List<TableMeta> tableMetaResult = metastoreClientDelegate.getTableMeta(
        testDb.getName(), testTbl.getName(), tableTypes);
    assertEquals(CatalogToHiveConverter.convertTableMeta(
        testTbl, testDb.getName()), Iterables.getOnlyElement(tableMetaResult));
  }

  @Test
  public void testGetTableMetaNullEmptyTableType() throws Exception {
    List<Table> tables = Lists.newArrayList(testTbl);
    List<String> tableTypes = null;

    when(glueClient.getDatabases(any(GetDatabasesRequest.class))).thenReturn(
        new GetDatabasesResult().withDatabaseList(testDb));
    when(glueClient.getTables(any(GetTablesRequest.class))).thenReturn(
        new GetTablesResult().withTableList(tables));

    List<TableMeta> tableMetaResult = metastoreClientDelegate.getTableMeta(
        testDb.getName(), testTbl.getName(), tableTypes);
    assertEquals(CatalogToHiveConverter.convertTableMeta(
        testTbl, testDb.getName()), Iterables.getOnlyElement(tableMetaResult));

    tableTypes = Lists.newArrayList();
    tableMetaResult = metastoreClientDelegate.getTableMeta(
        testDb.getName(), testTbl.getName(), tableTypes);
    assertEquals(CatalogToHiveConverter.convertTableMeta(
        testTbl, testDb.getName()), Iterables.getOnlyElement(tableMetaResult));
  }

  @Test
  public void testCreateTableWithExistingDir() throws Exception {
    Path tblPath = new Path(testTbl.getStorageDescriptor().getLocation());
    setupMockWarehouseForPath(tblPath, true, true);

    when(glueClient.getDatabase(new GetDatabaseRequest().withName(testDb.getName())))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTable(new GetTableRequest().withDatabaseName(testTbl.getDatabaseName())
        .withName(testTbl.getName()))).thenThrow(new EntityNotFoundException(""));
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));

    metastoreClientDelegate.createTable(CatalogToHiveConverter.convertTable(testTbl, testTbl.getDatabaseName()));

    verify(glueClient, times(1)).createTable(any(CreateTableRequest.class));
    verify(wh).isDir(tblPath);
    verify(wh, never()).mkdirs(tblPath, true);
  }

  @Test
  public void testCreateTableWithExistingDirWithCatalogId() throws Exception {
    Path tblPath = new Path(testTbl.getStorageDescriptor().getLocation());
    setupMockWarehouseForPath(tblPath, true, true);

    when(glueClient.getDatabase(new GetDatabaseRequest()
        .withName(testDb.getName())
        .withCatalogId(CATALOG_ID)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTable(new GetTableRequest()
        .withDatabaseName(testTbl.getDatabaseName())
        .withCatalogId(CATALOG_ID)
        .withName(testTbl.getName()))).thenThrow(new EntityNotFoundException(""));
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));

    metastoreClientDelegateCatalogId.createTable(
        CatalogToHiveConverter.convertTable(testTbl, testTbl.getDatabaseName()));
    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(glueClient, times(1)).createTable(captor.capture());
    verify(wh).isDir(tblPath);
    verify(wh, never()).mkdirs(tblPath, true);
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
  }

  @Test
  public void testCreateTableWithoutExistingDir() throws Exception {
    Path tblPath = new Path(testTbl.getStorageDescriptor().getLocation());
    setupMockWarehouseForPath(tblPath, false, true);

    when(glueClient.getDatabase(new GetDatabaseRequest().withName(testDb.getName())))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTable(new GetTableRequest().withDatabaseName(testTbl.getDatabaseName())
        .withName(testTbl.getName()))).thenThrow(new EntityNotFoundException(""));
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    metastoreClientDelegate.createTable(CatalogToHiveConverter.convertTable(testTbl, testTbl.getDatabaseName()));

    verify(glueClient, times(1)).createTable(any(CreateTableRequest.class));
    verify(wh).isDir(tblPath);
    verify(wh).mkdirs(tblPath, true);
  }

  @Test (expected = org.apache.hadoop.hive.metastore.api.AlreadyExistsException.class)
  public void testCreateTableWithExistTable() throws Exception {
    setupMockWarehouseForPath(new Path(testTbl.getStorageDescriptor().getLocation()), true, false);
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(new GetTableResult().withTable(testTbl));
    metastoreClientDelegate.createTable(CatalogToHiveConverter.convertTable(testTbl, testTbl.getDatabaseName()));
  }

  @Test
  public void testAlterTable() throws Exception {
    org.apache.hadoop.hive.metastore.api.Table newHiveTable
        = CatalogToHiveConverter.convertTable(getTestTable(), testDb.getName());
    newHiveTable.setTableName(testTbl.getName());

    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));
    metastoreClientDelegateCatalogId.alterTable(
        testDb.getName(), testTbl.getName(), newHiveTable, null);

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(glueClient, times(1)).updateTable(captor.capture());

    TableInput expectedTableInput = GlueInputConverter.convertToTableInput(newHiveTable);
    assertEquals(expectedTableInput, captor.getValue().getTableInput());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testAlterTableRename() throws Exception {
    org.apache.hadoop.hive.metastore.api.Table newHiveTable
        = CatalogToHiveConverter.convertTable(getTestTable(), testDb.getName());
    metastoreClientDelegate.alterTable(testDb.getName(), testTbl.getName(), newHiveTable, null);
  }

  @Test
  public void testAlterTableSetExternalType() throws Exception {
    org.apache.hadoop.hive.metastore.api.Table newHiveTable
        = CatalogToHiveConverter.convertTable(getTestTable(), testDb.getName());
    newHiveTable.setTableType(MANAGED_TABLE.toString());
    newHiveTable.getParameters().put("EXTERNAL", "TRUE");

    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));
    metastoreClientDelegate.alterTable(
        testDb.getName(), newHiveTable.getTableName(), newHiveTable, null);

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(glueClient, times(1)).updateTable(captor.capture());
    assertEquals(EXTERNAL_TABLE.toString(), captor.getValue().getTableInput().getTableType());
  }

  @Test
  public void testAlterTableSetManagedType() throws Exception {
    org.apache.hadoop.hive.metastore.api.Table newHiveTable
        = CatalogToHiveConverter.convertTable(getTestTable(), testDb.getName());
    newHiveTable.setTableType(EXTERNAL_TABLE.toString());
    newHiveTable.getParameters().put("EXTERNAL", "FALSE");

    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));
    metastoreClientDelegate.alterTable(
        testDb.getName(), newHiveTable.getTableName(), newHiveTable, null);

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(glueClient, times(1)).updateTable(captor.capture());
    assertEquals(MANAGED_TABLE.toString(), captor.getValue().getTableInput().getTableType());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testListTableNamesByFilter() throws Exception {
    metastoreClientDelegate.listTableNamesByFilter("db", "filter", (short) 1);
  }

  @Test
  public void testDropTableWithDeleteData() throws Exception {
    Path tblPath = new Path(testTbl.getStorageDescriptor().getLocation());
    List<String> values = Lists.newArrayList("foo");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName()).withValues(values)
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());

    when(glueClient.getTable(new GetTableRequest()
        .withDatabaseName(testTbl.getDatabaseName()).withName(testTbl.getName())))
        .thenReturn(new GetTableResult().withTable(testTbl));
    when(glueClient.deletePartition(new DeletePartitionRequest()
        .withDatabaseName(testDb.getName()).withPartitionValues(values).withTableName(testTbl.getName())))
        .thenReturn(new DeletePartitionResult());
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(new GetPartitionsResult().withPartitions(partition));
    when(glueClient.getPartition(new GetPartitionRequest()
        .withDatabaseName(testDb.getName()).withTableName(testTbl.getName()).withPartitionValues(values)))
        .thenReturn(new GetPartitionResult().withPartition(partition));
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    metastoreClientDelegate.dropTable(
        testTbl.getDatabaseName(), testTbl.getName(), true, true, true);

    verify(glueClient).deleteTable(new DeleteTableRequest()
        .withDatabaseName(testTbl.getDatabaseName())
        .withName(testTbl.getName()));
    verify(wh).deleteDir(tblPath, true, true);
  }

  @Test
  public void testDropTableWithoutDeleteData() throws  Exception {
    Path tblPath = new Path(testTbl.getStorageDescriptor().getLocation());
    List<String> values = Lists.newArrayList("foo");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName()).withValues(values)
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());

    when(glueClient.getTable(new GetTableRequest()
        .withDatabaseName(testTbl.getDatabaseName()).withName(testTbl.getName())))
        .thenReturn(new GetTableResult().withTable(testTbl));
    when(glueClient.deletePartition(new DeletePartitionRequest()
        .withDatabaseName(testDb.getName()).withPartitionValues(values).withTableName(testTbl.getName())))
        .thenReturn(new DeletePartitionResult());
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(new GetPartitionsResult().withPartitions(partition));
    when(glueClient.getPartition(new GetPartitionRequest()
        .withDatabaseName(testDb.getName()).withTableName(testTbl.getName()).withPartitionValues(values)))
        .thenReturn(new GetPartitionResult().withPartition(partition));
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    metastoreClientDelegate.dropTable(
        testTbl.getDatabaseName(), testTbl.getName(), false, true, true);

    verify(glueClient).deleteTable(new DeleteTableRequest()
        .withDatabaseName(testTbl.getDatabaseName())
        .withName(testTbl.getName()));
    verify(wh, never()).deleteDir(tblPath, true, true);
  }

  @Test
  public void testDropExternalTableWithoutDeleteData() throws  Exception {
    Path tblPath = new Path(testTbl.getStorageDescriptor().getLocation());
    List<String> values = Lists.newArrayList("foo");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName()).withValues(values)
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());
    testTbl.getParameters().put("EXTERNAL", "TRUE");

    when(glueClient.getTable(new GetTableRequest()
        .withDatabaseName(testTbl.getDatabaseName()).withName(testTbl.getName())))
        .thenReturn(new GetTableResult().withTable(testTbl));
    when(glueClient.deletePartition(new DeletePartitionRequest()
        .withDatabaseName(testDb.getName()).withPartitionValues(values).withTableName(testTbl.getName())))
        .thenReturn(new DeletePartitionResult());
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(new GetPartitionsResult().withPartitions(partition));
    when(glueClient.getPartition(new GetPartitionRequest()
        .withDatabaseName(testDb.getName()).withTableName(testTbl.getName()).withPartitionValues(values)))
        .thenReturn(new GetPartitionResult().withPartition(partition));
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    metastoreClientDelegate.dropTable(
        testTbl.getDatabaseName(), testTbl.getName(), false, true, true);

    verify(glueClient).deleteTable(new DeleteTableRequest().withDatabaseName(
        testTbl.getDatabaseName()).withName(testTbl.getName()));
    verify(wh, never()).deleteDir(tblPath, true, true);
  }

  @Test
  public void testValidateTableAndCreateDirectoryVirtualView() throws Exception {
    testTbl.setTableType(TableType.VIRTUAL_VIEW.toString());
    testTbl.getStorageDescriptor().setLocation(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTbl = CatalogToHiveConverter.convertTable(
        testTbl, testTbl.getDatabaseName());

    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(new GetDatabaseResult().withDatabase(testDb));
    when(glueClient.getTable(new GetTableRequest()
        .withDatabaseName(testTbl.getDatabaseName()).withName(testTbl.getName())))
        .thenThrow(EntityNotFoundException.class);

    assertFalse(metastoreClientDelegate.validateNewTableAndCreateDirectory(hiveTbl));
    assertNull(testTbl.getStorageDescriptor().getLocation());
    verify(wh, never()).mkdirs(any(Path.class), anyBoolean());
  }

  // ======================= Partition =======================

  @Test
  public void testGetPartitionByValues() throws Exception {
    List<String> values = Lists.newArrayList("foo", "bar");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withValues(values)
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());
    GetPartitionRequest request = new GetPartitionRequest()
        .withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withPartitionValues(values);
    when(glueClient.getPartition(request)).thenReturn(new GetPartitionResult().withPartition(partition));
    org.apache.hadoop.hive.metastore.api.Partition result = metastoreClientDelegate.getPartition(
        testDb.getName(), testTbl.getName(), values);

    verify(glueClient, times(1)).getPartition(request);
    assertThat(result.getValues(), is(values));
  }

  @Test
  public void testGetPartitionByValuesWithCatalogId() throws Exception {
    List<String> values = Lists.newArrayList("foo", "bar");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withValues(values)
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());
    GetPartitionRequest request = new GetPartitionRequest()
        .withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withPartitionValues(values)
        .withCatalogId(CATALOG_ID);
    when(glueClient.getPartition(request)).thenReturn(new GetPartitionResult().withPartition(partition));
    org.apache.hadoop.hive.metastore.api.Partition result = metastoreClientDelegateCatalogId.getPartition(
        testDb.getName(), testTbl.getName(), values);

    ArgumentCaptor<GetPartitionRequest> captor = ArgumentCaptor.forClass(GetPartitionRequest.class);
    verify(glueClient, times(1)).getPartition(captor.capture());
    assertThat(result.getValues(), is(values));
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
  }

  @Test
  public void testGetPartitionByName() throws Exception {
    String partitionName = "/a=foo/b=bar";
    List<String> values = ImmutableList.of("foo", "bar");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withValues(values)
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
        .thenReturn(new GetPartitionResult().withPartition(partition));

    org.apache.hadoop.hive.metastore.api.Partition result
        = metastoreClientDelegate.getPartition(testDb.getName(), testTbl.getName(), partitionName);

    verify(glueClient).getPartition(any(GetPartitionRequest.class));
    assertThat(result.getValues(), is(values));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionEntityNotFound() throws Exception {
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
        .thenThrow(new EntityNotFoundException("Test exception: partition not found"));
    metastoreClientDelegate.getPartition(testDb.getName(), testTbl.getName(), "testPart");
    verify(glueClient, times(1)).getPartition(any(GetPartitionRequest.class));
  }

  @Test
  public void testGetPartitionsByNames() throws Exception {
    String partitionName = "/a=foo/b=bar";
    List<String> values = ImmutableList.of("foo", "bar");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withValues(values)
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());
    when(glueClient.batchGetPartition(any(BatchGetPartitionRequest.class)))
        .thenReturn(new BatchGetPartitionResult().withPartitions(partition));

    List<org.apache.hadoop.hive.metastore.api.Partition> result
        = metastoreClientDelegate.getPartitionsByNames(
            testDb.getName(), testTbl.getName(), ImmutableList.of(partitionName));

    verify(glueClient, times(1)).batchGetPartition(any(BatchGetPartitionRequest.class));
    assertNotNull(result);
    assertThat(Iterables.getOnlyElement(result).getValues(), is(values));
  }

  @Test
  public void testGetPartitionsByNamesWithCatalogId() throws Exception {
    String partitionName = "/a=foo/b=bar";
    List<String> values = ImmutableList.of("foo", "bar");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withValues(values)
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());
    when(glueClient.batchGetPartition(any(BatchGetPartitionRequest.class)))
        .thenReturn(new BatchGetPartitionResult().withPartitions(partition));

    List<org.apache.hadoop.hive.metastore.api.Partition> result
        = metastoreClientDelegateCatalogId.getPartitionsByNames(
            testDb.getName(), testTbl.getName(), ImmutableList.of(partitionName));

    ArgumentCaptor<BatchGetPartitionRequest> captor = ArgumentCaptor.forClass(BatchGetPartitionRequest.class);
    verify(glueClient, times(1)).batchGetPartition(captor.capture());
    assertNotNull(result);
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
  }

  @Test
  public void testGetPartitionsByNamePropagateException() throws Exception {
    String exceptionMessage = "Partition not found";
    when(glueClient.batchGetPartition(any(BatchGetPartitionRequest.class)))
        .thenThrow(new EntityNotFoundException(exceptionMessage));

    try {
      metastoreClientDelegate.getPartitionsByNames(
          testDb.getName(), testTbl.getName(), ImmutableList.of("/a=foo/b=bar"));
    } catch (Exception e) {
      assertThat(e, instanceOf(NoSuchObjectException.class));
      assertThat(e.getMessage(), containsString(exceptionMessage));
    }
    verify(glueClient, times(1)).batchGetPartition(any(BatchGetPartitionRequest.class));
  }

  @Test
  public void testGetPartitionsByNameTwoPages() throws Exception {
    int numPartNames = BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE + 10;
    List<String> partNames = getTestPartitionNames(numPartNames);

    when(glueClient.batchGetPartition(any(BatchGetPartitionRequest.class)))
        .thenReturn(new BatchGetPartitionResult().withPartitions(ImmutableList.<Partition>of()));

    metastoreClientDelegate.getPartitionsByNames(testDb.getName(), testTbl.getName(), partNames);
    verify(glueClient, times(2)).batchGetPartition(any(BatchGetPartitionRequest.class));
  }

  private static List<String> getTestPartitionNames(int numPartitions) {
    List<String> partNames = Lists.newArrayList();
    for (int i = 1; i < numPartitions; i++) {
      partNames.add(String.format("a=%d", i));
    }
    return partNames;
  }

  @Test
  public void testGetPartitions() throws Exception {
    List<String> expectedValues = Lists.newArrayList("foo", "bar");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
            .withTableName(testTbl.getName())
            .withValues(expectedValues);
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
            .thenReturn(new GetPartitionsResult().withPartitions(Lists.newArrayList(partition)));

    List<org.apache.hadoop.hive.metastore.api.Partition> res = metastoreClientDelegate.getPartitions(
            testDb.getName(), testTbl.getName(), null, 10);

    verify(glueClient, times(1)).getPartitions(any(GetPartitionsRequest.class));
    assertFalse(CollectionUtils.isEmpty(res));
    List<String> values = Iterables.getOnlyElement(res).getValues();
    assertThat(values, is(expectedValues));
  }

  @Test
  public void testGetPartitionsParallel() throws Exception {
    final int numSegments = 2;
    HiveConf hiveConf = new HiveConf(this.conf);
    hiveConf.setInt(GlueMetastoreClientDelegate.NUM_PARTITION_SEGMENTS_CONF, numSegments);
    GlueMetastoreClientDelegate delegate = new GlueMetastoreClientDelegate(
        hiveConf, new DefaultAWSGlueMetastore(hiveConf, glueClient), wh, lockManager);

    final Set<List<String>> expectedValues = Sets.newHashSet();
    final List<Partition> partitions = Lists.newArrayList();
    final int numPartitions = DefaultAWSGlueMetastore.GET_PARTITIONS_MAX_SIZE + 10;
    final int maxPartitionsToRequest = numPartitions - 1;

    for (int i = 1; i <= numPartitions; i++) {
      List<String> partitionKeys = Arrays.asList("keyA:" + i, "keyB:" + i);
      if (i <= maxPartitionsToRequest) {
        expectedValues.add(partitionKeys);
      }
      Partition partition = new Partition().withDatabaseName(testDb.getName())
              .withTableName(testTbl.getName())
              .withValues(partitionKeys);
      partitions.add(partition);
    }

    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenAnswer(new Answer<GetPartitionsResult>() {
          @Override
          public GetPartitionsResult answer(InvocationOnMock invocation) {
            GetPartitionsRequest request = invocation.getArgumentAt(0, GetPartitionsRequest.class);
            GetPartitionsResult result;
            if (request.getSegment() == null) {
              fail("Should pass in segment");
            }
            switch (request.getSegment().getSegmentNumber()) {
              case 0:
                result = new GetPartitionsResult().withPartitions(partitions.subList(0, numPartitions / 2));
                break;
              case 1:
                result = new GetPartitionsResult().withPartitions(
                    partitions.subList(numPartitions / 2, partitions.size()));
                break;
              default:
                result = new GetPartitionsResult().withPartitions(Collections.<Partition>emptyList());
                fail("Got segmentNumber >= " + numSegments);
            }
            return result;
          }
        });

    List<org.apache.hadoop.hive.metastore.api.Partition> res = delegate.getPartitions(
        testDb.getName(), testTbl.getName(), null, maxPartitionsToRequest);

    verify(glueClient, times(numSegments))
            .getPartitions(any(GetPartitionsRequest.class));
    assertFalse(CollectionUtils.isEmpty(res));
    Iterable<List<String>> values = Iterables.transform(res,
        new Function<org.apache.hadoop.hive.metastore.api.Partition, List<String>>() {
          public List<String> apply(org.apache.hadoop.hive.metastore.api.Partition partition) {
            return partition.getValues();
          }
        });
    assertThat(Sets.newHashSet(values), is(expectedValues));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionsPartialFailure() throws Exception {
    List<String> partitionKeys1 = Arrays.asList("foo1", "bar1");
    final Partition partition1 = new Partition().withDatabaseName(testDb.getName())
            .withTableName(testTbl.getName())
            .withValues(partitionKeys1);

    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
            .thenAnswer(new Answer<GetPartitionsResult>() {
              @Override
              public GetPartitionsResult answer(InvocationOnMock invocation) {
                GetPartitionsRequest request = invocation.getArgumentAt(0, GetPartitionsRequest.class);
                GetPartitionsResult result;
                switch (request.getSegment().getSegmentNumber()) {
                  case 0:
                    result = new GetPartitionsResult().withPartitions(Lists.newArrayList(partition1));
                    break;
                  default:
                    throw new OperationTimeoutException("timeout");
                }
                return result;
              }
            });

    List<org.apache.hadoop.hive.metastore.api.Partition> res = metastoreClientDelegate.getPartitions(
            testDb.getName(), testTbl.getName(), null, -1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTooHighGluePartitionSegments() throws MetaException {
    HiveConf hiveConf = new HiveConf(this.conf);
    hiveConf.setInt(GlueMetastoreClientDelegate.NUM_PARTITION_SEGMENTS_CONF,
            DefaultAWSGlueMetastore.MAX_NUM_PARTITION_SEGMENTS + 1);
    GlueMetastoreClientDelegate delegate = new GlueMetastoreClientDelegate(
        hiveConf, new DefaultAWSGlueMetastore(hiveConf, glueClient), wh, lockManager);
  }

  @Test
  public void testDropPartitionUsingValues() throws Exception {
    List<String> values = Lists.newArrayList("foo", "bar");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withValues(values)
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());
    DeletePartitionRequest request = new DeletePartitionRequest()
        .withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withPartitionValues(values);

    when(glueClient.deletePartition(request)).thenReturn(new DeletePartitionResult());
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
        .thenReturn(new GetPartitionResult().withPartition(partition));
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(new GetTableResult().withTable(testTbl));

    metastoreClientDelegate.dropPartition(
        testDb.getName(), testTbl.getName(), values, false, false, false);
    verify(glueClient, times(1)).deletePartition(request);
  }

  @Test
  public void testDropPartitionUsingValuesWithCatalogId() throws Exception {
    List<String> values = Lists.newArrayList("foo", "bar");
    Partition partition = new Partition().withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withValues(values)
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());
    DeletePartitionRequest request = new DeletePartitionRequest()
        .withDatabaseName(testDb.getName())
        .withTableName(testTbl.getName())
        .withPartitionValues(values);

    when(glueClient.deletePartition(request)).thenReturn(new DeletePartitionResult());
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
        .thenReturn(new GetPartitionResult().withPartition(partition));
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(new GetTableResult().withTable(testTbl));

    metastoreClientDelegateCatalogId.dropPartition(
        testDb.getName(), testTbl.getName(), values, false, false, false);
    ArgumentCaptor<DeletePartitionRequest> captor = ArgumentCaptor.forClass(DeletePartitionRequest.class);
    verify(glueClient, times(1)).deletePartition(captor.capture());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
  }

  @Test
  public void testAppendPartition() throws Exception {
    List<String> values = ImmutableList.of("foo");
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(new GetTableResult().withTable(testTbl));
    Path partLocation = new Path(testTbl.getStorageDescriptor().getLocation(),
        Warehouse.makePartName(CatalogToHiveConverter.convertFieldSchemaList(testTbl.getPartitionKeys()), values));
    setupMockWarehouseForPath(partLocation, false, true);
    mockBatchCreatePartitionsSucceed();

    org.apache.hadoop.hive.metastore.api.Partition res =
        metastoreClientDelegate.appendPartition(testDb.getName(), testTbl.getName(), values);

    verify(wh, times(1)).mkdirs(partLocation, true);
    assertThat(res.getValues(), is(values));
  }

  @Test
  public void testAddPartitionsEmpty() throws Exception {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = Lists.newArrayList();
    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated =
        metastoreClientDelegate.addPartitions(partitions, false, true);

    verify(glueClient, never()).getTable(any(GetTableRequest.class));
    verify(glueClient, never()).batchCreatePartition(any(BatchCreatePartitionRequest.class));
    assertTrue(CollectionUtils.isEmpty(partitionsCreated));
    assertDaemonThreadPools();
  }

  @Test
  public void testAddPartitions() throws Exception {
    mockBatchCreatePartitionsSucceed();
    setupMockWarehouseForPath(new Path(testTbl.getStorageDescriptor()
        .getLocation().toString()), false, true);
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));

    int numPartitions = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);
    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated =
        metastoreClientDelegate.addPartitions(partitions, false, true);

    verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
    verify(glueClient, times(1)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
    verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
    verify(wh, never()).deleteDir(any(Path.class), eq(true));
    assertEquals(numPartitions, partitionsCreated.size());
    assertEquals(new HashSet<org.apache.hadoop.hive.metastore.api.Partition>(partitionsCreated),
            new HashSet<org.apache.hadoop.hive.metastore.api.Partition>(partitions));
    assertDaemonThreadPools();
  }

  @Test
  public void testAddPartitionsEmptyPartitionLocation() throws Exception {
    // Case: table contains location & partition location is empty.
    // Test that created partitions contains location
    int numPartitions = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated =
        addPartitionsWithEmptyLocationsValid(numPartitions);
    verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
    for (org.apache.hadoop.hive.metastore.api.Partition part : partitionsCreated) {
      assertThat(part.getSd().getLocation(), notNullValue());
    }
    assertDaemonThreadPools();
  }

  @Test
  public void testAddPartitionsEmptyTableAndPartitionLocation() throws Exception {
    // Case: table location is empty (VIRTUAL_VIEW) & partition location is empty.
    // Test that created partitions does not contain location as these are Views.
    testTbl.getStorageDescriptor().setLocation(null);
    int numPartitions = 1;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated =
        addPartitionsWithEmptyLocationsValid(numPartitions);
    verify(wh, never()).mkdirs(any(Path.class), anyBoolean());
    assertThat(partitionsCreated.get(0).getSd().getLocation(), nullValue());
    assertDaemonThreadPools();
  }

  private List<org.apache.hadoop.hive.metastore.api.Partition> addPartitionsWithEmptyLocationsValid(
      int numPartitions) throws Exception {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);
    for (org.apache.hadoop.hive.metastore.api.Partition partition : partitions) {
      partition.getSd().setLocation(null);
    }
    mockBatchCreatePartitionsSucceed();
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));
    when(wh.mkdirs(any(Path.class), anyBoolean())).thenReturn(true);

    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated =
        metastoreClientDelegate.addPartitions(partitions, false, true);
    verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
    verify(glueClient, times(1)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
    verify(wh, never()).deleteDir(any(Path.class), anyBoolean());
    assertEquals(numPartitions, partitionsCreated.size());
    assertEquals(new HashSet<org.apache.hadoop.hive.metastore.api.Partition>(partitionsCreated),
            new HashSet<org.apache.hadoop.hive.metastore.api.Partition>(partitions));
    return partitionsCreated;
  }

  @Test(expected = MetaException.class)
  public void testAddPartitions_PartitionViewWithLocation() throws Exception {
    // Case: table location is empty (VIRTUAL_VIEW) with partition containing location
    // In Hive, this throws MetaException because it doesn't allow parititon views to have location
    Table table = testTbl;
    table.getStorageDescriptor().setLocation(null);

    int numPartitions = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);

    mockBatchCreatePartitionsSucceed();
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(table));
    when(wh.mkdirs(any(Path.class), anyBoolean())).thenReturn(true);

    metastoreClientDelegate.addPartitions(partitions, false, true);

    assertDaemonThreadPools();
  }

  @Test
  public void testAddPartitionsDoNotNeedResult() throws Exception {
    mockBatchCreatePartitionsSucceed();
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));

    int numPartitions = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);
    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated =
        metastoreClientDelegate.addPartitions(partitions, false, false);

    verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
    verify(glueClient, times(1)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
    verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
    verify(wh, never()).deleteDir(any(Path.class), eq(true));
    assertThat(partitionsCreated, is(nullValue()));
    assertDaemonThreadPools();
  }

  @Test
  public void testAddPartitionsTwoPages() throws Exception {
    mockBatchCreatePartitionsSucceed();
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));

    int numPartitions = (int) (BATCH_CREATE_PARTITIONS_MAX_REQUEST_SIZE * 1.2);
    int expectedBatches = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);
    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated =
        metastoreClientDelegate.addPartitions(partitions, false, true);

    verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
    verify(glueClient, times(expectedBatches)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
    verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
    verify(wh, never()).deleteDir(any(Path.class), eq(true));
    assertEquals(numPartitions, partitionsCreated.size());
    assertEquals(new HashSet<org.apache.hadoop.hive.metastore.api.Partition>(partitionsCreated),
            new HashSet<org.apache.hadoop.hive.metastore.api.Partition>(partitions));
    assertDaemonThreadPools();
  }

  @Test
  public void testAddPartitionsTwoPagesWithCatalogId() throws Exception {
    mockBatchCreatePartitionsSucceed();
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));

    int numPartitions = (int) (BATCH_CREATE_PARTITIONS_MAX_REQUEST_SIZE * 1.2);
    int expectedBatches = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);
    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated =
        metastoreClientDelegateCatalogId.addPartitions(partitions, false, true);
    ArgumentCaptor<BatchCreatePartitionRequest> captor = ArgumentCaptor.forClass(BatchCreatePartitionRequest.class);
    verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
    verify(glueClient, times(expectedBatches)).batchCreatePartition(captor.capture());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
    verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
    verify(wh, never()).deleteDir(any(Path.class), eq(true));
    assertEquals(numPartitions, partitionsCreated.size());
    assertEquals(new HashSet<org.apache.hadoop.hive.metastore.api.Partition>(partitionsCreated),
            new HashSet<org.apache.hadoop.hive.metastore.api.Partition>(partitions));
    assertDaemonThreadPools();
  }

  @Test
  public void testAddPartitionsFailedServiceException() throws Exception {
    int numPartitions = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);
    List<String> values = partitions.get(0).getValues();
    when(glueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenReturn(new BatchCreatePartitionResult().withErrors(ObjectTestUtils.getPartitionError(values,
        new InternalServiceException("exception"))));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));

    try {
      metastoreClientDelegate.addPartitions(partitions, false, true);
      fail("should throw");
    } catch (Exception e) {
      assertThat(e, is(instanceOf(MetaException.class)));
      verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
      verify(glueClient, times(1))
          .batchCreatePartition(any(BatchCreatePartitionRequest.class));
      verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
      verify(wh, times(1)).deleteDir(any(Path.class), eq(true));
      assertDaemonThreadPools();
    }
  }

  @Test
  public void testAddPartitionsFailedAlreadyExistsException() throws Exception {
    int numPartitions = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);
    List<String> values = ImmutableList.of("foo1");

    when(glueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenReturn(new BatchCreatePartitionResult().withErrors(ObjectTestUtils.getPartitionError(values,
        new com.amazonaws.services.glue.model.AlreadyExistsException("exception"))));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));

    try {
      metastoreClientDelegate.addPartitions(partitions, false, true);
      fail("Should throw");
    } catch (Exception e) {
      assertThat(e, is(instanceOf(org.apache.hadoop.hive.metastore.api.AlreadyExistsException.class)));
      verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
      verify(glueClient, times(1))
          .batchCreatePartition(any(BatchCreatePartitionRequest.class));
      verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
      verify(wh, times(1)).deleteDir(any(Path.class), eq(true));
      assertDaemonThreadPools();
    }
  }

  @Test
  public void testAddPartitionsThrowsEntityNotFoundException() throws Exception {
    when(glueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenThrow(new EntityNotFoundException("exception"));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));

    int numPartitions = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);

    try {
      metastoreClientDelegate.addPartitions(partitions, false, true);
      fail("Should throw");
    } catch (Exception e) {
      assertThat(e, is(instanceOf(NoSuchObjectException.class)));
      verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
      verify(glueClient, times(1))
          .batchCreatePartition(any(BatchCreatePartitionRequest.class));
      verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
      verify(wh, times(numPartitions)).deleteDir(any(Path.class), eq(true));
      assertDaemonThreadPools();
    }
  }

  @Test
  public void testAddPartitionsThrowsExceptionSecondPage() throws Exception {
    int numPartitions = 200;
    int secondPageSize = numPartitions - BATCH_CREATE_PARTITIONS_MAX_REQUEST_SIZE;
    when(glueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenReturn(new BatchCreatePartitionResult())
        .thenThrow(new InvalidInputException("exception"));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));

    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);

    try {
      metastoreClientDelegate.addPartitions(partitions, false, true);
      fail("Should throw");
    } catch (Exception e) {
      assertThat(e, is(instanceOf(InvalidObjectException.class)));
      verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
      verify(glueClient, times(2))
          .batchCreatePartition(any(BatchCreatePartitionRequest.class));
      verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
      verify(wh, times(secondPageSize)).deleteDir(any(Path.class), eq(true));
      assertDaemonThreadPools();
    }
  }

  @Test
  public void testAddPartitionsIfNotExists() throws Exception {
    List<String> values = ImmutableList.of("foo1");
    when(glueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenReturn(new BatchCreatePartitionResult().withErrors(ObjectTestUtils.getPartitionError(values,
        new com.amazonaws.services.glue.model.AlreadyExistsException("exception"))));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));

    int numPartitions = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);
    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated =
        metastoreClientDelegate.addPartitions(partitions, true, true);

    verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
    verify(glueClient, times(1)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
    verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
    verify(wh, never()).deleteDir(any(Path.class), eq(true));
    assertEquals(1, partitionsCreated.size());
    assertTrue(partitions.contains(partitionsCreated.get(0)));
    assertDaemonThreadPools();
  }

  @Test
  public void testAddPartitionsKeysAndValuesNotMatch() throws Exception {
    int numPartitions = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);
    // make the partition value size inconsistent with key size
    partitions.get(1).setValues(Lists.newArrayList("foo1", "bar1"));

    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(new GetTableResult().withTable(testTbl));

    try {
      metastoreClientDelegate.addPartitions(partitions, true, true);
      fail("should throw");
    } catch (IllegalArgumentException e) {
      verify(wh, never()).getDnsPath(any(Path.class));
      assertDaemonThreadPools();
    }
  }

  @Test
  public void testAddPartitionsDeleteAddedPathsWhenAddPathFail() throws Exception {
    int numPartitions = 2;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(numPartitions);

    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));
    when(wh.isDir(any(Path.class))).thenReturn(false);
    when(wh.mkdirs(any(Path.class), eq(true))).thenReturn(true).thenReturn(false); // succeed first, then fail

    try {
      metastoreClientDelegate.addPartitions(partitions, true, true);
      fail("should throw");
    } catch (MetaException e) {
      verify(wh, times(numPartitions)).getDnsPath(any(Path.class));
      verify(wh, times(numPartitions)).isDir(any(Path.class));
      verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
      verify(wh, times(1)).deleteDir(any(Path.class), eq(true));
      assertDaemonThreadPools();
    }
  }

  @Test
  public void testAddPartitionsCallGetPartitionForInternalServiceException() throws Exception {
    int numPartitions = 3;
    String dbName = testDb.getName();
    String tableName = testTbl.getName();
    List<String> values1 = Lists.newArrayList("val1");
    List<String> values2 = Lists.newArrayList("val2");
    List<String> values3 = Lists.newArrayList("val3");
    Partition partition1 = ObjectTestUtils.getTestPartition(dbName, tableName, values1);
    Partition partition2 = ObjectTestUtils.getTestPartition(dbName, tableName, values2);
    Partition partition3 = ObjectTestUtils.getTestPartition(dbName, tableName, values3);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2, partition3);

    when(glueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenThrow(new InternalServiceException("InternalServiceException"));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(testTbl));
    when(glueClient.getPartition(new GetPartitionRequest()
        .withDatabaseName(dbName)
        .withTableName(tableName)
        .withPartitionValues(partition1.getValues())))
        .thenReturn(new GetPartitionResult().withPartition(partition1));
    when(glueClient.getPartition(new GetPartitionRequest()
        .withDatabaseName(dbName)
        .withTableName(tableName)
        .withPartitionValues(partition2.getValues())))
        .thenThrow(new EntityNotFoundException("EntityNotFoundException"));
    when(glueClient.getPartition(new GetPartitionRequest()
        .withDatabaseName(dbName)
        .withTableName(tableName)
        .withPartitionValues(partition3.getValues())))
        .thenThrow(new NullPointerException("NullPointerException"));

    try {
      metastoreClientDelegate.addPartitions(CatalogToHiveConverter
          .convertPartitions(partitions), false, true);
      fail("Should throw");
    } catch (Exception e) {
      assertThat(e, is(instanceOf(MetaException.class)));
      verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
      verify(glueClient, times(1))
          .batchCreatePartition(any(BatchCreatePartitionRequest.class));
      verify(glueClient, times(numPartitions)).getPartition(any(GetPartitionRequest.class));
      verify(wh, times(numPartitions)).mkdirs(any(Path.class), eq(true));
      verify(wh, times(2)).deleteDir(any(Path.class), eq(true));
      assertDaemonThreadPools();
    }
  }

  private void mockBatchCreatePartitionsSucceed() {
    when(glueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenReturn(new BatchCreatePartitionResult());
  }

  private List<org.apache.hadoop.hive.metastore.api.Partition> getTestPartitions(int count) {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      List<String> values = ImmutableList.of("foo" + i);
      Partition partition = ObjectTestUtils.getTestPartition(testDb.getName(), testTbl.getName(), values);
      partitions.add(CatalogToHiveConverter.convertPartition(partition));
    }
    return partitions;
  }

  @Test
  public void testAlterPartitions() throws Exception {
    List<String> values = ImmutableList.of("foo", "bar");
    Partition partition = getTestPartition(testTbl.getDatabaseName(), testTbl.getName(), values);
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = CatalogToHiveConverter.convertPartition(partition);
    PartitionInput input = GlueInputConverter.convertToPartitionInput(partition);
    UpdatePartitionRequest request = new UpdatePartitionRequest()
        .withDatabaseName(testTbl.getDatabaseName())
        .withTableName(testTbl.getName())
        .withPartitionInput(input)
        .withPartitionValueList(partition.getValues());

    when(glueClient.updatePartition(request)).thenReturn(new UpdatePartitionResult());
    metastoreClientDelegate.alterPartitions(testDb.getName(), testTbl.getName(), ImmutableList.of(hivePartition));

    verify(glueClient, times(1)).updatePartition(any(UpdatePartitionRequest.class));
  }

  @Test
  public void testAlterParititonDDLTimeUpdated() throws Exception {
    List<String> values = ImmutableList.of("foo", "bar");
    org.apache.hadoop.hive.metastore.api.Partition partition
        = CatalogToHiveConverter.convertPartition(
            getTestPartition(testTbl.getDatabaseName(), testTbl.getName(), values));
    metastoreClientDelegate.alterPartitions(
        testTbl.getDatabaseName(), testTbl.getName(), Lists.newArrayList(partition));

    ArgumentCaptor<UpdatePartitionRequest> captor = ArgumentCaptor.forClass(UpdatePartitionRequest.class);
    verify(glueClient, times(1)).updatePartition(captor.capture());
    assertTrue(captor.getValue().getPartitionInput().getParameters().containsKey(hive_metastoreConstants.DDL_TIME));
  }

  // =================== Roles & Privilege ===================

  @Test(expected = UnsupportedOperationException.class)
  public void testGrantPublicRole() throws Exception {
    metastoreClientDelegate.grantRole("public", "user",
        PrincipalType.USER, "grantor",
        PrincipalType.ROLE, true);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRevokeRole() throws Exception {
    metastoreClientDelegate.revokeRole("role", "user",
        PrincipalType.USER, true);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCreateRole() throws Exception {
    metastoreClientDelegate.createRole(new org.apache.hadoop.hive.metastore.api.Role(
        "role", (int) (new Date().getTime() / 1000), "owner"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCreatePublicRole() throws Exception {
    metastoreClientDelegate.createRole(new org.apache.hadoop.hive.metastore.api.Role(
        "public", (int) (new Date().getTime() / 1000), "owner"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDropRole() throws Exception {
    metastoreClientDelegate.dropRole("role");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDropPublicRole() throws Exception {
    metastoreClientDelegate.dropRole("public");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDropAdminRole() throws Exception {
    metastoreClientDelegate.dropRole("admin");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testListRolesWithRolePrincipalType() throws Exception {
    metastoreClientDelegate.listRoles("user", PrincipalType.ROLE);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetPrincipalsInRole() throws Exception {
    metastoreClientDelegate.getPrincipalsInRole(
        new org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest("role"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRoleGrantsForPrincipal() throws Exception {
    metastoreClientDelegate.getRoleGrantsForPrincipal(
        new org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest("user",
            PrincipalType.USER));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGrantRole() throws Exception {
    metastoreClientDelegate.grantRole("role", "user",
        PrincipalType.USER, "grantor",
        PrincipalType.ROLE, true);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGrantPrivileges() throws Exception {
    metastoreClientDelegate.grantPrivileges(ObjectTestUtils.getPrivilegeBag());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRevokePrivileges() throws Exception {
    metastoreClientDelegate.revokePrivileges(ObjectTestUtils.getPrivilegeBag(), false);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testListPrivileges() throws Exception {
    String principal = "user1";
    PrincipalType principalType =
        PrincipalType.USER;

    metastoreClientDelegate.listPrivileges(principal, principalType, ObjectTestUtils.getHiveObjectRef());
  }

  @Test
  public void testGetPrincipalPrivilegeSet() throws Exception {
    String user = "user1";
    List<String> groupList = ImmutableList.of();
    org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet privilegeSet = metastoreClientDelegate
        .getPrivilegeSet(ObjectTestUtils.getHiveObjectRef(), user, groupList);

    assertThat(privilegeSet, is(nullValue()));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGrantPrivilegesThrowingMetaException() throws Exception {
    metastoreClientDelegate.grantPrivileges(ObjectTestUtils.getPrivilegeBag());
  }

  // ====================== Statistics ======================

  @Test(expected = UnsupportedOperationException.class)
  public void testDeletePartitionColumnStatisticsValid() throws Exception {
    String databaseName = "database-name";
    String tableName = "table-name";
    String partitionName = "A=a/B=b";
    String columnName = "column-name";

    metastoreClientDelegate.deletePartitionColumnStatistics(databaseName, tableName, partitionName, columnName);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeleteTableColumnStatistics() throws Exception {
    String databaseName = "database-name";
    String tableName = "table-name";
    String columnName = "column-name";

    metastoreClientDelegate.deleteTableColumnStatistics(databaseName, tableName, columnName);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetPartitionColumnStatisticsValid() throws Exception {
    String databaseName = "database-name";
    String tableName = "table-name";
    List<String> partitionNames = ImmutableList.of("A=a/B=b", "A=x/B=y");
    List<String> columnNames = ImmutableList.of("decimal-column", "string-column");

    metastoreClientDelegate.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetTableColumnStatistics() throws Exception {
    String databaseName = "database-name";
    String tableName = "table-name";
    List<String> columnNames = ImmutableList.of("decimal-column", "string-column");

    metastoreClientDelegate.getTableColumnStatistics(databaseName, tableName, columnNames);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUpdatePartitionColumnStatistics() throws Exception {
    org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics =
        ObjectTestUtils.getHivePartitionColumnStatistics();

    metastoreClientDelegate.updatePartitionColumnStatistics(columnStatistics);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUpdateTableColumnStatistics() throws Exception {
    org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics =
        ObjectTestUtils.getHiveTableColumnStatistics();

    metastoreClientDelegate.updateTableColumnStatistics(columnStatistics);
  }

  private void assertDaemonThreadPools() {
    String threadNameCreatePrefix =
        GlueMetastoreClientDelegate.GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT.substring(0,
            GlueMetastoreClientDelegate.GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT.indexOf('%'));
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      String threadName = thread.getName();
      if (threadName != null && threadName.startsWith(threadNameCreatePrefix)) {
        assertTrue(thread.isDaemon());
      }
    }
  }

  //==================== Functions =====================

  @Test
  public void getFunction() throws Exception {
    UserDefinedFunction udf = createUserDefinedFunction();
    when(glueClient.getUserDefinedFunction(any(GetUserDefinedFunctionRequest.class))).thenReturn(
        new GetUserDefinedFunctionResult().withUserDefinedFunction(udf));
    metastoreClientDelegateCatalogId.getFunction(testDb.getName(), "test-func");
    ArgumentCaptor<GetUserDefinedFunctionRequest> captor =
        ArgumentCaptor.forClass(GetUserDefinedFunctionRequest.class);
    verify(glueClient, times(1)).getUserDefinedFunction(captor.capture());
    GetUserDefinedFunctionRequest request = captor.getValue();
    assertEquals(CATALOG_ID, request.getCatalogId());
    assertEquals(testDb.getName(), request.getDatabaseName());
    assertEquals("test-func", request.getFunctionName());
  }

  @Test
  public void getFunctions() throws Exception {
    UserDefinedFunction udf1 = createUserDefinedFunction();
    UserDefinedFunction udf2 = createUserDefinedFunction();

    List<UserDefinedFunction> udfList = new ArrayList<>();
    udfList.add(udf1);
    udfList.add(udf2);

    when(glueClient.getUserDefinedFunctions(any(GetUserDefinedFunctionsRequest.class))).thenReturn(
        new GetUserDefinedFunctionsResult().withUserDefinedFunctions(udfList).withNextToken(null));
    List<String> result = metastoreClientDelegateCatalogId.getFunctions(testDb.getName(), "test-func");
    ArgumentCaptor<GetUserDefinedFunctionsRequest> captor = ArgumentCaptor
        .forClass(GetUserDefinedFunctionsRequest.class);
    verify(glueClient, times(1)).getUserDefinedFunctions(captor.capture());
    GetUserDefinedFunctionsRequest request = captor.getValue();
    assertEquals(CATALOG_ID, request.getCatalogId());
    assertEquals(testDb.getName(), request.getDatabaseName());
    assertEquals("test-func", request.getPattern());
    assertEquals(2, result.size());
  }

  @Test
  public void testCreateFunction() throws Exception {
    org.apache.hadoop.hive.metastore.api.Function hiveFunction = createHiveFunction();
    metastoreClientDelegateCatalogId.createFunction(hiveFunction);
    ArgumentCaptor<CreateUserDefinedFunctionRequest> captor = ArgumentCaptor
        .forClass(CreateUserDefinedFunctionRequest.class);
    verify(glueClient, times(1)).createUserDefinedFunction(captor.capture());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
  }

  @Test
  public void testDropFunction() throws Exception {
    metastoreClientDelegateCatalogId.dropFunction(testDb.getName(), "test-func");
    ArgumentCaptor<DeleteUserDefinedFunctionRequest> captor = ArgumentCaptor
        .forClass(DeleteUserDefinedFunctionRequest.class);
    verify(glueClient, times(1)).deleteUserDefinedFunction(captor.capture());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
  }

  @Test
  public void testAlterFunction() throws Exception {
    org.apache.hadoop.hive.metastore.api.Function hiveFunction = createHiveFunction();
    metastoreClientDelegateCatalogId.alterFunction(testDb.getName(), "test-func", createHiveFunction());
    ArgumentCaptor<UpdateUserDefinedFunctionRequest> captor = ArgumentCaptor
        .forClass(UpdateUserDefinedFunctionRequest.class);
    verify(glueClient, times(1)).updateUserDefinedFunction(captor.capture());
    UpdateUserDefinedFunctionRequest request = captor.getValue();
    assertEquals(testDb.getName(), request.getDatabaseName());
    assertEquals("test-func", request.getFunctionName());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
  }

  private org.apache.hadoop.hive.metastore.api.Function createHiveFunction() {
    org.apache.hadoop.hive.metastore.api.Function hiveFunction = new org.apache.hadoop.hive.metastore.api.Function();
    hiveFunction.setClassName("testClass");
    hiveFunction.setFunctionName("test-func");
    hiveFunction.setOwnerName("test-owner");
    hiveFunction.setOwnerType(PrincipalType.USER);
    return hiveFunction;
  }

  private UserDefinedFunction createUserDefinedFunction() {
    UserDefinedFunction udf = new UserDefinedFunction();
    udf.setFunctionName("test-func");
    udf.setClassName("test-class");
    udf.setCreateTime(new Date());
    udf.setOwnerName("test-owner");
    udf.setOwnerType(com.amazonaws.services.glue.model.PrincipalType.USER.name());
    return udf;
  }

  // ==================== Schema =====================
  @Test
  public void testGetFields() throws Exception {
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(new GetTableResult().withTable(testTbl));
    List<FieldSchema> res = metastoreClientDelegateCatalogId.getFields(testDb.getName(), testTbl.getName());
    ArgumentCaptor<GetTableRequest> captor = ArgumentCaptor.forClass(GetTableRequest.class);
    verify(glueClient, times(1)).getTable(captor.capture());
    GetTableRequest request = captor.getValue();
    assertEquals(testDb.getName(), request.getDatabaseName());
    assertEquals(testTbl.getName(), request.getName());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
    assertEquals(1, res.size());
  }

  @Test
  public void testGetSchema() throws Exception {
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(new GetTableResult().withTable(testTbl));
    List<FieldSchema> res = metastoreClientDelegateCatalogId.getFields(testDb.getName(), testTbl.getName());
    ArgumentCaptor<GetTableRequest> captor = ArgumentCaptor.forClass(GetTableRequest.class);
    verify(glueClient, times(1)).getTable(captor.capture());
    GetTableRequest request = captor.getValue();
    assertEquals(testDb.getName(), request.getDatabaseName());
    assertEquals(testTbl.getName(), request.getName());
    assertEquals(CATALOG_ID, captor.getValue().getCatalogId());
    assertEquals(1, res.size());
  }
}
