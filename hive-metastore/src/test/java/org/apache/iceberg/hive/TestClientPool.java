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

package org.apache.iceberg.hive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

public class TestClientPool {

  @Test(expected = RuntimeException.class)
  public void testNewClientFailure() throws Exception {
    try (MockClientPool pool = new MockClientPool(2, Exception.class)) {
      pool.run(Object::toString);
    }
  }

  @Test
  public void testConnectionFailureRecovery() throws Exception {
    MockHiveClientPool clients = new MockHiveClientPool(2, TTransportException.class);

    // Throw a TTransportException wrapped by MetaException
    clients.run(client -> client.getAllTables("defalut"));
    clients.run(client -> client.getTableMeta("defalut", null, null));
    clients.run(client -> client.getTables("defalut", null));
    clients.run(client -> client.getTables("defalut", null, null));
    clients.run(client -> client.getDatabases("defalut"));
    clients.run(client -> client.getAllDatabases());

    // Throw a TTransportException
    clients.run(client -> client.tableExists("defalut", "any"));
  }

  private static class MockClientPool extends ClientPool<Object, Exception> {

    MockClientPool(int poolSize, Class<? extends Exception> reconnectExc) {
      super(poolSize, reconnectExc);
    }

    @Override
    protected Object newClient() {
      throw new RuntimeException();
    }

    @Override
    protected Object reconnect(Object client) {
      return null;
    }

    @Override
    protected void close(Object client) {

    }
  }

  private static class MockHiveClientPool extends ClientPool<IMetaStoreClient, Exception> {

    MockHiveClientPool(int poolSize, Class<? extends Exception> reconnectExc) {
      super(poolSize, reconnectExc);
    }

    @Override
    protected IMetaStoreClient newClient() {
      return new MockMetaStoreFailureClient();
    }

    @Override
    protected IMetaStoreClient reconnect(IMetaStoreClient client) {
      return new MockMetaStoreSuccessClient();
    }

    @Override
    protected void close(IMetaStoreClient client) {

    }

    @Override
    protected boolean isConnectionException(Exception e) {
      if (super.isConnectionException(e) || (e != null && e instanceof MetaException &&
              e.getMessage().contains("Got exception: org.apache.thrift.transport.TTransportException"))) {
        return true;
      }
      return false;
    }
  }

  private static class MockMetaStoreFailureClient extends MockMetaStoreClientBase {

    MockMetaStoreFailureClient(){}

    private void getException() throws MetaException {
      Exception e = new TTransportException();
      String exInfo = "Got exception: " + e.getClass().getName() + " "
              + e.getMessage();
      throw new MetaException(exInfo);
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException {
      getException();
      return null;
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes) throws TException {
      getException();
      return null;
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern) throws MetaException {
      getException();
      return null;
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws MetaException {
      getException();
      return null;
    }

    @Override
    public List<String> getDatabases(String databasePattern) throws MetaException{
      getException();
      return null;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException {
      getException();
      return null;
    }

    @Override
    public boolean tableExists(String databaseName, String tableName) throws MetaException,
            TException, UnknownDBException {
      throw new TTransportException();
    }
  }

  private static class MockMetaStoreSuccessClient extends MockMetaStoreClientBase {

    MockMetaStoreSuccessClient() {}

    @Override
    public List<String> getAllTables(String dbName) throws MetaException {
      return null;
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes) throws MetaException {
      return null;
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern) throws MetaException {
      return null;
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws MetaException {
      return null;
    }

    @Override
    public List<String> getDatabases(String databasePattern) throws MetaException{
      return null;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException {
      return null;
    }
  }

  private static class MockMetaStoreClientBase implements IMetaStoreClient {

    @Override
    public boolean isCompatibleWith(HiveConf conf) {
      return false;
    }

    @Override
    public void setHiveAddedJars(String addedJars) {

    }

    @Override
    public boolean isLocalMetaStore() {
      return false;
    }

    @Override
    public void reconnect() throws MetaException {

    }

    @Override
    public void close() {

    }

    @Override
    public void setMetaConf(String key, String value) throws MetaException, TException {

    }

    @Override
    public String getMetaConf(String key) throws MetaException, TException {
      return null;
    }

    @Override
    public List<String> getDatabases(String databasePattern) throws MetaException, TException {
      return null;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
      return null;
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern) throws MetaException, TException, UnknownDBException {
      return null;
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern, TableType tableType) throws MetaException, TException, UnknownDBException {
      return null;
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes) throws MetaException, TException, UnknownDBException {
      return null;
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException, TException, UnknownDBException {
      return null;
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws MetaException, TException, InvalidOperationException, UnknownDBException {
      return null;
    }

    @Override
    public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab) throws MetaException, TException, NoSuchObjectException {

    }

    @Override
    public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws MetaException, TException, NoSuchObjectException {

    }

    @Override
    public void dropTable(String tableName, boolean deleteData) throws MetaException, UnknownTableException, TException, NoSuchObjectException {

    }

    @Override
    public void dropTable(String dbname, String tableName) throws MetaException, TException, NoSuchObjectException {

    }

    @Override
    public boolean tableExists(String databaseName, String tableName) throws MetaException, TException, UnknownDBException {
      return false;
    }

    @Override
    public boolean tableExists(String tableName) throws MetaException, TException, UnknownDBException {
      return false;
    }

    @Override
    public Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException {
      return null;
    }

    @Override
    public Database getDatabase(String databaseName) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException, TException, NoSuchObjectException {
      return null;
    }

    @Override
    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames) throws MetaException, InvalidOperationException, UnknownDBException, TException {
      return null;
    }

    @Override
    public Partition appendPartition(String tableName, String dbName, List<String> partVals) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
      return null;
    }

    @Override
    public Partition appendPartition(String tableName, String dbName, String name) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
      return null;
    }

    @Override
    public Partition add_partition(Partition partition) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
      return null;
    }

    @Override
    public int add_partitions(List<Partition> partitions) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
      return 0;
    }

    @Override
    public int add_partitions_pspec(PartitionSpecProxy partitionSpec) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
      return 0;
    }

    @Override
    public List<Partition> add_partitions(List<Partition> partitions, boolean ifNotExists, boolean needResults) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
      return null;
    }

    @Override
    public Partition getPartition(String dbName, String tblName, List<String> partVals) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceDb, String sourceTable, String destdb, String destTableName) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
      return null;
    }

    @Override
    public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceDb, String sourceTable, String destdb, String destTableName) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
      return null;
    }

    @Override
    public Partition getPartition(String dbName, String tblName, String name) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
      return null;
    }

    @Override
    public Partition getPartitionWithAuthInfo(String dbName, String tableName, List<String> pvals, String userName, List<String> groupNames) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
      return null;
    }

    @Override
    public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
      return null;
    }

    @Override
    public List<Partition> listPartitions(String db_name, String tbl_name, List<String> part_vals, short max_parts) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts) throws MetaException, TException {
      return null;
    }

    @Override
    public List<String> listPartitionNames(String db_name, String tbl_name, List<String> part_vals, short max_parts) throws MetaException, TException, NoSuchObjectException {
      return null;
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request) throws MetaException, TException, NoSuchObjectException {
      return null;
    }

    @Override
    public int getNumPartitionsByFilter(String dbName, String tableName, String filter) throws MetaException, NoSuchObjectException, TException {
      return 0;
    }

    @Override
    public List<Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter, short max_parts) throws MetaException, NoSuchObjectException, TException {
      return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name, String filter, int max_parts) throws MetaException, NoSuchObjectException, TException {
      return null;
    }

    @Override
    public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr, String default_partition_name, short max_parts, List<Partition> result) throws TException {
      return false;
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, short s, String userName, List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
      return null;
    }

    @Override
    public List<Partition> getPartitionsByNames(String db_name, String tbl_name, List<String> part_names) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, List<String> partialPvals, short s, String userName, List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
      return null;
    }

    @Override
    public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {

    }

    @Override
    public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
      return false;
    }

    @Override
    public void validatePartitionNameCharacters(List<String> partVals) throws TException, MetaException {

    }

    @Override
    public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void alter_table(String defaultDatabaseName, String tblName, Table table) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_table(String defaultDatabaseName, String tblName, Table table, boolean cascade) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_table_with_environmentContext(String defaultDatabaseName, String tblName, Table table, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void createDatabase(Database db) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String name) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alterDatabase(String name, Database db) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
      return false;
    }

    @Override
    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, PartitionDropOptions options) throws TException {
      return false;
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists, boolean needResults) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions options) throws TException {
      return null;
    }

    @Override
    public boolean dropPartition(String db_name, String tbl_name, String name, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
      return false;
    }

    @Override
    public void alter_partition(String dbName, String tblName, Partition newPart) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partition(String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partitions(String dbName, String tblName, List<Partition> newParts) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partitions(String dbName, String tblName, List<Partition> newParts, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void renamePartition(String dbname, String name, List<String> part_vals, Partition newPart) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public List<FieldSchema> getFields(String db, String tableName) throws MetaException, TException, UnknownTableException, UnknownDBException {
      return null;
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName) throws MetaException, TException, UnknownTableException, UnknownDBException {
      return null;
    }

    @Override
    public String getConfigValue(String name, String defaultValue) throws TException, ConfigValSecurityException {
      return null;
    }

    @Override
    public List<String> partitionNameToVals(String name) throws MetaException, TException {
      return null;
    }

    @Override
    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
      return null;
    }

    @Override
    public void createIndex(Index index, Table indexTable) throws InvalidObjectException, MetaException, NoSuchObjectException, TException, AlreadyExistsException {

    }

    @Override
    public void alter_index(String dbName, String tblName, String indexName, Index index) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public Index getIndex(String dbName, String tblName, String indexName) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
      return null;
    }

    @Override
    public List<Index> listIndexes(String db_name, String tbl_name, short max) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public List<String> listIndexNames(String db_name, String tbl_name, short max) throws MetaException, TException {
      return null;
    }

    @Override
    public boolean dropIndex(String db_name, String tbl_name, String name, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
      return false;
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics statsObj) throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
      return false;
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj) throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
      return false;
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName, List<String> partNames, List<String> colNames) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
      return false;
    }

    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
      return false;
    }

    @Override
    public boolean create_role(Role role) throws MetaException, TException {
      return false;
    }

    @Override
    public boolean drop_role(String role_name) throws MetaException, TException {
      return false;
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
      return null;
    }

    @Override
    public boolean grant_role(String role_name, String user_name, PrincipalType principalType, String grantor, PrincipalType grantorType, boolean grantOption) throws MetaException, TException {
      return false;
    }

    @Override
    public boolean revoke_role(String role_name, String user_name, PrincipalType principalType, boolean grantOption) throws MetaException, TException {
      return false;
    }

    @Override
    public List<Role> list_roles(String principalName, PrincipalType principalType) throws MetaException, TException {
      return null;
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name, List<String> group_names) throws MetaException, TException {
      return null;
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(String principal_name, PrincipalType principal_type, HiveObjectRef hiveObject) throws MetaException, TException {
      return null;
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
      return false;
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws MetaException, TException {
      return false;
    }

    @Override
    public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws MetaException, TException {
      return null;
    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
      return 0;
    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {

    }

    @Override
    public String getTokenStrForm() throws IOException {
      return null;
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
      return false;
    }

    @Override
    public boolean removeToken(String tokenIdentifier) throws TException {
      return false;
    }

    @Override
    public String getToken(String tokenIdentifier) throws TException {
      return null;
    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
      return null;
    }

    @Override
    public int addMasterKey(String key) throws MetaException, TException {
      return 0;
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public boolean removeMasterKey(Integer keySeq) throws TException {
      return false;
    }

    @Override
    public String[] getMasterKeys() throws TException {
      return new String[0];
    }

    @Override
    public void createFunction(Function func) throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterFunction(String dbName, String funcName, Function newFunction) throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void dropFunction(String dbName, String funcName) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {

    }

    @Override
    public Function getFunction(String dbName, String funcName) throws MetaException, TException {
      return null;
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
      return null;
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
      return null;
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
      return null;
    }

    @Override
    public ValidTxnList getValidTxns(long currentTxn) throws TException {
      return null;
    }

    @Override
    public long openTxn(String user) throws TException {
      return 0;
    }

    @Override
    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
      return null;
    }

    @Override
    public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {

    }

    @Override
    public void commitTxn(long txnid) throws NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public void abortTxns(List<Long> txnids) throws TException {

    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
      return null;
    }

    @Override
    public LockResponse lock(LockRequest request) throws NoSuchTxnException, TxnAbortedException, TException {
      return null;
    }

    @Override
    public LockResponse checkLock(long lockid) throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
      return null;
    }

    @Override
    public void unlock(long lockid) throws NoSuchLockException, TxnOpenException, TException {

    }

    @Override
    public ShowLocksResponse showLocks() throws TException {
      return null;
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
      return null;
    }

    @Override
    public void heartbeat(long txnid, long lockid) throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
      return null;
    }

    @Override
    public void compact(String dbname, String tableName, String partitionName, CompactionType type) throws TException {

    }

    @Override
    public void compact(String dbname, String tableName, String partitionName, CompactionType type, Map<String, String> tblproperties) throws TException {

    }

    @Override
    public CompactionResponse compact2(String dbname, String tableName, String partitionName, CompactionType type, Map<String, String> tblproperties) throws TException {
      return null;
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
      return null;
    }

    @Override
    public void addDynamicPartitions(long txnId, String dbName, String tableName, List<String> partNames) throws TException {

    }

    @Override
    public void addDynamicPartitions(long txnId, String dbName, String tableName, List<String> partNames, DataOperationType operationType) throws TException {

    }

    @Override
    public void insertTable(Table table, boolean overwrite) throws MetaException {

    }

    @Override
    public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, NotificationFilter filter) throws TException {
      return null;
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
      return null;
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest request) throws TException {
      return null;
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincRoleReq) throws MetaException, TException {
      return null;
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
      return null;
    }

    @Override
    public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName) throws NoSuchObjectException, MetaException, TException {
      return null;
    }

    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request) throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
      return false;
    }

    @Override
    public void flushCache() {

    }

    @Override
    public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
      return null;
    }

    @Override
    public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(List<Long> fileIds, ByteBuffer sarg, boolean doGetFooters) throws TException {
      return null;
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws TException {

    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {

    }

    @Override
    public boolean isSameConfObj(HiveConf c) {
      return false;
    }

    @Override
    public boolean cacheFileMetadata(String dbName, String tableName, String partName, boolean allParts) throws TException {
      return false;
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request) throws MetaException, NoSuchObjectException, TException {
      return null;
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request) throws MetaException, NoSuchObjectException, TException {
      return null;
    }

    @Override
    public void createTableWithConstraints(Table tTbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void dropConstraint(String dbName, String tableName, String constraintName) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws MetaException, NoSuchObjectException, TException {

    }
  }

}
