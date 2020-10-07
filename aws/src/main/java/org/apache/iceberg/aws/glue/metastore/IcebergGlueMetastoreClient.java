/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aws.glue.metastore;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.iceberg.aws.glue.converters.CatalogToHiveConverter;
import org.apache.iceberg.aws.glue.converters.ConverterUtils;
import org.apache.iceberg.aws.glue.converters.GlueInputConverter;
import org.apache.iceberg.aws.glue.converters.HiveToCatalogConverter;
import org.apache.iceberg.aws.glue.lock.DynamoLockManager;
import org.apache.iceberg.aws.glue.lock.LockManager;
import org.apache.iceberg.aws.glue.shims.AwsGlueHiveShims;
import org.apache.iceberg.aws.glue.shims.ShimsLoader;
import org.apache.iceberg.aws.glue.util.BatchDeletePartitionsHelper;
import org.apache.iceberg.aws.glue.util.ExpressionHelper;
import org.apache.iceberg.aws.glue.util.LoggingHelper;
import org.apache.iceberg.aws.glue.util.MetastoreClientUtils;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"Slf4jConstantLogMessage", "UnusedMethod"})
public class IcebergGlueMetastoreClient implements IMetaStoreClient {

  // TODO "hook" into Hive logging (hive or hive.metastore)
  private static final Logger LOG = LoggerFactory.getLogger(IcebergGlueMetastoreClient.class);

  private final HiveConf conf;
  private final AWSGlue glueClient;
  private final Warehouse wh;
  private final GlueMetastoreClientDelegate glueMetastoreClientDelegate;
  private final String catalogId;

  private static final int BATCH_DELETE_PARTITIONS_PAGE_SIZE = 25;
  private static final int BATCH_DELETE_PARTITIONS_THREADS_COUNT = 5;
  static final String BATCH_DELETE_PARTITIONS_THREAD_POOL_NAME_FORMAT = "batch-delete-partitions-%d";
  private static final ExecutorService BATCH_DELETE_PARTITIONS_THREAD_POOL = Executors.newFixedThreadPool(
      BATCH_DELETE_PARTITIONS_THREADS_COUNT,
      new ThreadFactoryBuilder()
          .setNameFormat(BATCH_DELETE_PARTITIONS_THREAD_POOL_NAME_FORMAT)
          .setDaemon(true).build()
  );

  private Map<String, String> currentMetaVars;
  private final AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();

  public IcebergGlueMetastoreClient(HiveConf conf) throws MetaException {
    this.conf = conf;
    glueClient = new AWSGlueClientFactory(this.conf).newClient();

    // TODO preserve existing functionality for HiveMetaHook
    wh = new Warehouse(this.conf);

    AWSGlueMetastore glueMetastore = new AWSGlueMetastoreFactory().newMetastore(conf);
    LockManager lockManager = new DynamoLockManager(conf);
    glueMetastoreClientDelegate = new GlueMetastoreClientDelegate(this.conf, glueMetastore, wh, lockManager);

    snapshotActiveConf();
    catalogId = MetastoreClientUtils.getCatalogId(conf);
    if (!doesDefaultDBExist()) {
      createDefaultDatabase();
    }
  }

  /**
   * Currently used for unit tests
   */
  public static class Builder {

    private HiveConf conf;
    private Warehouse wh;
    private GlueClientFactory clientFactory;
    private AWSGlueMetastoreFactory metastoreFactory;
    private boolean createDefaults = true;
    private String catalogId;

    public Builder withHiveConf(HiveConf confInput) {
      this.conf = confInput;
      return this;
    }

    public Builder withClientFactory(GlueClientFactory clientFactoryInput) {
      this.clientFactory = clientFactoryInput;
      return this;
    }

    public Builder withMetastoreFactory(AWSGlueMetastoreFactory metastoreFactoryInput) {
      this.metastoreFactory = metastoreFactoryInput;
      return this;
    }

    public Builder withWarehouse(Warehouse whInput) {
      this.wh = whInput;
      return this;
    }

    public Builder withCatalogId(String catalogIdInput) {
      this.catalogId = catalogIdInput;
      return this;
    }

    public IcebergGlueMetastoreClient build() throws MetaException {
      return new IcebergGlueMetastoreClient(this);
    }

    public Builder createDefaults(boolean createDefaultDB) {
      this.createDefaults = createDefaultDB;
      return this;
    }
  }

  private IcebergGlueMetastoreClient(Builder builder) throws MetaException {
    conf = MoreObjects.firstNonNull(builder.conf, new HiveConf());

    if (builder.wh != null) {
      this.wh = builder.wh;
    } else {
      this.wh = new Warehouse(conf);
    }

    if (builder.catalogId != null) {
      this.catalogId = builder.catalogId;
    } else {
      this.catalogId = null;
    }

    GlueClientFactory clientFactory = MoreObjects.firstNonNull(builder.clientFactory, new AWSGlueClientFactory(conf));
    AWSGlueMetastoreFactory metastoreFactory = MoreObjects.firstNonNull(builder.metastoreFactory,
            new AWSGlueMetastoreFactory());

    glueClient = clientFactory.newClient();
    AWSGlueMetastore glueMetastore = metastoreFactory.newMetastore(conf);
    LockManager lockManager = new DynamoLockManager(conf);
    glueMetastoreClientDelegate = new GlueMetastoreClientDelegate(this.conf, glueMetastore, wh, lockManager);

    /**
     * It seems weird to create databases as part of client construction. This
     * part should probably be moved to the section in hive code right after the
     * metastore client is instantiated. For now, simply copying the
     * functionality in the thrift server
     */
    if (builder.createDefaults && !doesDefaultDBExist()) {
      createDefaultDatabase();
    }
  }

  private boolean doesDefaultDBExist() throws MetaException {
    try {
      GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest()
          .withName(MetaStoreUtils.DEFAULT_DATABASE_NAME)
          .withCatalogId(catalogId);
      glueClient.getDatabase(getDatabaseRequest);
    } catch (EntityNotFoundException e) {
      return false;
    } catch (AmazonServiceException e) {
      String msg = "Unable to verify existence of default database: ";
      LOG.error(msg, e);
      throw new MetaException(msg + e);
    }
    return true;
  }

  private void createDefaultDatabase() throws MetaException {
    Database defaultDB = new Database();
    defaultDB.setName(MetaStoreUtils.DEFAULT_DATABASE_NAME);
    defaultDB.setDescription(MetaStoreUtils.DEFAULT_DATABASE_COMMENT);
    defaultDB.setLocationUri(wh.getDefaultDatabasePath(MetaStoreUtils.DEFAULT_DATABASE_NAME).toString());

    org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet principalPrivilegeSet
          = new org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet();
    principalPrivilegeSet.setRolePrivileges(
        Maps.<String, List<org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo>>newHashMap());

    defaultDB.setPrivileges(principalPrivilegeSet);

    /**
     * TODO: Grant access to role PUBLIC after role support is added
     */
    try {
      createDatabase(defaultDB);
    } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
      LOG.warn("database - default already exists. Ignoring..", e);
    } catch (Exception e) {
      LOG.error("Unable to create default database", e);
    }
  }

  @Override
  public void createDatabase(Database database) throws InvalidObjectException,
        org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    glueMetastoreClientDelegate.createDatabase(database);
  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getDatabase(name);
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException, TException {
    return glueMetastoreClientDelegate.getDatabases(pattern);
  }

  @Override
  public List<String> getAllDatabases() throws MetaException, TException {
    return getDatabases(".*");
  }

  @Override
  public void alterDatabase(String databaseName, Database database) throws NoSuchObjectException, MetaException,
        TException {
    glueMetastoreClientDelegate.alterDatabase(databaseName, database);
  }

  @Override
  public void dropDatabase(String name) throws NoSuchObjectException, InvalidOperationException, MetaException,
        TException {
    dropDatabase(name, true, false, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb) throws NoSuchObjectException,
        InvalidOperationException, MetaException, TException {
    dropDatabase(name, deleteData, ignoreUnknownDb, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.dropDatabase(name, deleteData, ignoreUnknownDb, cascade);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition add_partition(
      org.apache.hadoop.hive.metastore.api.Partition partition)
      throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
      TException {
    glueMetastoreClientDelegate.addPartitions(Lists.newArrayList(partition), false, true);
    return partition;
  }

  @Override
  public int add_partitions(List<org.apache.hadoop.hive.metastore.api.Partition> partitions)
        throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
        TException {
    return glueMetastoreClientDelegate.addPartitions(partitions, false, true).size();
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> add_partitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
      boolean ifNotExists,
      boolean needResult
  ) throws TException {
    return glueMetastoreClientDelegate.addPartitions(partitions, ifNotExists, needResult);
  }

  @Override
  public int add_partitions_pspec(
      PartitionSpecProxy pSpec
  ) throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      MetaException, TException {
    return glueMetastoreClientDelegate.addPartitionsSpecProxy(pSpec);
  }

  @Override
  public void alterFunction(
      String dbName, String functionName, org.apache.hadoop.hive.metastore.api.Function newFunction)
      throws InvalidObjectException, MetaException, TException {
    glueMetastoreClientDelegate.alterFunction(dbName, functionName, newFunction);
  }

  @Override
  public void alter_index(String dbName, String tblName, String indexName, Index index)
      throws InvalidOperationException, MetaException, TException {
    Table catalogIndexTableObject = HiveToCatalogConverter.convertIndexToTableObject(index);
    org.apache.hadoop.hive.metastore.api.Table originTable = getTable(dbName, tblName);
    String indexTableObjectName = GlueMetastoreClientDelegate.INDEX_PREFIX + indexName;
    if (!originTable.getParameters().containsKey(indexTableObjectName)) {
      throw new NoSuchObjectException("can not find index: " + indexName);
    }

    originTable.getParameters().put(indexTableObjectName, ConverterUtils.catalogTableToString(catalogIndexTableObject));
    alter_table(dbName, tblName, originTable);
  }

  @Override
  public void alter_partition(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Partition partition
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, Lists.newArrayList(partition));
  }

  @Override
  public void alter_partition(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Partition partition,
      EnvironmentContext environmentContext
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, Lists.newArrayList(partition));
  }

  @Override
  public void alter_partitions(
      String dbName,
      String tblName,
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, partitions);
  }

  @Override
  public void alter_partitions(
      String dbName,
      String tblName,
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
      EnvironmentContext environmentContext
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, partitions);
  }

  @Override
  public PartitionValuesResponse listPartitionValues(
      PartitionValuesRequest request) throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException("listPartitionValues is not supported");
  }

  @Override
  public void alter_table(
      String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Table table)
      throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterTable(dbName, tblName, table, null);
  }

  @Override
  public void alter_table(
      String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Table table, boolean cascade)
      throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterTable(dbName, tblName, table, null);
  }

  @Override
  public void alter_table_with_environmentContext(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Table table,
      EnvironmentContext environmentContext
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterTable(dbName, tblName, table, environmentContext);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(
      String dbName, String tblName, List<String> values)
      throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      MetaException, TException {
    return glueMetastoreClientDelegate.appendPartition(dbName, tblName, values);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(
      String dbName, String tblName, String partitionName) throws InvalidObjectException,
      org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    List<String> partVals = partitionNameToVals(partitionName);
    return glueMetastoreClientDelegate.appendPartition(dbName, tblName, partVals);
  }

  @Override
  public boolean create_role(org.apache.hadoop.hive.metastore.api.Role role) throws MetaException, TException {
    return glueMetastoreClientDelegate.createRole(role);
  }

  @Override
  public boolean drop_role(String roleName) throws MetaException, TException {
    return glueMetastoreClientDelegate.dropRole(roleName);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Role> list_roles(
      String principalName, org.apache.hadoop.hive.metastore.api.PrincipalType principalType
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.listRoles(principalName, principalType);
  }

  @Override
  public List<String> listRoleNames() throws MetaException, TException {
    return glueMetastoreClientDelegate.listRoleNames();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse get_principals_in_role(
      org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest request) throws MetaException, TException {
    return glueMetastoreClientDelegate.getPrincipalsInRole(request);
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest request) throws MetaException, TException {
    return glueMetastoreClientDelegate.getRoleGrantsForPrincipal(request);
  }

  @Override
  public boolean grant_role(
      String roleName,
      String userName,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
      String grantor, org.apache.hadoop.hive.metastore.api.PrincipalType grantorType,
      boolean grantOption
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.grantRole(
        roleName, userName, principalType, grantor, grantorType, grantOption);
  }

  @Override
  public boolean revoke_role(
      String roleName,
      String userName,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
      boolean grantOption
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.revokeRole(roleName, userName, principalType, grantOption);
  }

  @Override
  public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
    glueMetastoreClientDelegate.cancelDelegationToken(tokenStrForm);
  }

  @Override
  public String getTokenStrForm() throws IOException {
    return glueMetastoreClientDelegate.getTokenStrForm();
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
    return glueMetastoreClientDelegate.addToken(tokenIdentifier, delegationToken);
  }

  @Override
  public boolean removeToken(String tokenIdentifier) throws TException {
    return glueMetastoreClientDelegate.removeToken(tokenIdentifier);
  }

  @Override
  public String getToken(String tokenIdentifier) throws TException {
    return glueMetastoreClientDelegate.getToken(tokenIdentifier);
  }

  @Override
  public List<String> getAllTokenIdentifiers() throws TException {
    return glueMetastoreClientDelegate.getAllTokenIdentifiers();
  }

  @Override
  public int addMasterKey(String key) throws MetaException, TException {
    return glueMetastoreClientDelegate.addMasterKey(key);
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException, TException {
    glueMetastoreClientDelegate.updateMasterKey(seqNo, key);
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) throws TException {
    return glueMetastoreClientDelegate.removeMasterKey(keySeq);
  }

  @Override
  public String[] getMasterKeys() throws TException {
    return glueMetastoreClientDelegate.getMasterKeys();
  }

  @Override
  public LockResponse checkLock(long lockId)
      throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    return glueMetastoreClientDelegate.checkLock(lockId);
  }

  @Override
  public void close() {
    currentMetaVars = null;
  }

  @Override
  public void commitTxn(long txnId) throws NoSuchTxnException, TxnAbortedException, TException {
    glueMetastoreClientDelegate.commitTxn(txnId);
  }

  @Override
  public void abortTxns(List<Long> txnIds) throws TException {
    glueMetastoreClientDelegate.abortTxns(txnIds);
  }

  @Deprecated
  public void compact(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType
  ) throws TException {
    glueMetastoreClientDelegate.compact(dbName, tblName, partitionName, compactionType);
  }

  @Deprecated
  public void compact(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType,
      Map<String, String> tblProperties
  ) throws TException {
    glueMetastoreClientDelegate.compact(dbName, tblName, partitionName, compactionType, tblProperties);
  }

  @Override
  public CompactionResponse compact2(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType,
      Map<String, String> tblProperties
  ) throws TException {
    return glueMetastoreClientDelegate.compact2(dbName, tblName, partitionName, compactionType, tblProperties);
  }

  @Override
  public void createFunction(org.apache.hadoop.hive.metastore.api.Function function)
      throws InvalidObjectException, MetaException, TException {
    glueMetastoreClientDelegate.createFunction(function);
  }

  @Override
  public void createIndex(Index index, org.apache.hadoop.hive.metastore.api.Table indexTable)
      throws InvalidObjectException, MetaException, NoSuchObjectException,
      TException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException {
    boolean dirCreated = glueMetastoreClientDelegate.validateNewTableAndCreateDirectory(indexTable);
    boolean indexTableCreated = false;
    String dbName = index.getDbName();
    String indexTableName = index.getIndexTableName();
    String originTableName = index.getOrigTableName();
    Path indexTablePath = new Path(indexTable.getSd().getLocation());
    Table catalogIndexTableObject = HiveToCatalogConverter.convertIndexToTableObject(index);
    String indexTableObjectName = GlueMetastoreClientDelegate.INDEX_PREFIX + index.getIndexName();

    try {
      org.apache.hadoop.hive.metastore.api.Table originTable = getTable(dbName, originTableName);
      Map<String, String> parameters = originTable.getParameters();
      if (parameters.containsKey(indexTableObjectName)) {
        throw new org.apache.hadoop.hive.metastore.api.AlreadyExistsException(
            "Index: " + index.getIndexName() + " already exist");
      }
      createTable(indexTable);
      indexTableCreated = true;
      originTable.getParameters().put(indexTableObjectName,
          ConverterUtils.catalogTableToString(catalogIndexTableObject));
      alter_table(dbName, originTableName, originTable);
    } catch (Exception e) {
      if (dirCreated) {
        wh.deleteDir(indexTablePath, true);
      }
      if (indexTableCreated) {
        dropTable(dbName, indexTableName);
      }
      String msg = "Unable to create index: ";
      LOG.error(msg, e);
      if (e instanceof TException) {
        throw e;
      } else {
        throw new MetaException(msg + e);
      }
    }
  }

  @Override
  public void createTable(org.apache.hadoop.hive.metastore.api.Table tbl)
      throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, InvalidObjectException, MetaException,
        NoSuchObjectException, TException {
    glueMetastoreClientDelegate.createTable(tbl);
  }

  @Override
  public boolean deletePartitionColumnStatistics(
      String dbName, String tableName, String partName, String colName
  ) throws NoSuchObjectException, MetaException, InvalidObjectException,
      TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.deletePartitionColumnStatistics(dbName, tableName, partName, colName);
  }

  @Override
  public boolean deleteTableColumnStatistics(
      String dbName, String tableName, String colName
  ) throws NoSuchObjectException, MetaException, InvalidObjectException,
      TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.deleteTableColumnStatistics(dbName, tableName, colName);
  }

  @Override
  public void dropFunction(String dbName, String functionName) throws MetaException, NoSuchObjectException,
        InvalidObjectException, org.apache.hadoop.hive.metastore.api.InvalidInputException, TException {
    glueMetastoreClientDelegate.dropFunction(dbName, functionName);
  }

  @Override
  public boolean dropIndex(String dbName, String tblName, String name, boolean deleteData) throws NoSuchObjectException,
        MetaException, TException {
    Index indexToDrop = getIndex(dbName, tblName, name);
    String indexTableName = indexToDrop.getIndexTableName();

    // Drop the index metadata
    org.apache.hadoop.hive.metastore.api.Table originTable = getTable(dbName, tblName);
    Map<String, String> parameters = originTable.getParameters();
    String indexTableObjectName = GlueMetastoreClientDelegate.INDEX_PREFIX + name;
    if (!parameters.containsKey(indexTableObjectName)) {
      throw new NoSuchObjectException("can not find Index: " + name);
    }
    parameters.remove(indexTableObjectName);

    alter_table(dbName, tblName, originTable);

    // Now drop the data associated with the table used to hold the index data
    if (indexTableName != null && indexTableName.length() > 0) {
      dropTable(dbName, indexTableName, deleteData, true);
    }

    return true;
  }

  private void deleteParentRecursive(Path parent, int depth, boolean mustPurge) throws IOException, MetaException {
    if (depth > 0 && parent != null && wh.isWritable(parent) && wh.isEmpty(parent)) {
      wh.deleteDir(parent, true, mustPurge);
      deleteParentRecursive(parent.getParent(), depth - 1, mustPurge);
    }
  }

  // This logic is taken from HiveMetaStore#isMustPurge
  private boolean isMustPurge(org.apache.hadoop.hive.metastore.api.Table table, boolean ifPurge) {
    return ifPurge || "true".equalsIgnoreCase(table.getParameters().get("auto.purge"));
  }

  @Override
  public boolean dropPartition(String dbName, String tblName, List<String> values, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, false, deleteData, false);
  }

  @Override
  public boolean dropPartition(
      String dbName, String tblName, List<String> values, PartitionDropOptions options)
      throws TException {
    return glueMetastoreClientDelegate.dropPartition(
        dbName, tblName, values, options.ifExists, options.deleteData, options.purgeData);
  }

  @Override
  public boolean dropPartition(String dbName, String tblName, String partitionName, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    List<String> values = partitionNameToVals(partitionName);
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, false, deleteData, false);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(
      String dbName,
      String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs,
      boolean deleteData,
      boolean ifExists
  ) throws NoSuchObjectException, MetaException, TException {
    // use defaults from PartitionDropOptions for purgeData
    return dropPartitions_core(dbName, tblName, partExprs, deleteData, false);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(
      String dbName,
      String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs,
      boolean deleteData,
      boolean ifExists,
      boolean needResults
  ) throws NoSuchObjectException, MetaException, TException {
    return dropPartitions_core(dbName, tblName, partExprs, deleteData, false);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(
      String dbName,
      String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs,
      PartitionDropOptions options
  ) throws TException {
    return dropPartitions_core(dbName, tblName, partExprs, options.deleteData, options.purgeData);
  }

  private List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions_core(
      String databaseName,
      String tableName,
      List<ObjectPair<Integer, byte[]>> partExprs,
      boolean deleteData,
      boolean purgeData
  ) throws TException {
    throw new UnsupportedOperationException("dropPartitions_core is not supported");
  }

  /**
   * Delete all partitions in the list provided with BatchDeletePartitions request. It doesn't use transaction,
   * so the call may result in partial failure.
   * @param dbName dbName
   * @param tableName tableName
   * @param partitionsToDelete partitionsToDelete
   * @return the partitions successfully deleted
   * @throws TException TException
   */
  private List<org.apache.hadoop.hive.metastore.api.Partition> batchDeletePartitions(
        final String dbName, final String tableName, final List<Partition> partitionsToDelete,
        final boolean deleteData, final boolean purgeData) throws TException {

    List<org.apache.hadoop.hive.metastore.api.Partition> deleted = Lists.newArrayList();
    if (partitionsToDelete == null) {
      return deleted;
    }

    validateBatchDeletePartitionsArguments(dbName, tableName, partitionsToDelete);

    List<Future<BatchDeletePartitionsHelper>> batchDeletePartitionsFutures = Lists.newArrayList();

    int numOfPartitionsToDelete = partitionsToDelete.size();
    for (int i = 0; i < numOfPartitionsToDelete; i += BATCH_DELETE_PARTITIONS_PAGE_SIZE) {
      int end = Math.min(i + BATCH_DELETE_PARTITIONS_PAGE_SIZE, numOfPartitionsToDelete);
      final List<Partition> partitionsOnePage = partitionsToDelete.subList(i, end);

      batchDeletePartitionsFutures.add(BATCH_DELETE_PARTITIONS_THREAD_POOL.submit(
          new Callable<BatchDeletePartitionsHelper>() {
            @Override
            public BatchDeletePartitionsHelper call() throws Exception {
              return new BatchDeletePartitionsHelper(
                  glueClient, dbName, tableName, catalogId, partitionsOnePage).deletePartitions();
            }
          }));
    }

    TException tException = null;
    for (Future<BatchDeletePartitionsHelper> future : batchDeletePartitionsFutures) {
      try {
        BatchDeletePartitionsHelper batchDeletePartitionsHelper = future.get();
        for (Partition partition : batchDeletePartitionsHelper.getPartitionsDeleted()) {
          org.apache.hadoop.hive.metastore.api.Partition hivePartition =
                CatalogToHiveConverter.convertPartition(partition);
          try {
            performDropPartitionPostProcessing(dbName, tableName, hivePartition, deleteData, purgeData);
          } catch (TException e) {
            LOG.error("Drop partition directory failed.", e);
            tException = tException == null ? e : tException;
          }
          deleted.add(hivePartition);
        }
        tException = tException == null ? batchDeletePartitionsHelper.getFirstTException() : tException;
      } catch (Exception e) {
        LOG.error("Exception thrown by BatchDeletePartitions thread pool. ", e);
      }
    }

    if (tException != null) {
      throw tException;
    }
    return deleted;
  }

  private void validateBatchDeletePartitionsArguments(final String dbName, final String tableName,
                                                      final List<Partition> partitionsToDelete) {

    Preconditions.checkArgument(dbName != null, "Database name cannot be null");
    Preconditions.checkArgument(tableName != null, "Table name cannot be null");
    for (Partition partition : partitionsToDelete) {
      Preconditions.checkArgument(dbName.equals(partition.getDatabaseName()), "Database name cannot be null");
      Preconditions.checkArgument(tableName.equals(partition.getTableName()), "Table name cannot be null");
      Preconditions.checkArgument(partition.getValues() != null, "Partition values cannot be null");
    }
  }

  // Preserve the logic from Hive metastore
  private void performDropPartitionPostProcessing(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Partition partition,
      boolean deleteData,
      boolean ifPurge) throws MetaException, NoSuchObjectException, TException {
    if (deleteData && partition.getSd() != null && partition.getSd().getLocation() != null) {
      Path partPath = new Path(partition.getSd().getLocation());
      org.apache.hadoop.hive.metastore.api.Table table = getTable(dbName, tblName);
      if (MetastoreClientUtils.isExternalTable(table)) {
        // Don't delete external table data
        return;
      }
      boolean mustPurge = isMustPurge(table, ifPurge);
      wh.deleteDir(partPath, true, mustPurge);
      try {
        List<String> values = partition.getValues();
        deleteParentRecursive(partPath.getParent(), values.size() - 1, mustPurge);
      } catch (IOException e) {
        throw new MetaException(e.getMessage());
      }
    }
  }

  @Deprecated
  public void dropTable(String tableName, boolean deleteData) throws MetaException, UnknownTableException, TException,
        NoSuchObjectException {
    dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName, deleteData, false);
  }

  @Override
  public void dropTable(String dbname, String tableName) throws MetaException, TException, NoSuchObjectException {
    dropTable(dbname, tableName, true, true, false);
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab)
        throws MetaException, TException, NoSuchObjectException {
    dropTable(dbname, tableName, deleteData, ignoreUnknownTab, false);
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge)
        throws MetaException, TException, NoSuchObjectException {
    glueMetastoreClientDelegate.dropTable(dbname, tableName, deleteData, ignoreUnknownTab, ifPurge);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(
      Map<String, String> partitionSpecs,
      String srcDb,
      String srcTbl,
      String dstDb,
      String dstTbl
  ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return glueMetastoreClientDelegate.exchangePartition(partitionSpecs, srcDb, srcTbl, dstDb, dstTbl);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(
      Map<String, String> partitionSpecs,
      String sourceDb,
      String sourceTbl,
      String destDb,
      String destTbl
  ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return glueMetastoreClientDelegate.exchangePartitions(partitionSpecs, sourceDb, sourceTbl, destDb, destTbl);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName)
      throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getAggrColStatsFor(dbName, tblName, colNames, partName);
  }

  @Override
  public List<String> getAllTables(String dbname) throws MetaException, TException, UnknownDBException {
    return getTables(dbname, ".*");
  }

  @Override
  public String getConfigValue(String name, String defaultValue) throws TException, ConfigValSecurityException {
    if (!Pattern.matches("(hive|hdfs|mapred).*", name)) {
      throw new ConfigValSecurityException("For security reasons, the config key " + name + " cannot be accessed");
    }

    return conf.get(name, defaultValue);
  }

  @Override
  public String getDelegationToken(
      String owner, String renewerKerberosPrincipalName
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.getDelegationToken(owner, renewerKerberosPrincipalName);
  }

  @Override
  public List<FieldSchema> getFields(String db, String tableName) throws MetaException, TException,
        UnknownTableException, UnknownDBException {
    return glueMetastoreClientDelegate.getFields(db, tableName);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Function getFunction(
      String dbName, String functionName) throws MetaException, TException {
    return glueMetastoreClientDelegate.getFunction(dbName, functionName);
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
    return glueMetastoreClientDelegate.getFunctions(dbName, pattern);
  }

  @Override
  public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
    List<String> databaseNames = getDatabases(".*");
    List<org.apache.hadoop.hive.metastore.api.Function> result = new ArrayList<>();
    try {
      for (String databaseName : databaseNames) {
        GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest = new GetUserDefinedFunctionsRequest()
            .withDatabaseName(databaseName).withPattern(".*").withCatalogId(catalogId);

        List<UserDefinedFunction> catalogFunctions = glueClient.getUserDefinedFunctions(
            getUserDefinedFunctionsRequest)
            .getUserDefinedFunctions();
        for (UserDefinedFunction catalogFunction : catalogFunctions) {
          result.add(CatalogToHiveConverter.convertFunction(databaseName, catalogFunction));
        }
      }

      GetAllFunctionsResponse response = new GetAllFunctionsResponse();
      response.setFunctions(result);
      return response;
    } catch (AmazonServiceException e) {
      LOG.error("encountered AWS exception", e);
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get Functions: ";
      LOG.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  @Override
  public Index getIndex(String dbName, String tblName, String indexName) throws MetaException, UnknownTableException,
        NoSuchObjectException, TException {
    org.apache.hadoop.hive.metastore.api.Table originTable = getTable(dbName, tblName);
    Map<String, String> map = originTable.getParameters();
    String indexTableName = GlueMetastoreClientDelegate.INDEX_PREFIX + indexName;
    if (!map.containsKey(indexTableName)) {
      throw new NoSuchObjectException("can not find index: " + indexName);
    }
    Table indexTableObject = ConverterUtils.stringToCatalogTable(map.get(indexTableName));
    return CatalogToHiveConverter.convertTableObjectToIndex(indexTableObject);
  }

  @Override
  public String getMetaConf(String key) throws MetaException, TException {
    ConfVars metaConfVar = HiveConf.getMetaConf(key);
    if (metaConfVar == null) {
      throw new MetaException("Invalid configuration key " + key);
    }
    return conf.get(key, metaConfVar.getDefaultValue());
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName, List<String> values)
      throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartition(dbName, tblName, values);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartition(
      String dbName, String tblName, String partitionName)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.getPartition(dbName, tblName, partitionName);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName,
      String tableName,
      List<String> partitionNames,
      List<String> columnNames
  ) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartitionColumnStatistics(dbName, tableName, partitionNames, columnNames);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(
        String databaseName, String tableName, List<String> values,
        String userName, List<String> groupNames)
        throws MetaException, UnknownTableException, NoSuchObjectException, TException {

    // TODO move this into the service
    org.apache.hadoop.hive.metastore.api.Partition partition = getPartition(databaseName, tableName, values);
    org.apache.hadoop.hive.metastore.api.Table table = getTable(databaseName, tableName);
    if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
      String partName = Warehouse.makePartName(table.getPartitionKeys(), values);
      HiveObjectRef obj = new HiveObjectRef();
      obj.setObjectType(HiveObjectType.PARTITION);
      obj.setDbName(databaseName);
      obj.setObjectName(tableName);
      obj.setPartValues(values);
      org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet privilegeSet =
            this.get_privilege_set(obj, userName, groupNames);
      partition.setPrivileges(privilegeSet);
    }

    return partition;
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(
        String databaseName, String tableName, List<String> partitionNames)
        throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartitionsByNames(databaseName, tableName, partitionNames);
  }

  @Override
  public List<FieldSchema> getSchema(String db, String tableName)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    return glueMetastoreClientDelegate.getSchema(db, tableName);
  }

  @Deprecated
  public org.apache.hadoop.hive.metastore.api.Table getTable(String tableName)
      throws MetaException, TException, NoSuchObjectException {
    // this has been deprecated
    return getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName)
        throws MetaException, TException, NoSuchObjectException {
    return glueMetastoreClientDelegate.getTable(dbName, tableName);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames)
      throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getTableColumnStatistics(dbName, tableName, colNames);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Table> getTableObjectsByName(
      String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    List<org.apache.hadoop.hive.metastore.api.Table> hiveTables = Lists.newArrayList();
    for (String tableName : tableNames) {
      hiveTables.add(getTable(dbName, tableName));
    }

    return hiveTables;
  }

  @Override
  public List<String> getTables(
      String dbname, String tablePattern) throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTables(dbname, tablePattern);
  }

  @Override
  public List<String> getTables(String dbname, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTables(dbname, tablePattern, tableType);
  }

  @Override
  public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTableMeta(dbPatterns, tablePatterns, tableTypes);
  }

  @Override
  public ValidTxnList getValidTxns() throws TException {
    return glueMetastoreClientDelegate.getValidTxns();
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    return glueMetastoreClientDelegate.getValidTxns(currentTxn);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet get_privilege_set(
      HiveObjectRef obj,
      String user, List<String> groups
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.getPrivilegeSet(obj, user, groups);
  }

  @Override
  public boolean grant_privileges(org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges)
      throws MetaException, TException {
    return glueMetastoreClientDelegate.grantPrivileges(privileges);
  }

  @Override
  public boolean revoke_privileges(
      org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges,
      boolean grantOption
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.revokePrivileges(privileges, grantOption);
  }

  @Override
  public void heartbeat(long txnId, long lockId)
      throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    glueMetastoreClientDelegate.heartbeat(txnId, lockId);
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
    return glueMetastoreClientDelegate.heartbeatTxnRange(min, max);
  }

  @Override
  public boolean isCompatibleWith(HiveConf hiveConf) {
    if (currentMetaVars == null) {
      return false; // recreate
    }
    boolean compatible = true;
    for (ConfVars oneVar : HiveConf.metaVars) {
      // Since metaVars are all of different types, use string for comparison
      String oldVar = currentMetaVars.get(oneVar.varname);
      String newVar = hiveConf.get(oneVar.varname, "");
      if (oldVar == null ||
            (oneVar.isCaseSensitive() ? !oldVar.equals(newVar) : !oldVar.equalsIgnoreCase(newVar))) {
        LOG.info("Mestastore configuration " + oneVar.varname +
              " changed from " + oldVar + " to " + newVar);
        compatible = false;
      }
    }
    return compatible;
  }

  @Override
  public void setHiveAddedJars(String addedJars) {
    // taken from HiveMetaStoreClient
    HiveConf.setVar(conf, ConfVars.HIVEADDEDJARS, addedJars);
  }

  @Override
  public boolean isLocalMetaStore() {
    return false;
  }

  private void snapshotActiveConf() {
    currentMetaVars = new HashMap<String, String>(HiveConf.metaVars.length);
    for (ConfVars oneVar : HiveConf.metaVars) {
      currentMetaVars.put(oneVar.varname, conf.get(oneVar.varname, ""));
    }
  }

  @Override
  public boolean isPartitionMarkedForEvent(
      String dbName, String tblName, Map<String, String> partKVs, PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
      UnknownPartitionException, InvalidPartitionException {
    return glueMetastoreClientDelegate.isPartitionMarkedForEvent(dbName, tblName, partKVs, eventType);
  }

  @Override
  public List<String> listIndexNames(String db_name, String tbl_name, short max) throws MetaException, TException {
    // In current hive implementation, it ignores fields "max"
    // https://github.com/apache/hive/blob/rel/release-2.3.0/metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L3902-L3932
    List<Index> indexes = listIndexes(db_name, tbl_name, max);
    List<String> indexNames = Lists.newArrayList();
    for (Index index : indexes) {
      indexNames.add(index.getIndexName());
    }

    return indexNames;
  }

  @Override
  public List<Index> listIndexes(
      String db_name, String tbl_name, short max) throws NoSuchObjectException, MetaException, TException {
    // In current hive implementation, it ignores fields "max"
    // https://github.com/apache/hive/blob/rel/release-2.3.0/
    // metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L3867-L3899
    return glueMetastoreClientDelegate.listIndexes(db_name, tbl_name);
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName, short max)
        throws MetaException, TException {
    try {
      return listPartitionNames(dbName, tblName, null, max);
    } catch (NoSuchObjectException e) {
      // For compatibility with Hive 1.0.0
      return Collections.emptyList();
    }
  }

  @Override
  public List<String> listPartitionNames(String databaseName, String tableName,
                                         List<String> values, short max)
        throws MetaException, TException, NoSuchObjectException {
    return glueMetastoreClientDelegate.listPartitionNames(databaseName, tableName, values, max);
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tableName, String filter)
      throws MetaException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.getNumPartitionsByFilter(dbName, tableName, filter);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String dbName, String tblName, int max) throws TException {
    return glueMetastoreClientDelegate.listPartitionSpecs(dbName, tblName, max);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String dbName, String tblName, String filter, int max)
      throws MetaException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.listPartitionSpecsByFilter(dbName, tblName, filter, max);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String dbName, String tblName, short max)
      throws NoSuchObjectException, MetaException, TException {
    return listPartitions(dbName, tblName, null, max);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(
      String databaseName,
      String tableName,
      List<String> values,
      short max
  ) throws NoSuchObjectException, MetaException, TException {
    String expression = null;
    if (values != null) {
      org.apache.hadoop.hive.metastore.api.Table table = getTable(databaseName, tableName);
      expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, values);
    }
    return glueMetastoreClientDelegate.getPartitions(databaseName, tableName, expression, (long) max);
  }

  @Override
  public boolean listPartitionsByExpr(
      String databaseName,
      String tableName,
      byte[] expr,
      String defaultPartitionName,
      short max,
      List<org.apache.hadoop.hive.metastore.api.Partition> result
  ) throws TException {
    throw new UnsupportedOperationException("listPartitionsByExpr is not supported");
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(
      String databaseName,
      String tableName,
      String filterInput,
      short max
  ) throws MetaException, NoSuchObjectException, TException {
    String filter = filterInput;
    // we need to replace double quotes with single quotes in the filter expression
    // since server side does not accept double quote expressions.
    if (StringUtils.isNotBlank(filter)) {
      filter = ExpressionHelper.replaceDoubleQuoteWithSingleQuotes(filter);
    }
    return glueMetastoreClientDelegate.getPartitions(databaseName, tableName, filter, (long) max);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(
      String database, String table, short maxParts,
      String user, List<String> groups)
      throws MetaException, TException, NoSuchObjectException {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = listPartitions(database, table, maxParts);

    for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
      HiveObjectRef obj = new HiveObjectRef();
      obj.setObjectType(HiveObjectType.PARTITION);
      obj.setDbName(database);
      obj.setObjectName(table);
      obj.setPartValues(p.getValues());
      org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet set = this.get_privilege_set(obj, user, groups);
      p.setPrivileges(set);
    }

    return partitions;
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(
      String database, String table,
      List<String> partVals, short maxParts,
      String user, List<String> groups) throws MetaException, TException, NoSuchObjectException {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions =
        listPartitions(database, table, partVals, maxParts);

    for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
      HiveObjectRef obj = new HiveObjectRef();
      obj.setObjectType(HiveObjectType.PARTITION);
      obj.setDbName(database);
      obj.setObjectName(table);
      obj.setPartValues(p.getValues());
      org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet set;
      try {
        set = get_privilege_set(obj, user, groups);
      } catch (MetaException e) {
        LOG.info("No privileges found for user: {}, groups: [{}]",
            user, LoggingHelper.concatCollectionToStringForLogging(groups, ","), e);
        set = new org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet();
      }
      p.setPrivileges(set);
    }

    return partitions;
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws MetaException,
        TException, InvalidOperationException, UnknownDBException {
    return glueMetastoreClientDelegate.listTableNamesByFilter(dbName, filter, maxTables);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(
      String principal,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
      HiveObjectRef objectRef
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.listPrivileges(principal, principalType, objectRef);
  }

  @Override
  public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
    return glueMetastoreClientDelegate.lock(lockRequest);
  }

  @Override
  public void markPartitionForEvent(
      String dbName,
      String tblName,
      Map<String, String> partKVs,
      PartitionEventType eventType
  ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
      UnknownPartitionException, InvalidPartitionException {
    glueMetastoreClientDelegate.markPartitionForEvent(dbName, tblName, partKVs, eventType);
  }

  @Override
  public long openTxn(String user) throws TException {
    return glueMetastoreClientDelegate.openTxn(user);
  }

  @Override
  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    return glueMetastoreClientDelegate.openTxns(user, numTxns);
  }

  @Override
  public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
    // Lifted from HiveMetaStore
    if (name.length() == 0) {
      return new HashMap<String, String>();
    }
    return Warehouse.makeSpecFromName(name);
  }

  @Override
  public List<String> partitionNameToVals(String name) throws MetaException, TException {
    return glueMetastoreClientDelegate.partitionNameToVals(name);
  }

  @Override
  public void reconnect() throws MetaException {
    // TODO reset active Hive confs for metastore glueClient
    LOG.debug("reconnect() was called.");
  }

  @Override
  public void renamePartition(String dbName, String tblName, List<String> partitionValues,
                              org.apache.hadoop.hive.metastore.api.Partition newPartition)
        throws InvalidOperationException, MetaException, TException {

    // Set DDL time to now if not specified
    setDDLTime(newPartition);
    org.apache.hadoop.hive.metastore.api.Table tbl;
    org.apache.hadoop.hive.metastore.api.Partition oldPart;

    try {
      tbl = getTable(dbName, tblName);
      oldPart = getPartition(dbName, tblName, partitionValues);
    } catch (NoSuchObjectException e) {
      throw new InvalidOperationException(e.getMessage());
    }

    if (newPartition.getSd() == null || oldPart.getSd() == null) {
      throw new InvalidOperationException("Storage descriptor cannot be null");
    }

    // if an external partition is renamed, the location should not change
    if (!Strings.isNullOrEmpty(tbl.getTableType()) && tbl.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) {
      newPartition.getSd().setLocation(oldPart.getSd().getLocation());
      renamePartitionInCatalog(dbName, tblName, partitionValues, newPartition);
    } else {

      Path destPath = getDestinationPathForRename(dbName, tbl, newPartition);
      Path srcPath = new Path(oldPart.getSd().getLocation());
      FileSystem srcFs = wh.getFs(srcPath);
      FileSystem destFs = wh.getFs(destPath);

      verifyDestinationLocation(srcFs, destFs, srcPath, destPath, tbl, newPartition);
      newPartition.getSd().setLocation(destPath.toString());

      renamePartitionInCatalog(dbName, tblName, partitionValues, newPartition);
      boolean success = true;
      try {
        if (srcFs.exists(srcPath)) {
          // if destPath's parent path doesn't exist, we should mkdir it
          Path destParentPath = destPath.getParent();
          if (!wh.mkdirs(destParentPath, true)) {
            throw new IOException("Unable to create path " + destParentPath);
          }
          wh.renameDir(srcPath, destPath, true);
        }
      } catch (IOException e) {
        success = false;
        throw new InvalidOperationException("Unable to access old location " +
            srcPath + " for partition " + tbl.getDbName() + "." +
            tbl.getTableName() + " " + partitionValues);
      } finally {
        if (!success) {
          // revert metastore operation
          renamePartitionInCatalog(dbName, tblName, newPartition.getValues(), oldPart);
        }
      }
    }
  }

  private void verifyDestinationLocation(
      FileSystem srcFs,
      FileSystem destFs,
      Path srcPath,
      Path destPath,
      org.apache.hadoop.hive.metastore.api.Table tbl,
      org.apache.hadoop.hive.metastore.api.Partition newPartition)
      throws InvalidOperationException {
    String oldPartLoc = srcPath.toString();
    String newPartLoc = destPath.toString();

    // check that src and dest are on the same file system
    if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
      throw new InvalidOperationException("table new location " + destPath +
          " is on a different file system than the old location " +
          srcPath + ". This operation is not supported");
    }
    try {
      srcFs.exists(srcPath); // check that src exists and also checks
      if (newPartLoc.compareTo(oldPartLoc) != 0 && destFs.exists(destPath)) {
        throw new InvalidOperationException("New location for this partition " +
            tbl.getDbName() + "." + tbl.getTableName() + "." + newPartition.getValues() +
            " already exists : " + destPath);
      }
    } catch (IOException e) {
      throw new InvalidOperationException("Unable to access new location " +
          destPath + " for partition " + tbl.getDbName() + "." +
          tbl.getTableName() + " " + newPartition.getValues());
    }
  }

  private Path getDestinationPathForRename(
      String dbName,
      org.apache.hadoop.hive.metastore.api.Table tbl,
      org.apache.hadoop.hive.metastore.api.Partition newPartition)
      throws InvalidOperationException, MetaException, TException {
    try {
      Path destPath = new Path(hiveShims.getDefaultTablePath(getDatabase(dbName), tbl.getTableName(), wh),
            Warehouse.makePartName(tbl.getPartitionKeys(), newPartition.getValues()));
      return constructRenamedPath(destPath, new Path(newPartition.getSd().getLocation()));
    } catch (NoSuchObjectException e) {
      throw new InvalidOperationException(
            "Unable to change partition or table. Database " + dbName + " does not exist" +
                " Check metastore logs for detailed stack." + e.getMessage());
    }
  }

  private void setDDLTime(org.apache.hadoop.hive.metastore.api.Partition partition) {
    if (partition.getParameters() == null ||
          partition.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
          Integer.parseInt(partition.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
      partition.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
            .currentTimeMillis() / 1000));
    }
  }

  private void renamePartitionInCatalog(
      String databaseName, String tableName,
      List<String> partitionValues, org.apache.hadoop.hive.metastore.api.Partition newPartition)
      throws InvalidOperationException, MetaException, TException {
    try {
      glueClient.updatePartition(
          new UpdatePartitionRequest()
          .withDatabaseName(databaseName)
          .withTableName(tableName)
          .withPartitionValueList(partitionValues)
          .withPartitionInput(GlueInputConverter.convertToPartitionInput(newPartition)));
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    }
  }

  @Override
  public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
    return glueMetastoreClientDelegate.renewDelegationToken(tokenStrForm);
  }

  @Override
  public void rollbackTxn(long txnId) throws NoSuchTxnException, TException {
    glueMetastoreClientDelegate.rollbackTxn(txnId);
  }

  @Override
  public void setMetaConf(String key, String value) throws MetaException, TException {
    ConfVars confVar = HiveConf.getMetaConf(key);
    if (confVar == null) {
      throw new MetaException("Invalid configuration key " + key);
    }
    String validate = confVar.validate(value);
    if (validate != null) {
      throw new MetaException("Invalid configuration value " + value + " for key " + key + " by " + validate);
    }
    conf.set(key, value);
  }

  @Override
  public boolean setPartitionColumnStatistics(org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest request)
      throws NoSuchObjectException, InvalidObjectException,
      MetaException, TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.setPartitionColumnStatistics(request);
  }

  @Override
  public void flushCache() {
    // no op
  }

  @Override
  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
    return glueMetastoreClientDelegate.getFileMetadata(fileIds);
  }

  @Override
  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
      List<Long> fileIds,
      ByteBuffer sarg,
      boolean doGetFooters
  ) throws TException {
    return glueMetastoreClientDelegate.getFileMetadataBySarg(fileIds, sarg, doGetFooters);
  }

  @Override
  public void clearFileMetadata(List<Long> fileIds) throws TException {
    glueMetastoreClientDelegate.clearFileMetadata(fileIds);
  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
    glueMetastoreClientDelegate.putFileMetadata(fileIds, metadata);
  }

  @Override
  public boolean isSameConfObj(HiveConf hiveConf) {
    // taken from HiveMetaStoreClient
    return this.conf == hiveConf;
  }

  @Override
  public boolean cacheFileMetadata(String dbName, String tblName, String partName, boolean allParts) throws TException {
    return glueMetastoreClientDelegate.cacheFileMetadata(dbName, tblName, partName, allParts);
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest)
      throws MetaException, NoSuchObjectException, TException {
    // PrimaryKeys are currently unsupported
    // return null to allow DESCRIBE (FORMATTED | EXTENDED)
    return null;
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
      throws MetaException, NoSuchObjectException, TException {
    // PrimaryKeys are currently unsupported
    // return null to allow DESCRIBE (FORMATTED | EXTENDED)
    return null;
  }

  @Override
  public void createTableWithConstraints(
      org.apache.hadoop.hive.metastore.api.Table table,
      List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys
  ) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.createTableWithConstraints(table, primaryKeys, foreignKeys);
  }

  @Override
  public void dropConstraint(
      String dbName,
      String tblName,
      String constraintName
  ) throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.dropConstraint(dbName, tblName, constraintName);
  }

  @Override
  public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
      throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.addPrimaryKey(primaryKeyCols);
  }

  @Override
  public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
      throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.addForeignKey(foreignKeyCols);
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    return glueMetastoreClientDelegate.showCompactions();
  }

  @Override
  public void addDynamicPartitions(
      long txnId,
      String dbName,
      String tblName,
      List<String> partNames
  ) throws TException {
    glueMetastoreClientDelegate.addDynamicPartitions(txnId, dbName, tblName, partNames);
  }

  @Override
  public void addDynamicPartitions(
      long txnId,
      String dbName,
      String tblName,
      List<String> partNames,
      DataOperationType operationType
  ) throws TException {
    glueMetastoreClientDelegate.addDynamicPartitions(txnId, dbName, tblName, partNames, operationType);
  }

  @Override
  public void insertTable(org.apache.hadoop.hive.metastore.api.Table table, boolean overwrite) throws MetaException {
    glueMetastoreClientDelegate.insertTable(table, overwrite);
  }

  @Override
  public NotificationEventResponse getNextNotification(
      long lastEventId, int maxEvents, NotificationFilter notificationFilter) throws TException {
    return glueMetastoreClientDelegate.getNextNotification(lastEventId, maxEvents, notificationFilter);
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return glueMetastoreClientDelegate.getCurrentNotificationEventId();
  }

  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
    return glueMetastoreClientDelegate.fireListenerEvent(fireEventRequest);
  }

  @Override
  public ShowLocksResponse showLocks() throws TException {
    return glueMetastoreClientDelegate.showLocks();
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    return glueMetastoreClientDelegate.showLocks(showLocksRequest);
  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    return glueMetastoreClientDelegate.showTxns();
  }

  @Deprecated
  public boolean tableExists(String tableName) throws MetaException, TException, UnknownDBException {
    // this method has been deprecated;
    return tableExists(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws MetaException, TException,
        UnknownDBException {
    return glueMetastoreClientDelegate.tableExists(databaseName, tableName);
  }

  @Override
  public void unlock(long lockId) throws NoSuchLockException, TxnOpenException, TException {
    glueMetastoreClientDelegate.unlock(lockId);
  }

  @Override
  public boolean updatePartitionColumnStatistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.updatePartitionColumnStatistics(columnStatistics);
  }

  @Override
  public boolean updateTableColumnStatistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.updateTableColumnStatistics(columnStatistics);
  }

  @Override
  public void validatePartitionNameCharacters(List<String> part_vals) throws TException, MetaException {
    try {
      String partitionValidationRegex = conf.getVar(ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN);
      Pattern partitionValidationPattern = Strings.isNullOrEmpty(partitionValidationRegex) ? null
            : Pattern.compile(partitionValidationRegex);
      MetaStoreUtils.validatePartitionNameCharacters(part_vals, partitionValidationPattern);
    } catch (Exception e) {
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else {
        throw new MetaException(e.getMessage());
      }
    }
  }

  private Path constructRenamedPath(Path defaultNewPath, Path currentPath) {
    URI currentUri = currentPath.toUri();

    return new Path(currentUri.getScheme(), currentUri.getAuthority(),
          defaultNewPath.toUri().getPath());
  }

}
