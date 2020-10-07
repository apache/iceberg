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

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.aws.glue.util.AWSGlueConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.cache.Cache;
import org.apache.iceberg.relocated.com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AWSGlueMetastoreCacheDecorator extends AWSGlueMetastoreBaseDecorator {

  private static final Logger LOG = LoggerFactory.getLogger(AWSGlueMetastoreCacheDecorator.class);

  private final HiveConf conf;

  private final boolean databaseCacheEnabled;

  private final boolean tableCacheEnabled;

  private Cache<String, Database> databaseCache;

  private Cache<TableIdentifier, Table> tableCache;

  public AWSGlueMetastoreCacheDecorator(HiveConf conf, AWSGlueMetastore awsGlueMetastore) {
    super(awsGlueMetastore);

    Preconditions.checkNotNull(conf, "conf can not be null");
    this.conf = conf;

    databaseCacheEnabled = conf.getBoolean(AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE, false);
    if (databaseCacheEnabled) {
      int dbCacheSize = conf.getInt(AWSGlueConfig.AWS_GLUE_DB_CACHE_SIZE, 0);
      int dbCacheTtlMins = conf.getInt(AWSGlueConfig.AWS_GLUE_DB_CACHE_TTL_MINS, 0);

      // validate config values for size and ttl
      validateConfigValueIsGreaterThanZero(AWSGlueConfig.AWS_GLUE_DB_CACHE_SIZE, dbCacheSize);
      validateConfigValueIsGreaterThanZero(AWSGlueConfig.AWS_GLUE_DB_CACHE_TTL_MINS, dbCacheTtlMins);

      // initialize database cache
      databaseCache = CacheBuilder.newBuilder().maximumSize(dbCacheSize)
          .expireAfterWrite(dbCacheTtlMins, TimeUnit.MINUTES).build();
    } else {
      databaseCache = null;
    }

    tableCacheEnabled = conf.getBoolean(AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE, false);
    if (tableCacheEnabled) {
      int tableCacheSize = conf.getInt(AWSGlueConfig.AWS_GLUE_TABLE_CACHE_SIZE, 0);
      int tableCacheTtlMins = conf.getInt(AWSGlueConfig.AWS_GLUE_TABLE_CACHE_TTL_MINS, 0);

      // validate config values for size and ttl
      validateConfigValueIsGreaterThanZero(AWSGlueConfig.AWS_GLUE_TABLE_CACHE_SIZE, tableCacheSize);
      validateConfigValueIsGreaterThanZero(AWSGlueConfig.AWS_GLUE_TABLE_CACHE_TTL_MINS, tableCacheTtlMins);

      // initialize table cache
      tableCache = CacheBuilder.newBuilder().maximumSize(tableCacheSize)
          .expireAfterWrite(tableCacheTtlMins, TimeUnit.MINUTES).build();
    } else {
      tableCache = null;
    }

    LOG.info("Constructed");
  }

  private void validateConfigValueIsGreaterThanZero(String configName, int value) {
    Preconditions.checkArgument(value > 0, String.format("Invalid value for Hive Config %s. " +
        "Provide a value greater than zero", configName));

  }

  @Override
  public Database getDatabase(String dbName) {
    Database result;
    if (databaseCacheEnabled) {
      Database valueFromCache = databaseCache.getIfPresent(dbName);
      if (valueFromCache != null) {
        LOG.info("Cache hit for operation [getDatabase] on key [{}]", dbName);
        result = valueFromCache;
      } else {
        LOG.info("Cache miss for operation [getDatabase] on key [{}]", dbName);
        result = super.getDatabase(dbName);
        databaseCache.put(dbName, result);
      }
    } else {
      result = super.getDatabase(dbName);
    }
    return result;
  }

  @Override
  public void updateDatabase(String dbName, DatabaseInput databaseInput) {
    super.updateDatabase(dbName, databaseInput);
    if (databaseCacheEnabled) {
      purgeDatabaseFromCache(dbName);
    }
  }

  @Override
  public void deleteDatabase(String dbName) {
    super.deleteDatabase(dbName);
    if (databaseCacheEnabled) {
      purgeDatabaseFromCache(dbName);
    }
  }

  private void purgeDatabaseFromCache(String dbName) {
    databaseCache.invalidate(dbName);
  }

  @Override
  public Table getTable(String dbName, String tableName) {
    Table result;
    if (tableCacheEnabled) {
      TableIdentifier key = new TableIdentifier(dbName, tableName);
      Table valueFromCache = tableCache.getIfPresent(key);
      if (valueFromCache != null) {
        LOG.info("Cache hit for operation [getTable] on key [{}]", key);
        result = valueFromCache;
      } else {
        LOG.info("Cache miss for operation [getTable] on key [{}]", key);
        result = super.getTable(dbName, tableName);
        tableCache.put(key, result);
      }
    } else {
      result = super.getTable(dbName, tableName);
    }
    return result;
  }

  @Override
  public void updateTable(String dbName, TableInput tableInput) {
    super.updateTable(dbName, tableInput);
    if (tableCacheEnabled) {
      purgeTableFromCache(dbName, tableInput.getName());
    }
  }

  @Override
  public void deleteTable(String dbName, String tableName) {
    super.deleteTable(dbName, tableName);
    if (tableCacheEnabled) {
      purgeTableFromCache(dbName, tableName);
    }
  }

  private void purgeTableFromCache(String dbName, String tableName) {
    TableIdentifier key = new TableIdentifier(dbName, tableName);
    tableCache.invalidate(key);
  }


  static class TableIdentifier {
    private final String dbName;
    private final String tableName;

    TableIdentifier(String dbName, String tableName) {
      this.dbName = dbName;
      this.tableName = tableName;
    }

    public String getDbName() {
      return dbName;
    }

    public String getTableName() {
      return tableName;
    }

    @Override
    public String toString() {
      return "TableIdentifier{" +
          "dbName='" + dbName + '\'' +
          ", tableName='" + tableName + '\'' +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TableIdentifier that = (TableIdentifier) o;
      return Objects.equals(dbName, that.dbName) &&
          Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dbName, tableName);
    }
  }

  Cache<String, Database> getDatabaseCache() {
    return databaseCache;
  }

  void setDatabaseCache(Cache<String, Database> databaseCache) {
    this.databaseCache = databaseCache;
  }

  Cache<TableIdentifier, Table> getTableCache() {
    return tableCache;
  }

  void setTableCache(Cache<TableIdentifier, Table> tableCache) {
    this.tableCache = tableCache;
  }
}
