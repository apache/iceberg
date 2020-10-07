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

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.relocated.com.google.common.cache.Cache;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_SIZE;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_TTL_MINS;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_SIZE;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_TTL_MINS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAWSGlueMetastoreCacheDecorator {

  private AWSGlueMetastore glueMetastore;
  private HiveConf hiveConf;

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "table";
  private static final AWSGlueMetastoreCacheDecorator.TableIdentifier TABLE_IDENTIFIER =
      new AWSGlueMetastoreCacheDecorator.TableIdentifier(DB_NAME, TABLE_NAME);

  @Before
  public void before() {
    glueMetastore = mock(AWSGlueMetastore.class);
    hiveConf = spy(new HiveConf());
    when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
    when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(true);
    when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_SIZE, 0)).thenReturn(100);
    when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_TTL_MINS, 0)).thenReturn(100);
    when(hiveConf.getInt(AWS_GLUE_DB_CACHE_SIZE, 0)).thenReturn(100);
    when(hiveConf.getInt(AWS_GLUE_DB_CACHE_TTL_MINS, 0)).thenReturn(100);

  }

  @Test(expected = NullPointerException.class)
  public void testConstructorWithNullConf() {
    new AWSGlueMetastoreCacheDecorator(null, glueMetastore);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithInvalidTableCacheSize() {
    when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_SIZE, 0)).thenReturn(0);
    new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithInvalidTableCacheTtl() {
    when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_TTL_MINS, 0)).thenReturn(0);
    new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithInvalidDbCacheSize() {
    when(hiveConf.getInt(AWS_GLUE_DB_CACHE_SIZE, 0)).thenReturn(0);
    new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithInvalidDbCacheTtl() {
    when(hiveConf.getInt(AWS_GLUE_DB_CACHE_TTL_MINS, 0)).thenReturn(0);
    new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
  }

  @Test
  public void testGetDatabaseWhenCacheDisabled() {
    // disable cache
    when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
    Database db = new Database();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    when(glueMetastore.getDatabase(DB_NAME)).thenReturn(db);
    assertEquals(db, cacheDecorator.getDatabase(DB_NAME));
    assertNull(cacheDecorator.getDatabaseCache());
    verify(glueMetastore, times(1)).getDatabase(DB_NAME);
  }

  @Test
  public void testGetDatabaseWhenCacheEnabledAndCacheMiss() {
    Database db = new Database();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    assertNotNull(cacheDecorator.getDatabaseCache());
    Cache dbCache = mock(Cache.class);
    cacheDecorator.setDatabaseCache(dbCache);

    when(dbCache.getIfPresent(DB_NAME)).thenReturn(null);
    when(glueMetastore.getDatabase(DB_NAME)).thenReturn(db);
    doNothing().when(dbCache).put(DB_NAME, db);

    assertEquals(db, cacheDecorator.getDatabase(DB_NAME));

    verify(glueMetastore, times(1)).getDatabase(DB_NAME);
    verify(dbCache, times(1)).getIfPresent(DB_NAME);
    verify(dbCache, times(1)).put(DB_NAME, db);
  }

  @Test
  public void testGetDatabaseWhenCacheEnabledAndCacheHit() {
    Database db = new Database();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    assertNotNull(cacheDecorator.getDatabaseCache());
    Cache dbCache = mock(Cache.class);
    cacheDecorator.setDatabaseCache(dbCache);

    when(dbCache.getIfPresent(DB_NAME)).thenReturn(db);

    assertEquals(db, cacheDecorator.getDatabase(DB_NAME));

    verify(dbCache, times(1)).getIfPresent(DB_NAME);
  }

  @Test
  public void testUpdateDatabaseWhenCacheDisabled() {
    // disable cache
    when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
    DatabaseInput dbInput = new DatabaseInput();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    doNothing().when(glueMetastore).updateDatabase(DB_NAME, dbInput);
    cacheDecorator.updateDatabase(DB_NAME, dbInput);
    assertNull(cacheDecorator.getDatabaseCache());
    verify(glueMetastore, times(1)).updateDatabase(DB_NAME, dbInput);
  }

  @Test
  public void testUpdateDatabaseWhenCacheEnabled() {
    DatabaseInput dbInput = new DatabaseInput();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    cacheDecorator.getDatabaseCache().put(DB_NAME, new Database());
    doNothing().when(glueMetastore).updateDatabase(DB_NAME, dbInput);

    cacheDecorator.updateDatabase(DB_NAME, dbInput);

    // db should have been removed from cache
    assertNull(cacheDecorator.getDatabaseCache().getIfPresent(DB_NAME));
    verify(glueMetastore, times(1)).updateDatabase(DB_NAME, dbInput);
  }

  @Test
  public void testDeleteDatabaseWhenCacheDisabled() {
    // disable cache
    when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    doNothing().when(glueMetastore).deleteDatabase(DB_NAME);
    cacheDecorator.deleteDatabase(DB_NAME);
    assertNull(cacheDecorator.getDatabaseCache());
    verify(glueMetastore, times(1)).deleteDatabase(DB_NAME);
  }

  @Test
  public void testDeleteDatabaseWhenCacheEnabled() {
    DatabaseInput dbInput = new DatabaseInput();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    cacheDecorator.getDatabaseCache().put(DB_NAME, new Database());
    doNothing().when(glueMetastore).deleteDatabase(DB_NAME);

    cacheDecorator.deleteDatabase(DB_NAME);

    // db should have been removed from cache
    assertNull(cacheDecorator.getDatabaseCache().getIfPresent(DB_NAME));
    verify(glueMetastore, times(1)).deleteDatabase(DB_NAME);
  }

  @Test
  public void testGetTableWhenCacheDisabled() {
    // disable cache
    when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(false);
    Table table = new Table();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    when(glueMetastore.getTable(DB_NAME, TABLE_NAME)).thenReturn(table);
    assertEquals(table, cacheDecorator.getTable(DB_NAME, TABLE_NAME));
    assertNull(cacheDecorator.getTableCache());
    verify(glueMetastore, times(1)).getTable(DB_NAME, TABLE_NAME);
  }

  @Test
  public void testGetTableWhenCacheEnabledAndCacheMiss() {
    Table table = new Table();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    assertNotNull(cacheDecorator.getTableCache());
    Cache tableCache = mock(Cache.class);
    cacheDecorator.setTableCache(tableCache);

    when(tableCache.getIfPresent(TABLE_IDENTIFIER)).thenReturn(null);
    when(glueMetastore.getTable(DB_NAME, TABLE_NAME)).thenReturn(table);
    doNothing().when(tableCache).put(TABLE_IDENTIFIER, table);

    assertEquals(table, cacheDecorator.getTable(DB_NAME, TABLE_NAME));

    verify(glueMetastore, times(1)).getTable(DB_NAME, TABLE_NAME);
    verify(tableCache, times(1)).getIfPresent(TABLE_IDENTIFIER);
    verify(tableCache, times(1)).put(TABLE_IDENTIFIER, table);
  }

  @Test
  public void testGetTableWhenCacheEnabledAndCacheHit() {
    Table table = new Table();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    assertNotNull(cacheDecorator.getTableCache());
    Cache tableCache = mock(Cache.class);
    cacheDecorator.setTableCache(tableCache);

    when(tableCache.getIfPresent(TABLE_IDENTIFIER)).thenReturn(table);

    assertEquals(table, cacheDecorator.getTable(DB_NAME, TABLE_NAME));

    verify(tableCache, times(1)).getIfPresent(TABLE_IDENTIFIER);
  }

  @Test
  public void testUpdateTableWhenCacheDisabled() {
    // disable cache
    when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(false);
    TableInput tableInput = new TableInput();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    doNothing().when(glueMetastore).updateTable(TABLE_NAME, tableInput);
    cacheDecorator.updateTable(TABLE_NAME, tableInput);
    assertNull(cacheDecorator.getTableCache());
    verify(glueMetastore, times(1)).updateTable(TABLE_NAME, tableInput);
  }

  @Test
  public void testUpdateTableWhenCacheEnabled() {
    TableInput tableInput = new TableInput();
    tableInput.setName(TABLE_NAME);
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);

    cacheDecorator.getTableCache().put(TABLE_IDENTIFIER, new Table());
    doNothing().when(glueMetastore).updateTable(DB_NAME, tableInput);

    cacheDecorator.updateTable(DB_NAME, tableInput);

    // table should have been removed from cache
    assertNull(cacheDecorator.getTableCache().getIfPresent(TABLE_IDENTIFIER));
    verify(glueMetastore, times(1)).updateTable(DB_NAME, tableInput);
  }

  @Test
  public void testDeleteTableWhenCacheDisabled() {
    // disable cache
    when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(false);
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    doNothing().when(glueMetastore).deleteTable(DB_NAME, TABLE_NAME);
    cacheDecorator.deleteTable(DB_NAME, TABLE_NAME);
    assertNull(cacheDecorator.getTableCache());
    verify(glueMetastore, times(1)).deleteTable(DB_NAME, TABLE_NAME);
  }

  @Test
  public void testDeleteTableWhenCacheEnabled() {
    DatabaseInput dbInput = new DatabaseInput();
    AWSGlueMetastoreCacheDecorator cacheDecorator =
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    cacheDecorator.getTableCache().put(TABLE_IDENTIFIER, new Table());
    doNothing().when(glueMetastore).deleteDatabase(DB_NAME);

    cacheDecorator.deleteTable(DB_NAME, TABLE_NAME);

    // table should have been removed from cache
    assertNull(cacheDecorator.getTableCache().getIfPresent(TABLE_IDENTIFIER));
    verify(glueMetastore, times(1)).deleteTable(DB_NAME, TABLE_NAME);
  }

}
