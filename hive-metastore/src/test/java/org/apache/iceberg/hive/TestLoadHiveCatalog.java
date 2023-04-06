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

import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLoadHiveCatalog {

  private static TestHiveMetastore metastore;

  @BeforeClass
  public static void startMetastore() throws Exception {
    HiveConf hiveConf = new HiveConf(TestLoadHiveCatalog.class);
    metastore = new TestHiveMetastore();
    metastore.start(hiveConf);
  }

  @AfterClass
  public static void stopMetastore() throws Exception {
    if (metastore != null) {
      metastore.stop();
      metastore = null;
    }
  }

  @Test
  public void testCustomCacheKeys() throws Exception {
    HiveCatalog hiveCatalog1 =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                Collections.emptyMap(),
                metastore.hiveConf());
    HiveCatalog hiveCatalog2 =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                Collections.emptyMap(),
                metastore.hiveConf());

    CachedClientPool clientPool1 = (CachedClientPool) hiveCatalog1.clientPool();
    CachedClientPool clientPool2 = (CachedClientPool) hiveCatalog2.clientPool();
    Assert.assertSame(clientPool1.clientPool(), clientPool2.clientPool());

    Configuration conf1 = new Configuration(metastore.hiveConf());
    Configuration conf2 = new Configuration(metastore.hiveConf());
    conf1.set("any.key", "any.value");
    conf2.set("any.key", "any.value");
    hiveCatalog1 =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                ImmutableMap.of(CatalogProperties.CLIENT_POOL_CACHE_KEYS, "conf:any.key"),
                conf1);
    hiveCatalog2 =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                ImmutableMap.of(CatalogProperties.CLIENT_POOL_CACHE_KEYS, "conf:any.key"),
                conf2);
    clientPool1 = (CachedClientPool) hiveCatalog1.clientPool();
    clientPool2 = (CachedClientPool) hiveCatalog2.clientPool();
    Assert.assertSame(clientPool1.clientPool(), clientPool2.clientPool());

    conf2.set("any.key", "any.value2");
    hiveCatalog2 =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                ImmutableMap.of(CatalogProperties.CLIENT_POOL_CACHE_KEYS, "conf:any.key"),
                conf2);
    clientPool2 = (CachedClientPool) hiveCatalog2.clientPool();
    Assert.assertNotSame(clientPool1.clientPool(), clientPool2.clientPool());
  }
}
