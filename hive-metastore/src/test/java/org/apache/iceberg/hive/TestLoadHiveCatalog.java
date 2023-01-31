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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;
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
  public void testCustomClientPool() throws Exception {
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.CLIENT_POOL_IMPL, CachedClientPoolWrapper.class.getName());
    HiveCatalog hiveCatalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                properties,
                metastore.hiveConf());
    Assert.assertTrue(hiveCatalog.clientPool() instanceof CachedClientPoolWrapper);
    Assert.assertNotNull(((CachedClientPoolWrapper) hiveCatalog.clientPool()).hadoopConf);
  }

  public static class CachedClientPoolWrapper
      implements ClientPool<IMetaStoreClient, TException>, Configurable<Configuration> {

    private CachedClientPool delegate;
    private Configuration hadoopConf;

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action)
        throws TException, InterruptedException {
      return delegate.run(action);
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
        throws TException, InterruptedException {
      return delegate.run(action, retry);
    }

    @Override
    public void initialize(Map<String, String> properties) {
      delegate = new CachedClientPool(hadoopConf, properties);
    }

    @Override
    public void setConf(Configuration conf) {
      this.hadoopConf = conf;
    }
  }
}
