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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

public class HiveClientPoolProvider {

  private static Cache<String, HiveClientPool> clientPoolCache;

  private final Configuration conf;
  private final String metastoreUri;
  private final int clientPoolSize;
  private final long evictionInterval;

  HiveClientPoolProvider(Configuration conf) {
    this.conf = conf;
    this.metastoreUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
    this.clientPoolSize = conf.getInt(CatalogProperties.CLIENT_POOL_SIZE,
            CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
    this.evictionInterval = conf.getLong(CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
    init();
  }

  /**
   * Callers must not store the HiveClientPool instance returned by this method.
   * @return
   */
  public HiveClientPool clientPool() {
    return clientPoolCache.get(metastoreUri, k -> new HiveClientPool(clientPoolSize, conf));
  }

  private synchronized void init() {
    if (clientPoolCache == null) {
      clientPoolCache = Caffeine.newBuilder().expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
              .removalListener((key, value, cause) -> ((HiveClientPool) value).close())
              .build();
    }
  }

  @VisibleForTesting
  static Cache<String, HiveClientPool> clientPoolCache() {
    return clientPoolCache;
  }
}
