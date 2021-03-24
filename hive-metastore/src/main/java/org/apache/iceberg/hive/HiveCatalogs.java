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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public final class HiveCatalogs {

  private static final Cache<String, HiveCatalog> CATALOG_CACHE = Caffeine.newBuilder().build();

  private HiveCatalogs() {
  }

  /**
   * @deprecated please use the no-arg constructor, setConf and initialize to construct the catalog. Will be removed in
   * v0.12.0
   */
  @Deprecated
  public static HiveCatalog loadCatalog(Configuration conf) {
    // metastore URI can be null in local mode
    String metastoreUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
    return CATALOG_CACHE.get(metastoreUri, uri -> (HiveCatalog)
        CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", ImmutableMap.of(), conf));
  }
}
