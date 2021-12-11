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

package org.apache.iceberg;

import java.util.concurrent.TimeUnit;

public class CatalogProperties {

  private CatalogProperties() {
  }

  public static final String CATALOG_IMPL = "catalog-impl";
  public static final String FILE_IO_IMPL = "io-impl";
  public static final String WAREHOUSE_LOCATION = "warehouse";

  public static final String URI = "uri";
  public static final String CLIENT_POOL_SIZE = "clients";
  public static final int CLIENT_POOL_SIZE_DEFAULT = 2;
  public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS = "client.pool.cache.eviction-interval-ms";
  public static final long CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(5);

  public static final String APP_ID = "app-id";
  public static final String USER = "user";

}
