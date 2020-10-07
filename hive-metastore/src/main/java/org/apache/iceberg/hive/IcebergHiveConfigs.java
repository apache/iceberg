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

public class IcebergHiveConfigs {

  private IcebergHiveConfigs() {
  }

  public static final String HIVE_CLIENT_POOL_SIZE = "iceberg.hive.client-pool-size";
  public static final int HIVE_CLIENT_POOL_SIZE_DEFAULT = 5;

  public static final String HIVE_CLIENT_IMPL = "iceberg.hive.client-impl";
  public static final String HIVE_CLIENT_IMPL_DEFAULT = "org.apache.hadoop.hive.metastore.HiveMetaStoreClient";

  public static final String HIVE_ACQUIRE_LOCK_STATE_TIMEOUT_MS = "iceberg.hive.lock-timeout-ms";
  public static final long HIVE_ACQUIRE_LOCK_STATE_TIMEOUT_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes

}
