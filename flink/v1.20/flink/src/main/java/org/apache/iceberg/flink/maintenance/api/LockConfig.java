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
package org.apache.iceberg.flink.maintenance.api;

public class LockConfig {

  private LockConfig() {}

  public static final String CONFIG_PREFIX = TableMaintenanceConfig.CONFIG_PREFIX + "lock.";
  public static final String LOCK_TYPE = "type";
  public static final String LOCK_ID = "lock-id";

  // JDBC
  public static final String JDBC = "jdbc";
  public static final String JDBC_URI = JDBC + ".uri";
  public static final String JDBC_INIT_LOCK_TABLE = JDBC + ".init-lock-table";

  // zk
  public static final String ZK = "zookeeper";
  public static final String ZK_URI = ZK + ".uri";
  public static final String ZK_SESSION_TIMEOUT_MS = ZK + ".session-timeout-ms";
  public static final String ZK_CONNECTION_TIMEOUT_MS = ZK + ".connection-timeout-ms";
  public static final String ZK_BASE_SLEEP_MS = ZK + ".base-sleep-ms";
  public static final String ZK_MAX_RETRIES = ZK + ".max-retries";

  public static final int ZK_SESSION_TIMEOUT_MS_DEFAULT = 60000;
  public static final int ZK_CONNECTION_TIMEOUT_MS_DEFAULT = 15000;
  public static final int ZK_BASE_SLEEP_MS_DEFAULT = 3000;
  public static final int ZK_MAX_RETRIES_DEFAULT = 3;
}
