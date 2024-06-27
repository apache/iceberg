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
package org.apache.iceberg.flink.maintenance.operator;

import static org.apache.iceberg.flink.MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.RegisterExtension;

class OperatorTestBase {
  private static final int NUMBER_TASK_MANAGERS = 1;
  private static final int SLOTS_PER_TASK_MANAGER = 8;

  static final String TABLE_NAME = "test_table";

  @RegisterExtension
  protected static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
              .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
              .setConfiguration(new Configuration(DISABLE_CLASSLOADER_CHECK_CONFIG))
              .build());

  @RegisterExtension
  final FlinkSqlExtension sql =
      new FlinkSqlExtension(
          "catalog",
          ImmutableMap.of("type", "iceberg", FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop"),
          "db");
}
