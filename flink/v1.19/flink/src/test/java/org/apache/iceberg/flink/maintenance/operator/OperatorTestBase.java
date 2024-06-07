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

import java.io.File;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.awaitility.Awaitility;
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
              .setConfiguration(config())
              .build());

  @RegisterExtension
  final FlinkSqlExtension sql =
      new FlinkSqlExtension(
          "catalog",
          ImmutableMap.of("type", "iceberg", FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop"),
          "db");

  private static Configuration config() {
    Configuration config = new Configuration(DISABLE_CLASSLOADER_CHECK_CONFIG);
    MetricOptions.forReporter(config, "test_reporter")
        .set(MetricOptions.REPORTER_FACTORY_CLASS, MetricsReporterFactoryForTests.class.getName());
    return config;
  }

  /**
   * Close the {@link JobClient} and wait for the job closure. If the savepointDir is specified, it
   * stops the job with a savepoint.
   *
   * @param jobClient the job to close
   * @param savepointDir the savepointDir to store the last savepoint. If <code>null</code> then
   *     stop without a savepoint.
   * @return configuration for restarting the job from the savepoint
   */
  public static Configuration closeJobClient(JobClient jobClient, File savepointDir) {
    Configuration conf = new Configuration();
    if (jobClient != null) {
      if (savepointDir != null) {
        // Stop with savepoint
        jobClient.stopWithSavepoint(false, savepointDir.getPath(), SavepointFormatType.CANONICAL);
        // Wait until the savepoint is created and the job has been stopped
        Awaitility.await().until(() -> savepointDir.listFiles(File::isDirectory).length == 1);
        conf.set(
            SavepointConfigOptions.SAVEPOINT_PATH,
            savepointDir.listFiles(File::isDirectory)[0].getAbsolutePath());
      } else {
        jobClient.cancel();
      }

      // Wait until the job has been stopped
      Awaitility.await().until(() -> jobClient.getJobStatus().get().isTerminalState());
      return conf;
    }

    return null;
  }

  /**
   * Close the {@link JobClient} and wait for the job closure.
   *
   * @param jobClient the job to close
   */
  public static void closeJobClient(JobClient jobClient) {
    closeJobClient(jobClient, null);
  }
}
