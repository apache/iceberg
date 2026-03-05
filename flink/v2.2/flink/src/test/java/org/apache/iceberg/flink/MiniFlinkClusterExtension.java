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
package org.apache.iceberg.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

public class MiniFlinkClusterExtension {

  private static final int DEFAULT_TM_NUM = 1;
  private static final int DEFAULT_PARALLELISM = 4;

  public static final Configuration DISABLE_CLASSLOADER_CHECK_CONFIG =
      new Configuration()
          // disable classloader check as Avro may cache class/object in the serializers.
          .set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

  private MiniFlinkClusterExtension() {}

  /**
   * It will start a mini cluster with classloader.check-leaked-classloader=false, so that we won't
   * break the unit tests because of the class loader leak issue. In our iceberg integration tests,
   * there're some that will assert the results after finished the flink jobs, so actually we may
   * access the class loader that has been closed by the flink task managers if we enable the switch
   * classloader.check-leaked-classloader by default.
   */
  public static MiniClusterExtension createWithClassloaderCheckDisabled() {
    return new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberTaskManagers(DEFAULT_TM_NUM)
            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
            .setConfiguration(DISABLE_CLASSLOADER_CHECK_CONFIG)
            .build());
  }

  public static MiniClusterExtension createWithClassloaderCheckDisabled(
      InMemoryReporter inMemoryReporter) {
    Configuration configuration = new Configuration(DISABLE_CLASSLOADER_CHECK_CONFIG);
    inMemoryReporter.addToConfiguration(configuration);

    return new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberTaskManagers(DEFAULT_TM_NUM)
            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
            .setConfiguration(configuration)
            .build());
  }
}
