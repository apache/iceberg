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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Test;

class TestIcebergSinkTask {

  @Test
  void startSetsEnvironmentContext() {
    String previousName = EnvironmentContext.remove(EnvironmentContext.ENGINE_NAME);
    String previousVersion = EnvironmentContext.remove(EnvironmentContext.ENGINE_VERSION);
    IcebergSinkTask task = new IcebergSinkTask();

    try {
      task.start(taskConfig());

      assertThat(EnvironmentContext.get())
          .containsEntry(EnvironmentContext.ENGINE_NAME, "kafka-connect")
          .containsEntry(EnvironmentContext.ENGINE_VERSION, AppInfoParser.getVersion());
    } finally {
      task.stop();
      restoreEnvironmentContext(EnvironmentContext.ENGINE_NAME, previousName);
      restoreEnvironmentContext(EnvironmentContext.ENGINE_VERSION, previousVersion);
    }
  }

  private static Map<String, String> taskConfig() {
    return ImmutableMap.of(
        "name", "test-connector",
        "task.id", "0",
        "topics", "mytopic",
        "iceberg.tables", "mytable",
        "iceberg.catalog.catalog-impl", InMemoryCatalog.class.getName());
  }

  private static void restoreEnvironmentContext(String key, String value) {
    if (value != null) {
      EnvironmentContext.put(key, value);
    } else {
      EnvironmentContext.remove(key);
    }
  }
}
