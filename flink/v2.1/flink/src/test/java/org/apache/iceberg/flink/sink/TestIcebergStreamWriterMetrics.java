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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.net.URL;
import java.net.URLClassLoader;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.Test;

public class TestIcebergStreamWriterMetrics {

  @Test
  void histogramsCreatedWhenDropwizardAvailable() {
    IcebergStreamWriterMetrics metrics =
        new IcebergStreamWriterMetrics(
            UnregisteredMetricsGroup.createSinkWriterMetricGroup(), "db.table");

    assertThat(metrics.getDataFilesSizeHistogram()).isNotNull();
    assertThat(metrics.getDeleteFilesSizeHistogram()).isNotNull();

    assertThatNoException()
        .isThrownBy(() -> metrics.updateFlushResult(WriteResult.builder().build()));
  }

  @Test
  void histogramsSkippedWhenDropwizardMissing() throws Exception {
    try (EmptyClassloader loader = new EmptyClassloader()) {

      IcebergStreamWriterMetrics metrics =
          new IcebergStreamWriterMetrics(
              UnregisteredMetricsGroup.createSinkWriterMetricGroup(), "db.table", loader);

      assertThat(metrics.getDataFilesSizeHistogram()).isNull();
      assertThat(metrics.getDeleteFilesSizeHistogram()).isNull();

      assertThatNoException()
          .isThrownBy(() -> metrics.updateFlushResult(WriteResult.builder().build()));
    }
  }

  private static final class EmptyClassloader extends URLClassLoader {

    EmptyClassloader() {
      super(new URL[0]);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      throw new ClassNotFoundException(name);
    }
  }
}
