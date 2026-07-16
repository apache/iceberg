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
package org.apache.iceberg.aws.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;

public class TestAnalyticsAcceleratorInputStreamWrapper {

  @Test
  public void testReadTracksMetrics() throws IOException {
    S3SeekableInputStream delegate = mock(S3SeekableInputStream.class);
    // first a single-byte read, then a buffered read of 8 bytes, then EOF
    when(delegate.read()).thenReturn(1);
    when(delegate.read(any(byte[].class), anyInt(), anyInt())).thenReturn(8).thenReturn(-1);

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    try (AnalyticsAcceleratorInputStreamWrapper in =
        new AnalyticsAcceleratorInputStreamWrapper(delegate, metrics)) {
      assertThat(in.read()).isEqualTo(1);
      assertThat(readBytes.value()).isEqualTo(1);
      assertThat(readOperations.value()).isEqualTo(1);

      assertThat(in.read(new byte[16], 0, 16)).isEqualTo(8);
      assertThat(readBytes.value()).isEqualTo(9);
      assertThat(readOperations.value()).isEqualTo(2);

      // an EOF read counts neither bytes nor an operation
      assertThat(in.read(new byte[16], 0, 16)).isEqualTo(-1);
      assertThat(readBytes.value()).isEqualTo(9);
      assertThat(readOperations.value()).isEqualTo(2);
    }
  }

  /**
   * A {@link MetricsContext} that returns the same {@link Counter} instance for a given name, so
   * that tests can observe the counters the stream under test increments. {@link
   * DefaultMetricsContext} allocates a fresh counter on every {@code counter(...)} call.
   */
  private static class CachingMetricsContext extends DefaultMetricsContext {
    private final Map<String, org.apache.iceberg.metrics.Counter> counters =
        Maps.newConcurrentMap();

    @Override
    public org.apache.iceberg.metrics.Counter counter(String name, Unit unit) {
      return counters.computeIfAbsent(name, ignored -> super.counter(name, unit));
    }
  }
}
