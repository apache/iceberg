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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@ExtendWith(MockitoExtension.class)
public final class TestS3InputStream {

  @Mock private S3Client s3Client;
  @Mock private InputStream inputStream;

  private S3InputStream s3InputStream;

  @BeforeEach
  void before() {
    // lenient: the metrics tests re-stub getObject with their own data streams
    lenient()
        .when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
        .thenReturn(inputStream);
    s3InputStream = new S3InputStream(s3Client, mock());
  }

  @Test
  void testReadFullyClosesTheStream() throws IOException {
    s3InputStream.readFully(0, new byte[0]);

    verify(inputStream).close();
  }

  @Test
  void testReadTailClosesTheStream() throws IOException {
    s3InputStream.readTail(new byte[0], 0, 0);

    verify(inputStream).close();
  }

  @Test
  void testReadFullyTracksMetrics() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
        .thenReturn(new ByteArrayInputStream(data));

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    try (S3InputStream in =
        new S3InputStream(s3Client, mock(), new S3FileIOProperties(), metrics)) {
      in.readFully(0, new byte[data.length], 0, data.length);

      assertThat(readBytes.value()).isEqualTo(data.length);
      assertThat(readOperations.value()).isEqualTo(1);
    }
  }

  @Test
  void testReadTailTracksMetrics() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4};
    when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
        .thenReturn(new ByteArrayInputStream(data));

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    try (S3InputStream in =
        new S3InputStream(s3Client, mock(), new S3FileIOProperties(), metrics)) {
      int bytesRead = in.readTail(new byte[data.length], 0, data.length);

      assertThat(bytesRead).isEqualTo(data.length);
      assertThat(readBytes.value()).isEqualTo(data.length);
      assertThat(readOperations.value()).isEqualTo(1);
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
