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
package org.apache.iceberg.gcp.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.storage.BlobId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestAnalyticsCoreUtil {

  @Test
  public void readVectored() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream gcsInputStream = mock(GoogleCloudStorageInputStream.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");

    SeekableInputStream stream;
    try (MockedStatic<GoogleCloudStorageInputStream> mocked =
        mockStatic(GoogleCloudStorageInputStream.class)) {
      mocked
          .when(() -> GoogleCloudStorageInputStream.create(eq(fileSystem), any(GcsItemId.class)))
          .thenReturn(gcsInputStream);
      stream = AnalyticsCoreUtil.newStream(fileSystem, blobId, null, MetricsContext.nullMetrics());
    }

    CompletableFuture<ByteBuffer> future1 = new CompletableFuture<>();
    CompletableFuture<ByteBuffer> future2 = new CompletableFuture<>();
    List<FileRange> ranges =
        List.of(new FileRange(future1, 10L, 100), new FileRange(future2, 0, 50));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    ((RangeReadable) stream).readVectored(ranges, allocate);

    List<GcsObjectRange> objectRanges =
        List.of(
            GcsObjectRange.builder()
                .setOffset(10)
                .setLength(100)
                .setByteBufferFuture(future1)
                .build(),
            GcsObjectRange.builder()
                .setOffset(0)
                .setLength(50)
                .setByteBufferFuture(future2)
                .build());
    verify(gcsInputStream).readVectored(objectRanges, allocate);
  }

  @Test
  public void readVectoredCountsRequestedLengthSynchronously() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream gcsInputStream = mock(GoogleCloudStorageInputStream.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    SeekableInputStream stream;
    try (MockedStatic<GoogleCloudStorageInputStream> mocked =
        mockStatic(GoogleCloudStorageInputStream.class)) {
      mocked
          .when(() -> GoogleCloudStorageInputStream.create(eq(fileSystem), any(GcsItemId.class)))
          .thenReturn(gcsInputStream);
      stream = AnalyticsCoreUtil.newStream(fileSystem, blobId, null, metrics);
    }

    CompletableFuture<ByteBuffer> future1 = new CompletableFuture<>();
    CompletableFuture<ByteBuffer> future2 = new CompletableFuture<>();
    List<FileRange> ranges =
        List.of(new FileRange(future1, 10L, 100), new FileRange(future2, 200L, 50));

    ((RangeReadable) stream).readVectored(ranges, ByteBuffer::allocate);

    // Bytes are counted synchronously on the caller thread, using the requested range length,
    // before the range futures complete. This is deliberate: the futures complete on analytics-core
    // background threads, and counting there would attribute bytes to the wrong thread's Hadoop
    // Statistics and never reach Spark's task input metrics (see AnalyticsCoreUtil#readVectored).
    // The tradeoff is a bounded over-count on short reads / failed ranges.
    //
    // NOTE: this test uses DefaultMetricsContext (a plain LongAdder), which cannot verify the
    // per-thread Statistics attribution that motivates counting synchronously; that correctness
    // argument rests on the HadoopMetricsContext reasoning, not on this assertion.
    assertThat(readBytes.value()).isEqualTo(150);
    assertThat(readOperations.value()).isEqualTo(2);

    // completing (or failing) the futures afterward does not change the already-recorded metrics
    future1.complete(ByteBuffer.allocate(100));
    future2.completeExceptionally(new IOException("boom"));
    assertThat(readBytes.value()).isEqualTo(150);
    assertThat(readOperations.value()).isEqualTo(2);
  }

  @Test
  public void readDoesNotCountAtEof() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream gcsInputStream = mock(GoogleCloudStorageInputStream.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");
    // a real byte, then EOF; a buffered read of 8 bytes, then EOF
    when(gcsInputStream.read()).thenReturn(42).thenReturn(-1);
    when(gcsInputStream.read(any(byte[].class), anyInt(), anyInt())).thenReturn(8).thenReturn(-1);

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    SeekableInputStream stream;
    try (MockedStatic<GoogleCloudStorageInputStream> mocked =
        mockStatic(GoogleCloudStorageInputStream.class)) {
      mocked
          .when(() -> GoogleCloudStorageInputStream.create(eq(fileSystem), any(GcsItemId.class)))
          .thenReturn(gcsInputStream);
      stream = AnalyticsCoreUtil.newStream(fileSystem, blobId, null, metrics);
    }

    assertThat(stream.read()).isEqualTo(42);
    assertThat(readBytes.value()).isEqualTo(1);
    assertThat(readOperations.value()).isEqualTo(1);

    assertThat(stream.read(new byte[16], 0, 16)).isEqualTo(8);
    assertThat(readBytes.value()).isEqualTo(9);
    assertThat(readOperations.value()).isEqualTo(2);

    // EOF reads count neither bytes nor an operation
    assertThat(stream.read()).isEqualTo(-1);
    assertThat(stream.read(new byte[16], 0, 16)).isEqualTo(-1);
    assertThat(readBytes.value()).isEqualTo(9);
    assertThat(readOperations.value()).isEqualTo(2);
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
