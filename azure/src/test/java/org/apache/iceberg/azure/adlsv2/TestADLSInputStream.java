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
package org.apache.iceberg.azure.adlsv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.implementation.models.InternalDataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.PathProperties;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
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

@ExtendWith(MockitoExtension.class)
class TestADLSInputStream {
  private static final int EOF = -1;

  @Mock private DataLakeFileClient fileClient;
  @Mock private InputStream inputStream;

  private ADLSInputStream adlsInputStream;

  @BeforeEach
  void before() {
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(inputStream, mock());
    when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);
    adlsInputStream =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            0L,
            mock(),
            MetricsContext.nullMetrics());
  }

  @Test
  void testReadSingle() throws IOException {
    int i0 = 1;
    int i1 = 255;
    byte[] data = {(byte) i0, (byte) i1};
    InputStream byteStream = new ByteArrayInputStream(data);
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(byteStream, mock());
    when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);

    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            2L,
            mock(),
            MetricsContext.nullMetrics())) {

      assertThat(in.read()).isEqualTo(i0);
      assertThat(in.read()).isEqualTo(i1);
      assertThat(in.read()).isEqualTo(EOF);
    }
  }

  @Test
  void testReadBufferedEOF() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    InputStream byteStream = new ByteArrayInputStream(data);
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(byteStream, mock());
    when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);

    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            8L,
            mock(),
            MetricsContext.nullMetrics())) {

      byte[] actual = new byte[10];
      int bytesRead = in.read(actual, 0, 10);
      assertThat(bytesRead).isEqualTo(8);
      assertThat(Arrays.copyOfRange(actual, 0, bytesRead)).isEqualTo(data);

      assertThat(in.read(actual, 0, 10)).isEqualTo(EOF);
      assertThat(in.getPos()).isEqualTo(8);
    }
  }

  @Test
  void testReadFullyClosesTheStream() throws IOException {
    adlsInputStream.readFully(0, new byte[0]);

    verify(inputStream).close();
  }

  @Test
  void testReadTailClosesTheStream() throws IOException {
    adlsInputStream.readTail(new byte[0], 0, 0);

    verify(inputStream).close();
  }

  @Test
  void testReadFullyTracksMetrics() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    InputStream byteStream = new ByteArrayInputStream(data);
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(byteStream, mock());
    when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            (long) data.length,
            mock(),
            metrics)) {
      in.readFully(0, new byte[data.length], 0, data.length);

      assertThat(readBytes.value()).isEqualTo(data.length);
      assertThat(readOperations.value()).isEqualTo(1);
    }
  }

  @Test
  void testReadTailTracksMetrics() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    InputStream byteStream = new ByteArrayInputStream(data);
    // the constructor's openStream() reads the file size from the stream result, so report the
    // real length; otherwise readTail computes a negative start offset
    PathProperties properties = mock(PathProperties.class);
    when(properties.getFileSize()).thenReturn((long) data.length);
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(byteStream, properties);
    when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            (long) data.length,
            mock(),
            metrics)) {
      int tailLength = 4;
      int bytesRead = in.readTail(new byte[tailLength], 0, tailLength);

      assertThat(bytesRead).isEqualTo(tailLength);
      assertThat(readBytes.value()).isEqualTo(tailLength);
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
