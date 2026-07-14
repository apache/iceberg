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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.iceberg.metrics.MetricsContext;
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
}
