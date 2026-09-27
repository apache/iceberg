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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.metrics.CachingMetricsContext;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
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
    s3InputStream = new S3InputStream(s3Client, mock());
  }

  @Test
  void testReadFullyClosesTheStream() throws IOException {
    when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
        .thenReturn(inputStream);

    s3InputStream.readFully(0, new byte[0]);

    verify(inputStream).close();
  }

  @Test
  void testReadTailClosesTheStream() throws IOException {
    when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
        .thenReturn(inputStream);

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
  void testZeroLengthReadFullyDoesNotCountMetrics() throws IOException {
    when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
        .thenReturn(new ByteArrayInputStream(new byte[0]));

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    try (S3InputStream in =
        new S3InputStream(s3Client, mock(), new S3FileIOProperties(), metrics)) {
      in.readFully(0, new byte[0], 0, 0);

      // a zero-length readFully performs no real read; it must count neither bytes nor an operation
      assertThat(readBytes.value()).isEqualTo(0);
      assertThat(readOperations.value()).isEqualTo(0);
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

  @Test
  void testReadTailEmptyObjectDoesNotCountMetrics() throws IOException {
    when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
        .thenReturn(new ByteArrayInputStream(new byte[0]));

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    try (S3InputStream in =
        new S3InputStream(s3Client, mock(), new S3FileIOProperties(), metrics)) {
      int bytesRead = in.readTail(new byte[8], 0, 8);

      // an empty object yields no bytes; a no-data read counts neither bytes nor an operation
      assertThat(bytesRead).isEqualTo(0);
      assertThat(readBytes.value()).isEqualTo(0);
      assertThat(readOperations.value()).isEqualTo(0);
    }
  }
}
