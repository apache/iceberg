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

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestGcsInputStreamWrapper {

  @Mock private GoogleCloudStorageInputStream googleCloudStorageInputStream;

  private GcsInputStreamWrapper inputStreamWrapper;

  @BeforeEach
  public void before() {
    inputStreamWrapper =
        new GcsInputStreamWrapper(googleCloudStorageInputStream, MetricsContext.nullMetrics());
  }

  @Test
  public void getPos() throws IOException {
    inputStreamWrapper.getPos();

    Mockito.verify(googleCloudStorageInputStream).getPos();
  }

  @Test
  public void seek() throws IOException {
    long newPos = 1234L;
    inputStreamWrapper.seek(newPos);

    Mockito.verify(googleCloudStorageInputStream).seek(newPos);
  }

  @Test
  public void read() throws IOException {
    inputStreamWrapper.read();

    Mockito.verify(googleCloudStorageInputStream).read();
  }

  @Test
  public void readByteArray() throws IOException {
    byte[] buffer = new byte[1024];

    inputStreamWrapper.read(buffer);

    Mockito.verify(googleCloudStorageInputStream).read(buffer, 0, buffer.length);
  }

  @Test
  public void readByteArrayWithOffset() throws IOException {
    byte[] buffer = new byte[1024];
    int off = 10;
    int len = 100;

    inputStreamWrapper.read(buffer, off, len);

    Mockito.verify(googleCloudStorageInputStream).read(buffer, off, len);
  }

  @Test
  public void readFully() throws IOException {
    long position = 123L;
    byte[] buffer = new byte[1024];
    int offset = 10;
    int length = 100;

    inputStreamWrapper.readFully(position, buffer, offset, length);

    Mockito.verify(googleCloudStorageInputStream).readFully(position, buffer, offset, length);
  }

  @Test
  public void readTail() throws IOException {
    byte[] buffer = new byte[1024];
    int offset = 10;
    int length = 100;

    inputStreamWrapper.readTail(buffer, offset, length);

    Mockito.verify(googleCloudStorageInputStream).readTail(buffer, offset, length);
  }

  @Test
  public void readVectored() throws IOException {
    CompletableFuture<ByteBuffer> future1 = new CompletableFuture<>();
    CompletableFuture<ByteBuffer> future2 = new CompletableFuture<>();
    List<FileRange> ranges =
        List.of(new FileRange(future1, 10L, 100), new FileRange(future2, 0, 50));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    inputStreamWrapper.readVectored(ranges, allocate);
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

    Mockito.verify(googleCloudStorageInputStream).readVectored(objectRanges, allocate);
  }

  @Test
  public void close() throws IOException {
    inputStreamWrapper.close();

    Mockito.verify(googleCloudStorageInputStream).close();
  }
}
