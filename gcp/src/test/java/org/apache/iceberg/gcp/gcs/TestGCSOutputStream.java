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

import static org.apache.iceberg.gcp.GCPProperties.GCS_ENCRYPTION_KEY;
import static org.apache.iceberg.gcp.GCPProperties.GCS_USER_PROJECT;
import static org.apache.iceberg.gcp.GCPProperties.GCS_WRITE_THRESHOLD_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestGCSOutputStream {
  private static final String BUCKET = "test-bucket";
  private static final String TEST_ENCRYPTION_KEY =
      Base64.getEncoder().encodeToString(new byte[32]);
  private static final String TEST_USER_PROJECT = "my-billing-project";

  private final GCPProperties properties = new GCPProperties();
  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final Random random = new Random(1);

  @Test
  public void testWrite() {
    Stream.of(true, false)
        .forEach(
            arrayWrite -> {
              // Below default 8 MiB threshold: single-shot create
              writeAndVerify(storage, randomBlobId(), randomData(1024), arrayWrite, properties);

              // At/above default threshold: existing WriteChannel path
              writeAndVerify(
                  storage, randomBlobId(), randomData(8 * 1024 * 1024), arrayWrite, properties);
            });
  }

  @Test
  public void testMultipleClose() throws IOException {
    GCSOutputStream stream =
        new GCSOutputStream(storage, randomBlobId(), properties, MetricsContext.nullMetrics());
    stream.close();
    stream.close();
  }

  @Test
  public void testSingleShotBelowThresholdUsesCreate() throws IOException {
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(BlobTargetOption[].class)))
        .thenReturn(mock(Blob.class));

    GCPProperties props = new GCPProperties(ImmutableMap.of(GCS_WRITE_THRESHOLD_BYTES, "1024"));
    byte[] data = randomData(1023);

    try (GCSOutputStream stream =
        new GCSOutputStream(mockStorage, randomBlobId(), props, MetricsContext.nullMetrics())) {
      stream.write(data);
    }

    ArgumentCaptor<byte[]> content = ArgumentCaptor.forClass(byte[].class);
    verify(mockStorage)
        .create(any(BlobInfo.class), content.capture(), any(BlobTargetOption[].class));
    assertThat(content.getValue()).isEqualTo(data);
    verify(mockStorage, never()).writer(any(BlobInfo.class), any(BlobWriteOption[].class));
  }

  @Test
  public void testAtThresholdUsesWriteChannel() throws IOException {
    Storage mockStorage = mockStorageWithWriteChannel();
    GCPProperties props = new GCPProperties(ImmutableMap.of(GCS_WRITE_THRESHOLD_BYTES, "1024"));
    byte[] data = randomData(1024);

    try (GCSOutputStream stream =
        new GCSOutputStream(mockStorage, randomBlobId(), props, MetricsContext.nullMetrics())) {
      stream.write(data);
    }

    verify(mockStorage).writer(any(BlobInfo.class), any(BlobWriteOption[].class));
    verify(mockStorage, never())
        .create(any(BlobInfo.class), any(byte[].class), any(BlobTargetOption[].class));
  }

  @Test
  public void testWriteCrossingThresholdUsesWriteChannel() throws IOException {
    Storage mockStorage = mockStorageWithWriteChannel();
    GCPProperties props = new GCPProperties(ImmutableMap.of(GCS_WRITE_THRESHOLD_BYTES, "1024"));
    byte[] data = randomData(2048);

    try (GCSOutputStream stream =
        new GCSOutputStream(mockStorage, randomBlobId(), props, MetricsContext.nullMetrics())) {
      stream.write(data);
    }

    verify(mockStorage).writer(any(BlobInfo.class), any(BlobWriteOption[].class));
    verify(mockStorage, never())
        .create(any(BlobInfo.class), any(byte[].class), any(BlobTargetOption[].class));
  }

  @Test
  void singleShotPassesEncryptionAndUserProject() throws IOException {
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(BlobTargetOption[].class)))
        .thenReturn(mock(Blob.class));

    GCPProperties props =
        new GCPProperties(
            ImmutableMap.of(
                GCS_WRITE_THRESHOLD_BYTES,
                "1024",
                GCS_ENCRYPTION_KEY,
                TEST_ENCRYPTION_KEY,
                GCS_USER_PROJECT,
                TEST_USER_PROJECT));

    try (GCSOutputStream stream =
        new GCSOutputStream(mockStorage, randomBlobId(), props, MetricsContext.nullMetrics())) {
      stream.write(randomData(16));
    }

    ArgumentCaptor<BlobTargetOption[]> options = ArgumentCaptor.forClass(BlobTargetOption[].class);
    verify(mockStorage).create(any(BlobInfo.class), any(byte[].class), options.capture());
    assertThat(options.getValue()).hasSize(2);
    verify(mockStorage, never()).writer(any(BlobInfo.class), any(BlobWriteOption[].class));
  }

  @Test
  void writeChannelPassesEncryptionAndUserProject() throws IOException {
    Storage mockStorage = mockStorageWithWriteChannel();
    GCPProperties props =
        new GCPProperties(
            ImmutableMap.of(
                GCS_WRITE_THRESHOLD_BYTES,
                "1024",
                GCS_ENCRYPTION_KEY,
                TEST_ENCRYPTION_KEY,
                GCS_USER_PROJECT,
                TEST_USER_PROJECT));

    try (GCSOutputStream stream =
        new GCSOutputStream(mockStorage, randomBlobId(), props, MetricsContext.nullMetrics())) {
      stream.write(randomData(1024));
    }

    ArgumentCaptor<BlobWriteOption[]> options = ArgumentCaptor.forClass(BlobWriteOption[].class);
    verify(mockStorage).writer(any(BlobInfo.class), options.capture());
    assertThat(options.getValue()).hasSize(2);
    verify(mockStorage, never())
        .create(any(BlobInfo.class), any(byte[].class), any(BlobTargetOption[].class));
  }

  @Test
  public void testZeroThresholdUsesWriteChannel() throws IOException {
    Storage mockStorage = mockStorageWithWriteChannel();
    GCPProperties props = new GCPProperties(ImmutableMap.of(GCS_WRITE_THRESHOLD_BYTES, "0"));
    byte[] data = randomData(16);

    try (GCSOutputStream stream =
        new GCSOutputStream(mockStorage, randomBlobId(), props, MetricsContext.nullMetrics())) {
      stream.write(data);
    }

    verify(mockStorage).writer(any(BlobInfo.class), any(BlobWriteOption[].class));
    verify(mockStorage, never())
        .create(any(BlobInfo.class), any(byte[].class), any(BlobTargetOption[].class));
  }

  private Storage mockStorageWithWriteChannel() throws IOException {
    Storage mockStorage = mock(Storage.class);
    WriteChannel mockChannel = mock(WriteChannel.class);
    when(mockStorage.writer(any(BlobInfo.class), any(BlobWriteOption[].class)))
        .thenReturn(mockChannel);
    when(mockChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buf = invocation.getArgument(0);
              int remaining = buf.remaining();
              buf.position(buf.limit());
              return remaining;
            });
    return mockStorage;
  }

  private void writeAndVerify(
      Storage client, BlobId uri, byte[] data, boolean arrayWrite, GCPProperties props) {
    try (GCSOutputStream stream =
        new GCSOutputStream(client, uri, props, MetricsContext.nullMetrics())) {
      if (arrayWrite) {
        stream.write(data);
        assertThat(stream.getPos()).isEqualTo(data.length);
      } else {
        for (int i = 0; i < data.length; i++) {
          stream.write(data[i]);
          assertThat(stream.getPos()).isEqualTo(i + 1);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    byte[] actual = readGCSData(uri);
    assertThat(actual).isEqualTo(data);
  }

  private byte[] readGCSData(BlobId blobId) {
    return storage.get(blobId).getContent();
  }

  private byte[] randomData(int size) {
    byte[] result = new byte[size];
    random.nextBytes(result);
    return result;
  }

  private BlobId randomBlobId() {
    return BlobId.fromGsUtilUri(String.format("gs://%s/data/%s.dat", BUCKET, UUID.randomUUID()));
  }
}
