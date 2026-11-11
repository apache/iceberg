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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.io.UncheckedIOException;
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

  private final GCPProperties properties = new GCPProperties();
  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final Random random = new Random(1);

  @Test
  public void testWrite() {
    // Run tests for both byte and array write paths
    Stream.of(true, false)
        .forEach(
            arrayWrite -> {
              // Test small file write
              writeAndVerify(storage, randomBlobId(), randomData(1024), arrayWrite);

              // Test large file
              writeAndVerify(storage, randomBlobId(), randomData(10 * 1024 * 1024), arrayWrite);
            });
  }

  @Test
  public void testWriteWithKmsKeyName() {
    String kmsKeyName = "projects/p/locations/l/keyRings/r/cryptoKeys/k";
    GCPProperties cmekProperties =
        new GCPProperties(ImmutableMap.of(GCPProperties.GCS_KMS_KEY_NAME, kmsKeyName));
    Storage spyStorage = spy(storage);
    BlobId blobId = randomBlobId();

    writeAndVerify(spyStorage, blobId, randomData(1024), true, cmekProperties);

    ArgumentCaptor<BlobWriteOption> options = ArgumentCaptor.forClass(BlobWriteOption.class);
    verify(spyStorage).writer(any(BlobInfo.class), options.capture());
    assertThat(options.getAllValues()).contains(BlobWriteOption.kmsKeyName(kmsKeyName));
  }

  @Test
  public void testMultipleClose() throws IOException {
    GCSOutputStream stream =
        new GCSOutputStream(storage, randomBlobId(), properties, MetricsContext.nullMetrics());
    stream.close();
    stream.close();
  }

  private void writeAndVerify(Storage client, BlobId uri, byte[] data, boolean arrayWrite) {
    writeAndVerify(client, uri, data, arrayWrite, properties);
  }

  private void writeAndVerify(
      Storage client, BlobId uri, byte[] data, boolean arrayWrite, GCPProperties gcpProperties) {
    try (GCSOutputStream stream =
        new GCSOutputStream(client, uri, gcpProperties, MetricsContext.nullMetrics())) {
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
