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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo.ObjectCustomContextPayload;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestGCSOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(TestGCSOutputStream.class);
  private static final String BUCKET = "test-bucket";

  private final GCPProperties properties = new GCPProperties();
  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final Random random = new Random(1);

  @Test
  public void testWrite() {
    Map<String, String> blankContexts = ImmutableMap.of();
    Map<String, String> multipleContexts =
        ImmutableMap.of(
            "env", "prod",
            "analytics-id", "123456");

    Stream.of(true, false)
        .forEach(
            arrayWrite -> {
              // Test small file write with no contexts
              writeAndVerify(storage, randomBlobId(), randomData(1024), arrayWrite, blankContexts);

              // Test large file write with no contexts
              writeAndVerify(
                  storage, randomBlobId(), randomData(10 * 1024 * 1024), arrayWrite, blankContexts);

              // Test small file write with context
              writeAndVerify(
                  storage, randomBlobId(), randomData(1024), arrayWrite, multipleContexts);

              // Test large file write with context
              writeAndVerify(
                  storage,
                  randomBlobId(),
                  randomData(10 * 1024 * 1024),
                  arrayWrite,
                  multipleContexts);
            });
  }

  @Test
  public void testMultipleClose() throws IOException {
    GCSOutputStream stream =
        new GCSOutputStream(storage, randomBlobId(), properties, MetricsContext.nullMetrics());
    stream.close();
    stream.close();
  }

  private void writeAndVerify(
      Storage client, BlobId uri, byte[] data, boolean arrayWrite, Map<String, String> contexts) {
    try (GCSOutputStream stream =
        new GCSOutputStream(
            client, uri, propertiesWithContexts(contexts), MetricsContext.nullMetrics())) {
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
    verifyContexts(uri, contexts);
  }

  /** Builds a {@link GCPProperties} with the given context entries added under the write prefix. */
  private GCPProperties propertiesWithContexts(Map<String, String> contexts) {
    if (contexts == null || contexts.isEmpty()) {
      return properties;
    }

    Map<String, String> props =
        contexts.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> GCPProperties.GCS_WRITE_OBJECT_CONTEXT_PREFIX + e.getKey(),
                    Map.Entry::getValue));
    return new GCPProperties(props);
  }

  /** Asserts that every entry in {@code contexts} is present on the committed GCS object. */
  private void verifyContexts(BlobId blobId, Map<String, String> contexts) {
    if (contexts == null || contexts.isEmpty()) {
      return;
    }

    Map<String, ObjectCustomContextPayload> customContexts =
        storage.get(blobId).asBlobInfo().getContexts().getCustom();

    assertThat(customContexts).as("GCS object should have custom contexts attached").isNotNull();

    Map<String, String> actualContextValues =
        customContexts.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().getValue() // Extract the string value from the payload
                    ));

    assertThat(actualContextValues)
        .as("GCS object metadata should contain the configured contexts")
        .containsAllEntriesOf(contexts);
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
