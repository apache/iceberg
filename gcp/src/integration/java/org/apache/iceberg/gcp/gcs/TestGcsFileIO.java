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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public abstract class TestGcsFileIO {

  protected static final Random RANDOM = new Random(1);
  private static final String TEST_PREFIX = "integration-test";

  protected Storage storage;
  protected String bucket;
  private String prefix;
  private GCSFileIO fileIO;

  /** Subclasses assign {@link #storage} and {@link #bucket} here. */
  protected abstract void init() throws IOException;

  /** Backend connection properties for the analytics-core reader, e.g. host and auth. */
  protected Map<String, String> properties() {
    return ImmutableMap.of();
  }

  private Map<String, String> analyticsCoreProperties() {
    return ImmutableMap.<String, String>builder()
        .putAll(properties())
        .put(GCPProperties.GCS_ANALYTICS_CORE_ENABLED, "true")
        .buildKeepingLast();
  }

  private String location(String name) {
    return String.format("gs://%s/%s/%s", bucket, prefix, name);
  }

  @BeforeEach
  public void before() throws IOException {
    init();
    assertThat(storage).as("init() must set storage").isNotNull();
    assertThat(bucket).as("init() must set bucket").isNotNull();
    prefix = TEST_PREFIX + "/" + UUID.randomUUID();
    fileIO = new GCSFileIO(() -> storage);
    fileIO.initialize(ImmutableMap.of());
  }

  @AfterEach
  public void after() {
    for (Blob blob :
        storage.list(bucket, Storage.BlobListOption.prefix(prefix + "/")).iterateAll()) {
      storage.delete(blob.getBlobId());
    }
  }

  @Test
  public void newInputFileGcsAnalyticsCoreDisabled() throws IOException {
    String location = location("path/to/file.txt");
    byte[] expected = new byte[1024 * 1024];
    RANDOM.nextBytes(expected);
    storage.create(BlobInfo.newBuilder(BlobId.fromGsUtilUri(location)).build(), expected);
    InputFile in = fileIO.newInputFile(location);
    byte[] actual = new byte[1024 * 1024];

    try (InputStream is = in.newStream()) {
      IOUtil.readFully(is, actual, 0, expected.length);
    }

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void newInputFileGcsAnalyticsCoreEnabled() throws IOException {
    String location = location("path/to/file.txt");
    byte[] expected = new byte[1024 * 1024];
    RANDOM.nextBytes(expected);
    storage.create(BlobInfo.newBuilder(BlobId.fromGsUtilUri(location)).build(), expected);
    fileIO.initialize(analyticsCoreProperties());
    InputFile in = fileIO.newInputFile(location);
    byte[] actual = new byte[1024 * 1024];

    InputStream inputStream = in.newStream();
    try (InputStream is = inputStream) {
      IOUtil.readFully(is, actual, 0, expected.length);
    }

    assertThat(inputStream).isNotInstanceOf(GCSInputStream.class);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void deleteFiles() {
    List<String> locations = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      String location = location("delete-files/file-" + i + ".txt");
      locations.add(location);
      storage.create(BlobInfo.newBuilder(BlobId.fromGsUtilUri(location)).build(), new byte[] {1});
    }

    fileIO.deleteFiles(locations);

    for (String location : locations) {
      assertThat(fileIO.newInputFile(location).exists()).isFalse();
    }
  }

  @Test
  public void deleteFilesIgnoresMissingObjects() {
    List<String> existing = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      String location = location("delete-missing/file-" + i + ".txt");
      existing.add(location);
      storage.create(BlobInfo.newBuilder(BlobId.fromGsUtilUri(location)).build(), new byte[] {1});
    }

    List<String> toDelete = Lists.newArrayList(existing);
    toDelete.add(location("delete-missing/missing-1.txt"));
    toDelete.add(location("delete-missing/missing-2.txt"));

    // A mix of present and already-gone objects (HTTP 404) must delete cleanly, without a failure.
    assertThatCode(() -> fileIO.deleteFiles(toDelete)).doesNotThrowAnyException();

    for (String location : existing) {
      assertThat(fileIO.newInputFile(location).exists()).isFalse();
    }
  }

  @Test
  public void deleteFilesIsIdempotentOnRetry() {
    List<String> locations = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      String location = location("delete-retry/file-" + i + ".txt");
      locations.add(location);
      storage.create(BlobInfo.newBuilder(BlobId.fromGsUtilUri(location)).build(), new byte[] {1});
    }

    fileIO.deleteFiles(locations);
    for (String location : locations) {
      assertThat(fileIO.newInputFile(location).exists()).isFalse();
    }

    // Re-issuing the deletes, as a retried cleanup would, must not raise
    // BulkDeletionFailureException.
    assertThatCode(() -> fileIO.deleteFiles(locations)).doesNotThrowAnyException();
  }

  @Test
  public void listPrefix() {
    String dir1 = location("list-prefix/d1");
    String dir2 = location("list-prefix/d2");
    storage.create(
        BlobInfo.newBuilder(BlobId.fromGsUtilUri(dir1 + "/f1.txt")).build(), new byte[] {1});
    storage.create(
        BlobInfo.newBuilder(BlobId.fromGsUtilUri(dir1 + "/f2.txt")).build(), new byte[] {1});
    storage.create(
        BlobInfo.newBuilder(BlobId.fromGsUtilUri(dir2 + "/f3.txt")).build(), new byte[] {1});

    List<FileInfo> files = Lists.newArrayList(fileIO.listPrefix(location("list-prefix/")));
    List<String> paths = files.stream().map(FileInfo::location).collect(Collectors.toList());

    assertThat(files).hasSize(3);
    assertThat(paths).contains(dir1 + "/f1.txt", dir1 + "/f2.txt", dir2 + "/f3.txt");
  }

  @Test
  public void deletePrefix() {
    String prefixToDelete = location("delete-prefix/");
    storage.create(
        BlobInfo.newBuilder(BlobId.fromGsUtilUri(prefixToDelete + "f1.txt")).build(),
        new byte[] {1});
    storage.create(
        BlobInfo.newBuilder(BlobId.fromGsUtilUri(prefixToDelete + "f2.txt")).build(),
        new byte[] {1});

    int filesBeforeDelete = Lists.newArrayList(fileIO.listPrefix(prefixToDelete)).size();

    fileIO.deletePrefix(prefixToDelete);

    assertThat(filesBeforeDelete).isEqualTo(2);
    assertThat(Lists.newArrayList(fileIO.listPrefix(prefixToDelete))).isEmpty();
  }

  @Test
  public void readMissingLocation() throws IOException {
    String location = location("path/to/data.parquet");
    InputFile input = fileIO.newInputFile(location);

    // Creating an input stream or changing the read position in it are local operations
    try (SeekableInputStream in = input.newStream()) {
      in.seek(1);
    }

    try (SeekableInputStream in = input.newStream()) {
      assertThatThrownBy(in::read)
          .isInstanceOf(NotFoundException.class)
          .hasCauseInstanceOf(IOException.class)
          .hasMessage("Location does not exist: " + location);
    }
  }

  @Disabled(
      "Regression in gcs-analytics-core, "
          + "https://github.com/GoogleCloudPlatform/gcs-analytics-core/pull/371 fixes it, "
          + "enable once fixed version available")
  @Test
  public void readMissingLocationGcsAnalyticsCoreEnabled() throws IOException {
    String location = location("path/to/data.parquet");
    fileIO.initialize(analyticsCoreProperties());
    InputFile in = fileIO.newInputFile(location);

    assertThatThrownBy(() -> in.newStream().read())
        .isInstanceOf(NotFoundException.class)
        .hasCauseInstanceOf(IOException.class)
        .hasMessage("Location does not exist: " + location);
  }

  @Test
  public void deleteFile() {
    String location = location("path/to/file.txt");
    storage.create(BlobInfo.newBuilder(BlobId.fromGsUtilUri(location)).build(), new byte[] {1});
    InputFile in = fileIO.newInputFile(location);
    assertThat(in.exists()).as("File should exist before delete").isTrue();

    fileIO.deleteFile(in);

    assertThat(fileIO.newInputFile(location).exists()).isFalse();
  }
}
