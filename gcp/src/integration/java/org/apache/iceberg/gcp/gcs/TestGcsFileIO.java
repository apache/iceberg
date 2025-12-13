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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports.Binding;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class TestGcsFileIO {

  private static final String BUCKET = "test-bucket";
  private static final String PROJECT_ID = "test-project";
  private static final int GCS_EMULATOR_PORT = 4443;
  private static final Random RANDOM = new Random(1);

  @Container
  private static final GenericContainer<?> GCS_EMULATOR =
      new GenericContainer<>("fsouza/fake-gcs-server:latest")
          .withExposedPorts(GCS_EMULATOR_PORT)
          .withCreateContainerCmdModifier(
              cmd ->
                  cmd.withHostConfig(
                      new HostConfig()
                          .withPortBindings(
                              new PortBinding(
                                  Binding.bindPort(GCS_EMULATOR_PORT),
                                  new ExposedPort(GCS_EMULATOR_PORT)))))
          .withCommand(
              "-scheme",
              "http",
              "-external-url",
              String.format("http://localhost:%d", GCS_EMULATOR_PORT))
          .waitingFor(
              new HttpWaitStrategy()
                  .forPort(GCS_EMULATOR_PORT)
                  .forPath("/storage/v1/b")
                  .forStatusCode(200)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  private GCSFileIO fileIO;
  private static Storage storage;

  @BeforeAll
  public static void beforeClass() {
    String endpoint = String.format("http://localhost:%d", GCS_EMULATOR_PORT);
    StorageOptions options =
        StorageOptions.newBuilder()
            .setProjectId(PROJECT_ID)
            .setHost(endpoint)
            .setCredentials(NoCredentials.getInstance())
            .build();
    storage = options.getService();
    storage.create(BucketInfo.of(BUCKET));
  }

  @AfterAll
  public static void afterClass() {
    if (storage != null) {
      storage.delete(BUCKET);
    }
  }

  @BeforeEach
  public void before() {
    fileIO = new GCSFileIO(() -> storage);
    fileIO.initialize(ImmutableMap.of());
    for (Blob blob : storage.list(BUCKET).iterateAll()) {
      storage.delete(blob.getBlobId());
    }
  }

  @AfterEach
  public void after() {
    for (Blob blob : storage.list(BUCKET).iterateAll()) {
      storage.delete(blob.getBlobId());
    }
  }

  @Test
  public void newInputFileGcsAnalyticsCoreDisabled() throws IOException {
    String location = String.format("gs://%s/path/to/file.txt", BUCKET);
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
    String location = String.format("gs://%s/path/to/file.txt", BUCKET);
    byte[] expected = new byte[1024 * 1024];
    RANDOM.nextBytes(expected);
    storage.create(BlobInfo.newBuilder(BlobId.fromGsUtilUri(location)).build(), expected);
    fileIO.initialize(
        ImmutableMap.of(
            GCPProperties.GCS_ANALYTICS_CORE_ENABLED, "true",
            GCPProperties.GCS_NO_AUTH, "true",
            GCPProperties.GCS_SERVICE_HOST,
                String.format("http://localhost:%d", GCS_EMULATOR_PORT)));
    InputFile in = fileIO.newInputFile(location);
    byte[] actual = new byte[1024 * 1024];

    InputStream inputStream = in.newStream();
    try (InputStream is = inputStream) {
      IOUtil.readFully(is, actual, 0, expected.length);
    }

    assertThat(inputStream).isInstanceOf(GcsInputStreamWrapper.class);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void deleteFiles() {
    String prefix = "delete-files";
    List<String> locations = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      String location = String.format("gs://%s/%s/file-%d.txt", BUCKET, prefix, i);
      locations.add(location);
      storage.create(BlobInfo.newBuilder(BlobId.fromGsUtilUri(location)).build(), new byte[] {1});
    }

    fileIO.deleteFiles(locations);

    for (String location : locations) {
      assertThat(fileIO.newInputFile(location).exists()).isFalse();
    }
  }

  @Test
  public void listPrefix() {
    String prefix = "list-prefix";
    String dir1 = String.format("gs://%s/%s/d1", BUCKET, prefix);
    String dir2 = String.format("gs://%s/%s/d2", BUCKET, prefix);
    storage.create(
        BlobInfo.newBuilder(BlobId.fromGsUtilUri(dir1 + "/f1.txt")).build(), new byte[] {1});
    storage.create(
        BlobInfo.newBuilder(BlobId.fromGsUtilUri(dir1 + "/f2.txt")).build(), new byte[] {1});
    storage.create(
        BlobInfo.newBuilder(BlobId.fromGsUtilUri(dir2 + "/f3.txt")).build(), new byte[] {1});

    List<FileInfo> files =
        Lists.newArrayList(fileIO.listPrefix(String.format("gs://%s/%s/", BUCKET, prefix)));
    List<String> paths = files.stream().map(FileInfo::location).collect(Collectors.toList());

    assertThat(files).hasSize(3);
    assertThat(paths).contains(dir1 + "/f1.txt", dir1 + "/f2.txt", dir2 + "/f3.txt");
  }

  @Test
  public void deletePrefix() {
    String prefixToDelete = String.format("gs://%s/delete-prefix/", BUCKET);
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
  public void readMissingLocation() {
    String location = String.format("gs://%s/path/to/data.parquet", BUCKET);
    InputFile in = fileIO.newInputFile(location);

    assertThatThrownBy(() -> in.newStream().read())
        .isInstanceOf(IOException.class)
        .hasCauseInstanceOf(StorageException.class)
        .hasMessageContaining("404 Not Found");
  }

  @Test
  public void deleteFile() {
    String location = String.format("gs://%s/path/to/file.txt", BUCKET);
    storage.create(BlobInfo.newBuilder(BlobId.fromGsUtilUri(location)).build(), new byte[] {1});
    InputFile in = fileIO.newInputFile(location);
    assertThat(in.exists()).as("File should exist before delete").isTrue();

    fileIO.deleteFile(in);

    assertThat(fileIO.newInputFile(location).exists()).isFalse();
  }
}
