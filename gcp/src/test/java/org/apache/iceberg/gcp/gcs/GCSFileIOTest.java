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

import static java.lang.String.format;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_TOKEN;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GCSFileIOTest {
  private static final String TEST_BUCKET = "TEST_BUCKET";
  private final Random random = new Random(1);

  private final Storage storage = spy(LocalStorageHelper.getOptions().getService());
  private GCSFileIO io;

  @SuppressWarnings("unchecked")
  @BeforeEach
  public void before() {
    // LocalStorageHelper doesn't support batch operations, so mock that here
    doAnswer(
            invoke -> {
              Iterable<BlobId> iter = invoke.getArgument(0);
              List<Boolean> answer = Lists.newArrayList();
              iter.forEach(
                  blobId -> {
                    answer.add(storage.delete(blobId));
                  });
              return answer;
            })
        .when(storage)
        .delete(any(Iterable.class));

    io = new GCSFileIO(() -> storage, new GCPProperties());
  }

  @Test
  public void newInputFile() throws IOException {
    String location = format("gs://%s/path/to/file.txt", TEST_BUCKET);
    byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isFalse();

    OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    assertThat(in.exists()).isTrue();
    byte[] actual = new byte[1024 * 1024];

    try (InputStream is = in.newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }

    assertThat(expected).isEqualTo(actual);

    io.deleteFile(in);

    assertThat(io.newInputFile(location).exists()).isFalse();
  }

  @Test
  public void testDelete() {
    String path = "delete/path/data.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path).build());

    // There should be one blob in the bucket
    assertThat(
            StreamSupport.stream(storage.list(TEST_BUCKET).iterateAll().spliterator(), false)
                .count())
        .isEqualTo(1);

    io.deleteFile(gsUri(path));

    // The bucket should now be empty
    assertThat(
            StreamSupport.stream(storage.list(TEST_BUCKET).iterateAll().spliterator(), false)
                .count())
        .isZero();
  }

  private String gsUri(String path) {
    return format("gs://%s/%s", TEST_BUCKET, path);
  }

  @Test
  public void testListPrefix() {
    String prefix = "list/path/";
    String path1 = prefix + "data1.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path1).build());
    String path2 = prefix + "data2.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path2).build());
    String path3 = "list/skip/data3.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path3).build());

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("list/")).spliterator(), false).count())
        .isEqualTo(3);

    assertThat(StreamSupport.stream(io.listPrefix(gsUri(prefix)).spliterator(), false).count())
        .isEqualTo(2);

    assertThat(StreamSupport.stream(io.listPrefix(gsUri(path1)).spliterator(), false).count())
        .isEqualTo(1);
  }

  @Test
  public void testDeleteFiles() {
    String prefix = "del/path/";
    String path1 = prefix + "data1.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path1).build());
    String path2 = prefix + "data2.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path2).build());
    String path3 = "del/skip/data3.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path3).build());

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("del/")).spliterator(), false).count())
        .isEqualTo(3);

    Iterable<String> deletes =
        () -> ImmutableList.of(gsUri(path1), gsUri(path3)).stream().iterator();
    io.deleteFiles(deletes);

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("del/")).spliterator(), false).count())
        .isEqualTo(1);
  }

  @Test
  public void testDeletePrefix() {
    String prefix = "del/path/";
    String path1 = prefix + "data1.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path1).build());
    String path2 = prefix + "data2.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path2).build());
    String path3 = "del/skip/data3.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path3).build());

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("del/")).spliterator(), false).count())
        .isEqualTo(3);

    io.deletePrefix(gsUri(prefix));

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("del/")).spliterator(), false).count())
        .isEqualTo(1);
  }

  @Test
  public void testGCSFileIOKryoSerialization() throws IOException {
    FileIO testGCSFileIO = new GCSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testGCSFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testGCSFileIO);

    assertThat(testGCSFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }

  @Test
  public void testGCSFileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testGCSFileIO = new GCSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testGCSFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testGCSFileIO);

    assertThat(testGCSFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }

  @Test
  public void testResolvingFileIOLoad() {
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    resolvingFileIO.setConf(new Configuration());
    resolvingFileIO.initialize(ImmutableMap.of());
    FileIO result =
        DynMethods.builder("io")
            .hiddenImpl(ResolvingFileIO.class, String.class)
            .build(resolvingFileIO)
            .invoke("gs://foo/bar");
    assertThat(result).isInstanceOf(GCSFileIO.class);
  }

  @Test
  public void refreshCredentialsEndpointSet() {
    Storage client;
    try (GCSFileIO fileIO = new GCSFileIO()) {
      fileIO.initialize(
          ImmutableMap.of(
              GCS_OAUTH2_TOKEN,
              "gcsToken",
              GCS_OAUTH2_TOKEN_EXPIRES_AT,
              Long.toString(Instant.now().plus(5, ChronoUnit.MINUTES).toEpochMilli()),
              GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
              "/v1/credentials"));
      client = fileIO.client();
    }

    assertThat(client.getOptions().getCredentials())
        .isInstanceOf(OAuth2CredentialsWithRefresh.class);
  }

  @Test
  public void refreshCredentialsEndpointSetButRefreshDisabled() {
    Storage client;
    try (GCSFileIO fileIO = new GCSFileIO()) {
      fileIO.initialize(
          ImmutableMap.of(
              GCS_OAUTH2_TOKEN,
              "gcsTokenWithoutRefresh",
              GCS_OAUTH2_TOKEN_EXPIRES_AT,
              Long.toString(Instant.now().plus(5, ChronoUnit.MINUTES).toEpochMilli()),
              GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
              "/v1/credentials",
              GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED,
              "false"));
      client = fileIO.client();
    }

    assertThat(client.getOptions().getCredentials()).isInstanceOf(OAuth2Credentials.class);
  }

  @Test
  void recoverSoftDeletedObjectWhenSoftDeleteDisabled() {
    BlobId blobId = BlobId.of(TEST_BUCKET, "object");
    Bucket bucket = mock(Bucket.class);
    BucketInfo.SoftDeletePolicy policy = mock(BucketInfo.SoftDeletePolicy.class);

    when(storage.get(TEST_BUCKET)).thenReturn(bucket);
    when(bucket.getSoftDeletePolicy()).thenReturn(policy);
    when(policy.getRetentionDuration()).thenReturn(Duration.ofSeconds(0)); // soft-delete disabled

    assertThat(io.recoverSoftDeletedObject(blobId)).isFalse();
  }

  @Test
  void recoverSoftDeletedObject() {
    BlobId blobId = BlobId.of(TEST_BUCKET, "object");
    Bucket bucket = mock(Bucket.class);
    BucketInfo.SoftDeletePolicy policy = mock(BucketInfo.SoftDeletePolicy.class);
    Page<Blob> page = mock(Page.class);
    Blob softDeletedBlob = mock(Blob.class);

    when(storage.get(TEST_BUCKET)).thenReturn(bucket);
    when(bucket.getSoftDeletePolicy()).thenReturn(policy);
    when(policy.getRetentionDuration()).thenReturn(Duration.ofDays(7)); // soft-delete enabled
    doAnswer(invoke -> page).when(storage).list(eq(TEST_BUCKET), any(), any());
    when(page.streamAll()).thenReturn(Stream.of(softDeletedBlob));
    when(softDeletedBlob.getName()).thenReturn("object");
    when(softDeletedBlob.getSoftDeleteTime()).thenReturn(OffsetDateTime.now());
    when(softDeletedBlob.getBlobId()).thenReturn(blobId);
    doAnswer(invocation -> softDeletedBlob).when(storage).restore(any(), any());

    assertThat(io.recoverSoftDeletedObject(blobId)).isTrue();
  }

  @Test
  void recoverSoftDeletedObjectWhenNoSoftDeletedBlobExistsx() {
    BlobId blobId = BlobId.of(TEST_BUCKET, "object");
    Bucket bucket = mock(Bucket.class);
    BucketInfo.SoftDeletePolicy policy = mock(BucketInfo.SoftDeletePolicy.class);
    Page<Blob> page = mock(Page.class);

    when(storage.get(TEST_BUCKET)).thenReturn(bucket);
    when(bucket.getSoftDeletePolicy()).thenReturn(policy);
    when(policy.getRetentionDuration()).thenReturn(Duration.ofDays(7)); // soft-delete enabled
    when(page.streamAll()).thenReturn(Stream.empty());

    assertThat(io.recoverSoftDeletedObject(blobId)).isFalse();
  }

  @Test
  void recoverSoftDeleteWhenStorageExceptionThrown() {
    BlobId blobId = BlobId.of(TEST_BUCKET, "object");
    Bucket bucket = mock(Bucket.class);
    BucketInfo.SoftDeletePolicy policy = mock(BucketInfo.SoftDeletePolicy.class);

    when(storage.get(TEST_BUCKET)).thenReturn(bucket);
    when(bucket.getSoftDeletePolicy()).thenReturn(policy);
    when(policy.getRetentionDuration()).thenReturn(Duration.ofDays(7)); // soft-delete enabled
    doThrow(StorageException.class).when(storage).list(eq(TEST_BUCKET), any(), any());

    assertThat(io.recoverLatestVersion(blobId)).isFalse();
  }

  @Test
  void recoverLatestVersionWhenVersioningDisabled() {
    BlobId blobId = BlobId.of(TEST_BUCKET, "object");
    Bucket bucket = mock(Bucket.class);

    when(storage.get(TEST_BUCKET)).thenReturn(bucket);
    when(bucket.versioningEnabled()).thenReturn(false);

    assertThat(io.recoverLatestVersion(blobId)).isFalse();
  }

  @Test
  void recoverLatestVersion() {
    BlobId blobId = BlobId.of(TEST_BUCKET, "object");
    Bucket bucket = mock(Bucket.class);
    Blob versionedBlob = mock(Blob.class);
    CopyWriter copyWriter = mock(CopyWriter.class);
    Page<Blob> page = mock(Page.class);

    when(storage.get(TEST_BUCKET)).thenReturn(bucket);
    when(bucket.versioningEnabled()).thenReturn(true);
    when(versionedBlob.getName()).thenReturn("object");
    when(versionedBlob.getUpdateTimeOffsetDateTime()).thenReturn(OffsetDateTime.now());
    when(versionedBlob.getDeleteTimeOffsetDateTime()).thenReturn(OffsetDateTime.now());
    when(versionedBlob.getBlobId()).thenReturn(blobId);
    doAnswer(invoke -> page).when(storage).list(eq(TEST_BUCKET), any(), any());
    when(page.streamAll()).thenReturn(Stream.of(versionedBlob));
    // mock successful copy operation
    doAnswer(invocation -> copyWriter).when(storage).copy(any());
    when(storage.copy(any())).thenReturn(copyWriter);
    when(copyWriter.getResult()).thenReturn(versionedBlob);

    assertThat(io.recoverLatestVersion(blobId)).isTrue();
  }

  @Test
  void recoverLatestVersionWhenNoVersionExists() {
    BlobId blobId = BlobId.of(TEST_BUCKET, "object");
    Bucket bucket = mock(Bucket.class);
    Page<Blob> page = mock(Page.class);

    when(storage.get(TEST_BUCKET)).thenReturn(bucket);
    when(bucket.versioningEnabled()).thenReturn(true);
    doAnswer(invoke -> page).when(storage).list(eq(TEST_BUCKET), any(), any());
    when(page.streamAll()).thenReturn(Stream.empty());

    assertThat(io.recoverLatestVersion(blobId)).isFalse();
  }

  @Test
  void recoverLatestVersionWhenCopyFails() {
    BlobId blobId = BlobId.of(TEST_BUCKET, "object");
    Bucket bucket = mock(Bucket.class);
    Blob versionedBlob = mock(Blob.class);
    Page<Blob> page = mock(Page.class);

    when(storage.get(TEST_BUCKET)).thenReturn(bucket);
    when(bucket.versioningEnabled()).thenReturn(true);
    when(versionedBlob.getName()).thenReturn("object");
    when(versionedBlob.getUpdateTimeOffsetDateTime()).thenReturn(OffsetDateTime.now());
    when(versionedBlob.getBlobId()).thenReturn(blobId);
    doAnswer(invoke -> page).when(storage).list(eq(TEST_BUCKET), any(), any());
    when(page.streamAll()).thenReturn(Stream.of(versionedBlob));
    // mock failed copy operation
    doThrow(StorageException.class).when(storage).copy(any());

    assertThat(io.recoverLatestVersion(blobId)).isFalse();
  }
}
