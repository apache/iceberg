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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestGCSFileIO {
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

    io = new GCSFileIO(() -> storage);
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

    assertThat(actual).isEqualTo(expected);

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

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testGCSFileIOSerialization(
      TestHelpers.RoundTripSerializer<FileIO> roundTripSerializer)
      throws IOException, ClassNotFoundException {

    FileIO testGCSFileIO = new GCSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testGCSFileIO.initialize(ImmutableMap.of("k1", "v1", "k2", "v2"));
    FileIO roundTripSerializedFileIO = roundTripSerializer.apply(testGCSFileIO);

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
              CatalogProperties.URI,
              "http://catalog-endpoint",
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
  public void noStorageCredentialConfigured() {
    try (GCSFileIO fileIO = new GCSFileIO()) {
      fileIO.setCredentials(ImmutableList.of());
      fileIO.initialize(
          ImmutableMap.of(
              GCS_OAUTH2_TOKEN, "gcsTokenFromProperties", GCS_OAUTH2_TOKEN_EXPIRES_AT, "1000"));

      // make sure that the generic Storage Client is used for all storage paths if there are no
      // storage credentials configured
      assertThat(fileIO.client("gs://my-bucket/table1"))
          .isSameAs(fileIO.client("invalidStoragePath"))
          .isSameAs(fileIO.client("gs://random-bucket/"))
          .isSameAs(fileIO.client("gs://random-bucket/tableX"));

      assertThat(fileIO.client().getOptions().getCredentials())
          .isInstanceOf(OAuth2Credentials.class)
          .extracting("value")
          .extracting("temporaryAccess")
          .isEqualTo(new AccessToken("gcsTokenFromProperties", new Date(1000L)));

      assertThat(fileIO.client().getOptions().getCredentials())
          .isInstanceOf(OAuth2Credentials.class)
          .extracting("value")
          .extracting("temporaryAccess")
          .isEqualTo(new AccessToken("gcsTokenFromProperties", new Date(1000L)));
    }
  }

  @Test
  public void singleStorageCredentialConfigured() {
    StorageCredential gcsCredential =
        StorageCredential.create(
            "gs://custom-uri",
            ImmutableMap.of(
                "gcs.oauth2.token",
                "gcsTokenFromCredential",
                "gcs.oauth2.token-expires-at",
                "2000"));

    try (GCSFileIO fileIO = new GCSFileIO()) {
      fileIO.setCredentials(ImmutableList.of(gcsCredential));
      fileIO.initialize(
          ImmutableMap.of(
              GCS_OAUTH2_TOKEN, "gcsTokenFromProperties", GCS_OAUTH2_TOKEN_EXPIRES_AT, "1000"));

      assertThat(fileIO.client("gs://custom-uri/table1"))
          .isNotSameAs(fileIO.client("gs://random-bucket/"))
          .isNotSameAs(fileIO.client("gs://random-bucket/tableX"));

      assertThat(fileIO.client("gs://custom-uri/table1").getOptions().getCredentials())
          .isInstanceOf(OAuth2Credentials.class)
          .extracting("value")
          .extracting("temporaryAccess")
          .isEqualTo(new AccessToken("gcsTokenFromCredential", new Date(2000L)));

      assertThat(fileIO.client().getOptions().getCredentials())
          .isInstanceOf(OAuth2Credentials.class)
          .extracting("value")
          .extracting("temporaryAccess")
          .isEqualTo(new AccessToken("gcsTokenFromProperties", new Date(1000L)));

      // verify that the generic storage client is used for all storage prefixes that don't match
      // the storage credentials
      assertThat(fileIO.client("gs"))
          .isSameAs(fileIO.client("gs://random-bucket/tableX"))
          .isSameAs(fileIO.client("gs://bucketX/tableX"));

      assertThat(fileIO.client("gs://random-bucket/table1").getOptions().getCredentials())
          .isInstanceOf(OAuth2Credentials.class)
          .extracting("value")
          .extracting("temporaryAccess")
          .isEqualTo(new AccessToken("gcsTokenFromProperties", new Date(1000L)));
    }
  }

  @Test
  public void multipleStorageCredentialsConfigured() {
    StorageCredential gcsCredential1 =
        StorageCredential.create(
            "gs://custom-uri/1",
            ImmutableMap.of(
                "gcs.oauth2.token",
                "gcsTokenFromCredential1",
                "gcs.oauth2.token-expires-at",
                "2000"));

    StorageCredential gcsCredential2 =
        StorageCredential.create(
            "gs://custom-uri/2",
            ImmutableMap.of(
                "gcs.oauth2.token",
                "gcsTokenFromCredential2",
                "gcs.oauth2.token-expires-at",
                "3000"));

    try (GCSFileIO fileIO = new GCSFileIO()) {
      fileIO.setCredentials(ImmutableList.of(gcsCredential1, gcsCredential2));
      fileIO.initialize(
          ImmutableMap.of(
              GCS_OAUTH2_TOKEN, "gcsTokenFromProperties", GCS_OAUTH2_TOKEN_EXPIRES_AT, "1000"));

      assertThat(fileIO.client("gs://custom-uri/table1"))
          .isNotSameAs(fileIO.client("gs://custom-uri/1/table1"))
          .isNotSameAs(fileIO.client("gs://custom-uri/2/table1"));

      assertThat(fileIO.client("gs://custom-uri/1/table1").getOptions().getCredentials())
          .isInstanceOf(OAuth2Credentials.class)
          .extracting("value")
          .extracting("temporaryAccess")
          .isEqualTo(new AccessToken("gcsTokenFromCredential1", new Date(2000L)));

      assertThat(fileIO.client("gs://custom-uri/2/table1").getOptions().getCredentials())
          .isInstanceOf(OAuth2Credentials.class)
          .extracting("value")
          .extracting("temporaryAccess")
          .isEqualTo(new AccessToken("gcsTokenFromCredential2", new Date(3000L)));

      assertThat(fileIO.client("gs://custom-uri/table1").getOptions().getCredentials())
          .isInstanceOf(OAuth2Credentials.class)
          .extracting("value")
          .extracting("temporaryAccess")
          .isEqualTo(new AccessToken("gcsTokenFromProperties", new Date(1000L)));

      assertThat(fileIO.client().getOptions().getCredentials())
          .isInstanceOf(OAuth2Credentials.class)
          .extracting("value")
          .extracting("temporaryAccess")
          .isEqualTo(new AccessToken("gcsTokenFromProperties", new Date(1000L)));
    }
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void fileIOWithStorageCredentialsSerialization(
      TestHelpers.RoundTripSerializer<GCSFileIO> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    GCSFileIO fileIO = new GCSFileIO();
    fileIO.setCredentials(
        ImmutableList.of(
            StorageCredential.create("prefix", Map.of("key1", "val1", "key2", "val2"))));
    fileIO.initialize(Map.of());

    assertThat(roundTripSerializer.apply(fileIO).credentials()).isEqualTo(fileIO.credentials());
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void fileIOWithPrefixedStorageClientWithoutCredentialsSerialization(
      TestHelpers.RoundTripSerializer<GCSFileIO> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    GCSFileIO fileIO = new GCSFileIO();
    fileIO.initialize(
        Map.of(GCS_OAUTH2_TOKEN, "gcsTokenFromProperties", GCS_OAUTH2_TOKEN_EXPIRES_AT, "1000"));

    assertThat(fileIO.client("gs")).isInstanceOf(Storage.class);
    assertThat(fileIO.client("gs://bucket1/my-path/tableX")).isInstanceOf(Storage.class);
    assertThat(fileIO.client("gs://bucket1/my-path/tableX").getOptions().getCredentials())
        .isInstanceOf(OAuth2Credentials.class)
        .extracting("value")
        .extracting("temporaryAccess")
        .isEqualTo(new AccessToken("gcsTokenFromProperties", new Date(1000L)));

    GCSFileIO roundTripIO = roundTripSerializer.apply(fileIO);
    assertThat(roundTripIO).isNotNull();
    assertThat(roundTripIO.credentials()).isEqualTo(fileIO.credentials()).isEmpty();

    assertThat(roundTripIO.client("gs")).isInstanceOf(Storage.class);
    assertThat(roundTripIO.client("gs://bucket1/my-path/tableX")).isInstanceOf(Storage.class);
    assertThat(roundTripIO.client("gs://bucket1/my-path/tableX").getOptions().getCredentials())
        .isInstanceOf(OAuth2Credentials.class)
        .extracting("value")
        .extracting("temporaryAccess")
        .isEqualTo(new AccessToken("gcsTokenFromProperties", new Date(1000L)));
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void fileIOWithPrefixedStorageClientSerialization(
      TestHelpers.RoundTripSerializer<GCSFileIO> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    GCSFileIO fileIO = new GCSFileIO();
    fileIO.setCredentials(
        ImmutableList.of(
            StorageCredential.create(
                "gs://bucket1",
                ImmutableMap.of(
                    "gcs.oauth2.token",
                    "gcsTokenFromCredential",
                    "gcs.oauth2.token-expires-at",
                    "2000"))));
    fileIO.initialize(
        Map.of(GCS_OAUTH2_TOKEN, "gcsTokenFromProperties", GCS_OAUTH2_TOKEN_EXPIRES_AT, "1000"));

    assertThat(fileIO.client("gs")).isInstanceOf(Storage.class);
    assertThat(fileIO.client("gs://bucket1/my-path/tableX")).isInstanceOf(Storage.class);
    assertThat(fileIO.client("gs://bucket1/my-path/tableX").getOptions().getCredentials())
        .isInstanceOf(OAuth2Credentials.class)
        .extracting("value")
        .extracting("temporaryAccess")
        .isEqualTo(new AccessToken("gcsTokenFromCredential", new Date(2000L)));

    GCSFileIO roundTripIO = roundTripSerializer.apply(fileIO);
    assertThat(roundTripIO).isNotNull();
    assertThat(roundTripIO.credentials()).isEqualTo(fileIO.credentials());

    assertThat(roundTripIO.client("gs")).isInstanceOf(Storage.class);
    assertThat(roundTripIO.client("gs://bucket1/my-path/tableX")).isInstanceOf(Storage.class);
    assertThat(roundTripIO.client("gs://bucket1/my-path/tableX").getOptions().getCredentials())
        .isInstanceOf(OAuth2Credentials.class)
        .extracting("value")
        .extracting("temporaryAccess")
        .isEqualTo(new AccessToken("gcsTokenFromCredential", new Date(2000L)));
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void resolvingFileIOLoadWithoutStorageCredentials(
      TestHelpers.RoundTripSerializer<ResolvingFileIO> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    resolvingFileIO.initialize(ImmutableMap.of());

    ResolvingFileIO fileIO = roundTripSerializer.apply(resolvingFileIO);
    assertThat(fileIO.credentials()).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void resolvingFileIOLoadWithStorageCredentials(
      TestHelpers.RoundTripSerializer<ResolvingFileIO> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    StorageCredential credential = StorageCredential.create("prefix", Map.of("key1", "val1"));
    List<StorageCredential> storageCredentials = ImmutableList.of(credential);
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    resolvingFileIO.setCredentials(storageCredentials);
    resolvingFileIO.initialize(ImmutableMap.of());

    FileIO result =
        DynMethods.builder("io")
            .hiddenImpl(ResolvingFileIO.class, String.class)
            .build(resolvingFileIO)
            .invoke("gs://foo/bar");
    assertThat(result)
        .isInstanceOf(GCSFileIO.class)
        .asInstanceOf(InstanceOfAssertFactories.type(GCSFileIO.class))
        .extracting(GCSFileIO::credentials)
        .isEqualTo(storageCredentials);

    // make sure credentials are still present after serde
    ResolvingFileIO fileIO = roundTripSerializer.apply(resolvingFileIO);
    assertThat(fileIO.credentials()).isEqualTo(storageCredentials);
    result =
        DynMethods.builder("io")
            .hiddenImpl(ResolvingFileIO.class, String.class)
            .build(fileIO)
            .invoke("gs://foo/bar");
    assertThat(result)
        .isInstanceOf(GCSFileIO.class)
        .asInstanceOf(InstanceOfAssertFactories.type(GCSFileIO.class))
        .extracting(GCSFileIO::credentials)
        .isEqualTo(storageCredentials);
  }
}
