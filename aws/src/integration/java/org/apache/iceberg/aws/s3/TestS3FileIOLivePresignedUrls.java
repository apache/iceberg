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

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsIntegTestUtil;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

/**
 * Live-AWS verification that pre-signed URL reads work end-to-end against real S3: put an object,
 * mint a GET pre-signed URL with {@link S3Presigner}, and read it through {@link
 * S3FileIO#newInputFile(String)} using the URL unchanged as the location, so bytes are fetched over
 * HTTPS without the native S3 client.
 *
 * <p>Requires:
 *
 * <ul>
 *   <li>{@value AwsIntegTestUtil#AWS_ACCESS_KEY_ID}
 *   <li>{@value AwsIntegTestUtil#AWS_SECRET_ACCESS_KEY}
 *   <li>{@value AwsIntegTestUtil#AWS_SESSION_TOKEN} (when using temporary credentials)
 *   <li>{@value AwsIntegTestUtil#AWS_REGION}
 *   <li>{@value AwsIntegTestUtil#AWS_TEST_BUCKET}
 * </ul>
 */
@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_ACCESS_KEY_ID, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_SECRET_ACCESS_KEY, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_REGION, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_TEST_BUCKET, matches = ".*")
})
public class TestS3FileIOLivePresignedUrls {

  private static final Random RANDOM = new Random(42);

  private static S3Client s3;
  private static S3Presigner presigner;
  private static String bucketName;
  private static String prefix;

  private S3FileIO s3FileIO;

  @BeforeAll
  public static void beforeClass() {
    s3 = AwsClientFactories.defaultFactory().s3();
    bucketName = AwsIntegTestUtil.testBucketName();
    prefix = "presigned-url-live/" + UUID.randomUUID();
    presigner =
        S3Presigner.builder()
            .region(Region.of(AwsIntegTestUtil.testRegion()))
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .build();
  }

  @AfterAll
  public static void afterClass() {
    AwsIntegTestUtil.cleanS3GeneralPurposeBucket(s3, bucketName, prefix);
    if (presigner != null) {
      presigner.close();
    }
  }

  @BeforeEach
  void before() {
    // Native client throws if used: proves the read path is the pre-signed HTTPS URL alone.
    s3FileIO =
        new S3FileIO(
            () -> {
              throw new AssertionError(
                  "Native S3 client should not be used for an HTTP(S) file-path");
            });
    s3FileIO.initialize(ImmutableMap.of());
  }

  @AfterEach
  void after() {
    if (s3FileIO != null) {
      s3FileIO.close();
    }
  }

  @Test
  void readFullyViaPresignedUrlOnS3FileIO() throws IOException {
    byte[] expected = randomData(256 * 1024);
    String key = objectKey("read-fully.bin");
    putObject(key, expected);

    String url = presignGetUrl(key);
    InputFile inputFile = s3FileIO.newInputFile(url, expected.length);
    assertThat(inputFile.location()).isEqualTo(url);

    byte[] actual = new byte[expected.length];
    try (SeekableInputStream stream = inputFile.newStream()) {
      ((RangeReadable) stream).readFully(0, actual, 0, expected.length);
    }

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void seekAndGetLengthViaPresignedUrlOnS3FileIO() throws IOException {
    byte[] expected = randomData(1024);
    String key = objectKey("seek-length.bin");
    putObject(key, expected);

    String url = presignGetUrl(key);
    assertThat(s3FileIO.newInputFile(url).getLength()).isEqualTo(expected.length);

    int offset = 200;
    int length = 300;
    byte[] actual = new byte[length];
    try (SeekableInputStream stream = s3FileIO.newInputFile(url, expected.length).newStream()) {
      stream.seek(offset);
      IOUtil.readFully(stream, actual, 0, length);
    }

    assertThat(actual).isEqualTo(Arrays.copyOfRange(expected, offset, offset + length));
  }

  @Test
  void multiChunkReadViaPresignedUrlOnS3FileIO() throws IOException {
    // 10 MB exceeds the 8 MB HttpInputStream chunk size, forcing multiple chunk fetches.
    byte[] expected = randomData(10 * 1024 * 1024);
    String key = objectKey("multi-chunk.bin");
    putObject(key, expected);

    String url = presignGetUrl(key);

    byte[] actual = new byte[expected.length];
    try (SeekableInputStream stream = s3FileIO.newInputFile(url, expected.length).newStream()) {
      IOUtil.readFully(stream, actual, 0, expected.length);
    }

    assertThat(actual).isEqualTo(expected);
  }

  // ---- helpers ----

  private static String objectKey(String name) {
    return prefix + "/" + name;
  }

  private static String presignGetUrl(String key) {
    PresignedGetObjectRequest presigned =
        presigner.presignGetObject(
            GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofHours(1))
                .getObjectRequest(r -> r.bucket(bucketName).key(key))
                .build());
    return presigned.url().toString();
  }

  private static void putObject(String key, byte[] data) {
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(bucketName)
            .key(key)
            .contentLength((long) data.length)
            .build(),
        RequestBody.fromBytes(data));
  }

  private static byte[] randomData(int size) {
    byte[] data = new byte[size];
    RANDOM.nextBytes(data);
    return data;
  }
}
