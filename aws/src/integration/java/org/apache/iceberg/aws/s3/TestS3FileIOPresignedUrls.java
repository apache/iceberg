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
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

/**
 * Integration test verifying that {@link S3FileIO} reads objects from MinIO via a catalog-vended
 * pre-signed URL used directly as the file's location, with no S3 client ever invoked: objects are
 * written with a credentialed S3 client and presigned via {@link S3Presigner}, then read back
 * through an {@link S3FileIO} whose S3 client would fail if used, proving reads go through the
 * HTTP(S) short-circuit instead of the native, credentialed path.
 */
@Testcontainers
public class TestS3FileIOPresignedUrls {

  @Container private static final MinIOContainer MINIO = MinioUtil.createContainer();

  private static final String BUCKET = "presigned-test";
  private static final Random RANDOM = new Random(42);

  private S3Client s3;
  private S3Presigner presigner;
  private S3FileIO s3FileIO;

  @BeforeEach
  void before() {
    s3 = MinioUtil.createS3Client(MINIO);
    presigner = buildPresigner(MINIO);

    // The native S3 client throws if used, proving reads are served via the pre-signed URL alone.
    s3FileIO =
        new S3FileIO(
            () -> {
              throw new AssertionError(
                  "Native S3 client should not be used for an HTTP(S) file-path");
            });
    s3FileIO.initialize(ImmutableMap.of());

    createBucket(BUCKET);
  }

  @AfterEach
  void after() {
    s3FileIO.close();
  }

  @Test
  void readFullyViaPresignedUrl() throws IOException {
    int dataSize = 256 * 1024; // 256 KB — smaller than one chunk
    byte[] expected = randomData(dataSize);
    String key = "read-fully.bin";

    putObject(BUCKET, key, expected);
    String url = presignGetUrl(BUCKET, key);

    InputFile inputFile = s3FileIO.newInputFile(url, expected.length);

    byte[] actual = new byte[dataSize];
    try (SeekableInputStream stream = inputFile.newStream()) {
      ((RangeReadable) stream).readFully(0, actual, 0, dataSize);
    }

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void readTailViaPresignedUrl() throws IOException {
    byte[] expected = randomData(4096);
    String key = "read-tail.bin";
    int tailSize = 128;

    putObject(BUCKET, key, expected);
    String url = presignGetUrl(BUCKET, key);

    InputFile inputFile = s3FileIO.newInputFile(url, expected.length);

    byte[] tailBuffer = new byte[tailSize];
    int read;
    try (SeekableInputStream stream = inputFile.newStream()) {
      read = ((RangeReadable) stream).readTail(tailBuffer, 0, tailSize);
    }

    assertThat(read).isEqualTo(tailSize);
    assertThat(tailBuffer)
        .isEqualTo(Arrays.copyOfRange(expected, expected.length - tailSize, expected.length));
  }

  @Test
  void seekAndReadViaPresignedUrl() throws IOException {
    int dataSize = 1024;
    byte[] expected = randomData(dataSize);
    String key = "seek-read.bin";

    putObject(BUCKET, key, expected);
    String url = presignGetUrl(BUCKET, key);

    InputFile inputFile = s3FileIO.newInputFile(url, expected.length);

    int offset = 200;
    int length = 300;
    byte[] actual = new byte[length];
    try (SeekableInputStream stream = inputFile.newStream()) {
      stream.seek(offset);
      IOUtil.readFully(stream, actual, 0, length);
    }

    assertThat(actual).isEqualTo(Arrays.copyOfRange(expected, offset, offset + length));
  }

  @Test
  void getLengthViaPresignedUrl() {
    byte[] data = randomData(512);
    String key = "length.bin";

    putObject(BUCKET, key, data);
    String url = presignGetUrl(BUCKET, key);

    // No length hint: HTTPInputFile probes with a Range: bytes=0-0 GET and reads the total size
    // from the Content-Range header.
    InputFile inputFile = s3FileIO.newInputFile(url);

    assertThat(inputFile.getLength()).isEqualTo(data.length);
  }

  @Test
  void multiChunkReadViaPresignedUrl() throws IOException {
    // 5 MB exceeds the 4 MB HTTPInputStream chunk size, forcing multiple chunk fetches.
    int dataSize = 5 * 1024 * 1024;
    byte[] expected = randomData(dataSize);
    String key = "multi-chunk.bin";

    putObject(BUCKET, key, expected);
    String url = presignGetUrl(BUCKET, key);

    InputFile inputFile = s3FileIO.newInputFile(url, expected.length);

    byte[] actual = new byte[dataSize];
    try (SeekableInputStream stream = inputFile.newStream()) {
      IOUtil.readFully(stream, actual, 0, dataSize);
    }

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void nativeS3ReadForS3Location() throws IOException {
    byte[] expected = randomData(1024);
    String key = "native-fallback.bin";
    String location = location(key);

    putObject(BUCKET, key, expected);

    S3FileIO nativeFileIO = new S3FileIO(() -> s3);
    nativeFileIO.initialize(ImmutableMap.of());
    try {
      byte[] actual = new byte[expected.length];
      try (SeekableInputStream stream = nativeFileIO.newInputFile(location).newStream()) {
        IOUtil.readFully(stream, actual, 0, expected.length);
      }

      assertThat(actual).isEqualTo(expected);
    } finally {
      nativeFileIO.close();
    }
  }

  // ---- helpers ----

  private static String location(String key) {
    return "s3://" + BUCKET + "/" + key;
  }

  private static S3Presigner buildPresigner(MinIOContainer container) {
    URI endpoint = URI.create(container.getS3URL());
    return S3Presigner.builder()
        .endpointOverride(endpoint)
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(container.getUserName(), container.getPassword())))
        .build();
  }

  private String presignGetUrl(String bucket, String key) {
    PresignedGetObjectRequest presigned =
        presigner.presignGetObject(
            GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofHours(1))
                .getObjectRequest(r -> r.bucket(bucket).key(key))
                .build());
    return presigned.url().toString();
  }

  private void putObject(String bucket, String key, byte[] data) {
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .contentLength((long) data.length)
            .build(),
        RequestBody.fromBytes(data));
  }

  private void createBucket(String bucketName) {
    try {
      s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException e) {
      // already exists
    }
  }

  private static byte[] randomData(int size) {
    byte[] data = new byte[size];
    RANDOM.nextBytes(data);
    return data;
  }
}
