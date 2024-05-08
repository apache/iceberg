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
import java.io.InputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.IoUtils;

/** Specialized test cases that validate {@link S3FileIO} against the Minio S3 implementation. */
public class TestS3FileIOMinio {
  private static final Region REGION = Region.US_WEST_2;
  private static final String BUCKET = "iceberg-s3-signer-test";
  static final AwsCredentialsProvider CREDENTIALS_PROVIDER =
      StaticCredentialsProvider.create(
          AwsBasicCredentials.create("accessKeyId", "secretAccessKey"));
  private static final MinioContainer MINIO_CONTAINER =
      new MinioContainer(CREDENTIALS_PROVIDER.resolveCredentials());

  private static S3Client s3;
  private S3FileIO s3FileIO;

  @BeforeAll
  static void start() {
    MINIO_CONTAINER.start();

    s3 =
        S3Client.builder()
            .region(REGION)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .applyMutation(
                s3ClientBuilder ->
                    s3ClientBuilder.httpClientBuilder(
                        software.amazon.awssdk.http.apache.ApacheHttpClient.builder()))
            .endpointOverride(MINIO_CONTAINER.getURI())
            .forcePathStyle(true) // OSX won't resolve subdomains
            .build();

    s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
  }

  @AfterAll
  static void stop() {
    MINIO_CONTAINER.close();
  }

  @BeforeEach
  public void setupFileIO() {
    s3FileIO = new S3FileIO(() -> s3);
  }

  @ParameterizedTest
  @ValueSource(strings = {"test", "te_st", "te st", "te~!@$%^&*()-+st", "te#st", "te?st"})
  void testReadFileWithSpecialChars(String dir) throws IOException {
    String key = dir + "/test_file";
    // Make sure the characters in the key are supported by the S3 client and server.
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromString("test"));
    InputStream in = s3.getObject(GetObjectRequest.builder().bucket(BUCKET).key(key).build());
    assertThat(IoUtils.toUtf8String(in)).isEqualTo("test");

    // Note: Iceberg produces similar S3 location strings when a table is partitioned
    // by a column with special characters in its name. Also, similar URIs are produced
    // by the AWS S3 UI.
    InputFile inputFile = s3FileIO.newInputFile("s3://" + BUCKET + "/" + key);
    SeekableInputStream in2 = inputFile.newStream();
    assertThat(IoUtils.toUtf8String(in2)).isEqualTo("test");
  }
}
