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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Testcontainers
public class TestMinioUtil {
  @Container private static final MinIOContainer MINIO = MinioUtil.createContainer();

  @Test
  void validateS3ConditionalWrites() {
    S3Client s3Client = MinioUtil.createS3Client(MINIO);

    String bucket = "test-bucket-" + UUID.randomUUID();

    CreateBucketResponse createBucketResponse =
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
    assertThat(createBucketResponse.sdkHttpResponse().isSuccessful()).isTrue();

    String key = "test-key-" + UUID.randomUUID().toString();
    for (int i = 0; i < 5; i++) {
      String payload = "test-payload-" + i;
      PutObjectRequest request =
          PutObjectRequest.builder().bucket(bucket).key(key).ifNoneMatch("*").build();
      RequestBody body = RequestBody.fromString(payload);
      if (i == 0) {
        PutObjectResponse response = s3Client.putObject(request, body);
        assertThat(response.sdkHttpResponse().isSuccessful()).isTrue();
      } else {
        assertThatThrownBy(() -> s3Client.putObject(request, body))
            .isInstanceOf(S3Exception.class)
            .hasMessageContaining("Service: S3, Status Code: 412")
            .hasMessageContaining("At least one of the pre-conditions you specified did not hold");
      }
    }

    var getResponse =
        s3Client.getObject(
            request -> request.bucket(bucket).key(key), ResponseTransformer.toBytes());
    String responseBody = getResponse.asUtf8String();
    assertThat(responseBody).isEqualTo("test-payload-0");
  }

  @Test
  void validateS3ConditionalWritesUsingAsyncClient() {
    S3AsyncClient s3AsyncClient = MinioUtil.createS3AsyncClient(MINIO);

    String bucket = "test-bucket-" + UUID.randomUUID();

    CreateBucketResponse createBucketResponse =
        s3AsyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).join();
    assertThat(createBucketResponse.sdkHttpResponse().isSuccessful()).isTrue();

    String key = "test-key-" + UUID.randomUUID().toString();
    for (int i = 0; i < 5; i++) {
      String payload = "test-payload-" + i;
      PutObjectRequest request =
          PutObjectRequest.builder().bucket(bucket).key(key).ifNoneMatch("*").build();
      AsyncRequestBody body = AsyncRequestBody.fromString(payload);
      if (i == 0) {
        PutObjectResponse response = s3AsyncClient.putObject(request, body).join();
        assertThat(response.sdkHttpResponse().isSuccessful()).isTrue();
      } else {
        assertThatThrownBy(() -> s3AsyncClient.putObject(request, body).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(S3Exception.class)
            .hasMessageContaining("Service: S3, Status Code: 412")
            .hasMessageContaining("At least one of the pre-conditions you specified did not hold");
      }
    }

    String responseBody =
        s3AsyncClient
            .getObject(
                request -> request.bucket(bucket).key(key), AsyncResponseTransformer.toBytes())
            .join()
            .asUtf8String();
    assertThat(responseBody).isEqualTo("test-payload-0");
  }
}
