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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

class TestS3FileIOClientFactory {

  private static final AtomicInteger INITIALIZED = new AtomicInteger();
  private static final AtomicInteger S3_CREATED = new AtomicInteger();

  @BeforeEach
  void resetCounts() {
    INITIALIZED.set(0);
    S3_CREATED.set(0);
  }

  private static Map<String, String> properties(Class<?> factoryClass) {
    String factoryProperty =
        AwsClientFactory.class.isAssignableFrom(factoryClass)
            ? AwsProperties.CLIENT_FACTORY
            : S3FileIOProperties.CLIENT_FACTORY;
    return ImmutableMap.of(factoryProperty, factoryClass.getName());
  }

  @ParameterizedTest
  @ValueSource(classes = {CountingS3FileIOClientFactory.class, CountingAwsClientFactory.class})
  void rootClientsUseFactoryFromInitialize(Class<?> factoryClass) {
    try (S3FileIO fileIO = new S3FileIO()) {
      fileIO.initialize(properties(factoryClass));
      assertThat(fileIO.client()).isNotNull();
      assertThat(fileIO.asyncClient()).isNotNull();
      assertThat(INITIALIZED).hasValue(1);
    }
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  void deserializedRootClientsUseFactoryFromInitialize(
      TestHelpers.RoundTripSerializer<S3FileIO> roundTripSerializer) throws Exception {
    try (S3FileIO fileIO = new S3FileIO()) {
      fileIO.initialize(properties(CountingAwsClientFactory.class));
      try (S3FileIO deserialized = roundTripSerializer.apply(fileIO)) {
        assertThat(deserialized.client()).isNotNull();
        assertThat(deserialized.asyncClient()).isNotNull();
      }
    }

    assertThat(INITIALIZED).hasValue(1);
  }

  @Test
  void storageCredentialPrefixUsesOneFactoryForBothClients() {
    String prefix = "s3://bucket/path";
    try (S3FileIO fileIO = new S3FileIO()) {
      fileIO.setCredentials(
          ImmutableList.of(
              StorageCredential.create(
                  prefix,
                  ImmutableMap.of(
                      S3FileIOProperties.ACCESS_KEY_ID,
                      "accessKey",
                      S3FileIOProperties.SECRET_ACCESS_KEY,
                      "secretKey"))));
      fileIO.initialize(properties(CountingAwsClientFactory.class));
      assertThat(fileIO.client(prefix + "/file")).isNotNull();
      assertThat(fileIO.asyncClient(prefix + "/file")).isNotNull();
      assertThat(INITIALIZED).hasValue(2);
    }
  }

  @Test
  void preloadCreatesRootClientFromFactoryFromInitialize() {
    Map<String, String> preloadProperties =
        ImmutableMap.<String, String>builder()
            .putAll(properties(CountingAwsClientFactory.class))
            .put(S3FileIOProperties.PRELOAD_CLIENT_ENABLED, "true")
            .build();
    try (S3FileIO fileIO = new S3FileIO()) {
      fileIO.initialize(preloadProperties);
      assertThat(fileIO.asyncClient()).isNotNull();
      assertThat(S3_CREATED).hasValue(1);
      assertThat(INITIALIZED).hasValue(1);
    }
  }

  public static class CountingS3FileIOClientFactory implements S3FileIOAwsClientFactory {
    @Override
    public S3Client s3() {
      S3_CREATED.incrementAndGet();
      return Mockito.mock(S3Client.class);
    }

    @Override
    public S3AsyncClient s3Async() {
      return Mockito.mock(S3AsyncClient.class);
    }

    @Override
    public void initialize(Map<String, String> properties) {
      INITIALIZED.incrementAndGet();
    }
  }

  public static class CountingAwsClientFactory implements AwsClientFactory {
    @Override
    public S3Client s3() {
      S3_CREATED.incrementAndGet();
      return Mockito.mock(S3Client.class);
    }

    @Override
    public S3AsyncClient s3Async() {
      return Mockito.mock(S3AsyncClient.class);
    }

    @Override
    public GlueClient glue() {
      return Mockito.mock(GlueClient.class);
    }

    @Override
    public KmsClient kms() {
      return Mockito.mock(KmsClient.class);
    }

    @Override
    public DynamoDbClient dynamo() {
      return Mockito.mock(DynamoDbClient.class);
    }

    @Override
    public void initialize(Map<String, String> properties) {
      INITIALIZED.incrementAndGet();
    }
  }
}
