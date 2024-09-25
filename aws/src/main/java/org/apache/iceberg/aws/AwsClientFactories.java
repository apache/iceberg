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
package org.apache.iceberg.aws;

import java.net.URI;
import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

public class AwsClientFactories {

  private static final DefaultAwsClientFactory AWS_CLIENT_FACTORY_DEFAULT =
      new DefaultAwsClientFactory();

  private AwsClientFactories() {}

  public static AwsClientFactory defaultFactory() {
    return AWS_CLIENT_FACTORY_DEFAULT;
  }

  public static AwsClientFactory from(Map<String, String> properties) {
    String factoryImpl =
        PropertyUtil.propertyAsString(
            properties, AwsProperties.CLIENT_FACTORY, DefaultAwsClientFactory.class.getName());
    return loadClientFactory(factoryImpl, properties);
  }

  private static AwsClientFactory loadClientFactory(String impl, Map<String, String> properties) {
    DynConstructors.Ctor<AwsClientFactory> ctor;
    try {
      ctor =
          DynConstructors.builder(AwsClientFactory.class)
              .loader(AwsClientFactories.class.getClassLoader())
              .hiddenImpl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize AwsClientFactory, missing no-arg constructor: %s", impl),
          e);
    }

    AwsClientFactory factory;
    try {
      factory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize AwsClientFactory, %s does not implement AwsClientFactory.", impl),
          e);
    }

    factory.initialize(properties);
    return factory;
  }

  static class DefaultAwsClientFactory implements AwsClientFactory {
    private AwsProperties awsProperties;
    private AwsClientProperties awsClientProperties;
    private S3FileIOProperties s3FileIOProperties;
    private HttpClientProperties httpClientProperties;

    DefaultAwsClientFactory() {
      awsProperties = new AwsProperties();
      awsClientProperties = new AwsClientProperties();
      s3FileIOProperties = new S3FileIOProperties();
      httpClientProperties = new HttpClientProperties();
    }

    @Override
    public S3Client s3() {
      return S3Client.builder()
          .applyMutation(awsClientProperties::applyClientRegionConfiguration)
          .applyMutation(httpClientProperties::applyHttpClientConfigurations)
          .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
          .applyMutation(s3FileIOProperties::applyServiceConfigurations)
          .applyMutation(
              b -> s3FileIOProperties.applyCredentialConfigurations(awsClientProperties, b))
          .applyMutation(s3FileIOProperties::applySignerConfiguration)
          .applyMutation(s3FileIOProperties::applyS3AccessGrantsConfigurations)
          .applyMutation(s3FileIOProperties::applyUserAgentConfigurations)
          .applyMutation(s3FileIOProperties::applyRetryConfigurations)
          .build();
    }

    @Override
    public GlueClient glue() {
      return GlueClient.builder()
          .applyMutation(awsClientProperties::applyClientRegionConfiguration)
          .applyMutation(httpClientProperties::applyHttpClientConfigurations)
          .applyMutation(awsProperties::applyGlueEndpointConfigurations)
          .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
          .build();
    }

    @Override
    public KmsClient kms() {
      return KmsClient.builder()
          .applyMutation(awsClientProperties::applyClientRegionConfiguration)
          .applyMutation(httpClientProperties::applyHttpClientConfigurations)
          .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
          .build();
    }

    @Override
    public DynamoDbClient dynamo() {
      return DynamoDbClient.builder()
          .applyMutation(awsClientProperties::applyClientRegionConfiguration)
          .applyMutation(httpClientProperties::applyHttpClientConfigurations)
          .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
          .applyMutation(awsProperties::applyDynamoDbEndpointConfigurations)
          .build();
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.awsProperties = new AwsProperties(properties);
      this.awsClientProperties = new AwsClientProperties(properties);
      this.s3FileIOProperties = new S3FileIOProperties(properties);
      this.httpClientProperties = new HttpClientProperties(properties);
    }
  }

  /**
   * Build a httpClientBuilder object
   *
   * @deprecated Not for public use. To configure the httpClient for a client, please use {@link
   *     HttpClientProperties#applyHttpClientConfigurations(AwsSyncClientBuilder)}. It will be
   *     removed in 2.0.0
   */
  @Deprecated
  public static SdkHttpClient.Builder configureHttpClientBuilder(String httpClientType) {
    String clientType = httpClientType;
    if (Strings.isNullOrEmpty(clientType)) {
      clientType = HttpClientProperties.CLIENT_TYPE_DEFAULT;
    }
    switch (clientType) {
      case HttpClientProperties.CLIENT_TYPE_URLCONNECTION:
        return UrlConnectionHttpClient.builder();
      case HttpClientProperties.CLIENT_TYPE_APACHE:
        return ApacheHttpClient.builder();
      default:
        throw new IllegalArgumentException("Unrecognized HTTP client type " + httpClientType);
    }
  }

  /**
   * Configure the endpoint setting for a client
   *
   * @deprecated Not for public use. To configure the endpoint for a client, please use {@link
   *     S3FileIOProperties#applyEndpointConfigurations(S3ClientBuilder)}, {@link
   *     AwsProperties#applyGlueEndpointConfigurations(GlueClientBuilder)}, or {@link
   *     AwsProperties#applyDynamoDbEndpointConfigurations(DynamoDbClientBuilder)} accordingly. It
   *     will be removed in 2.0.0
   */
  @Deprecated
  public static <T extends SdkClientBuilder> void configureEndpoint(T builder, String endpoint) {
    if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
    }
  }

  /**
   * Build an S3Configuration object
   *
   * @deprecated Not for public use. To build an S3Configuration object, use
   *     S3Configuration.builder() directly. It will be removed in 2.0.0
   */
  @Deprecated
  public static S3Configuration s3Configuration(
      Boolean pathStyleAccess, Boolean s3UseArnRegionEnabled) {
    return S3Configuration.builder()
        .pathStyleAccessEnabled(pathStyleAccess)
        .useArnRegionEnabled(s3UseArnRegionEnabled)
        .build();
  }

  /**
   * Build an AwsBasicCredential object
   *
   * @deprecated Not for public use. To configure the credentials for a s3 client, please use {@link
   *     S3FileIOProperties#applyCredentialConfigurations(AwsClientProperties, S3ClientBuilder)} in
   *     AwsProperties. It will be removed in 2.0.0.
   */
  @Deprecated
  static AwsCredentialsProvider credentialsProvider(
      String accessKeyId, String secretAccessKey, String sessionToken) {
    if (accessKeyId != null) {
      if (sessionToken == null) {
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKeyId, secretAccessKey));
      } else {
        return StaticCredentialsProvider.create(
            AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
      }
    } else {
      // Create a new credential provider for each client
      return DefaultCredentialsProvider.builder().build();
    }
  }
}
