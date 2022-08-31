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
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
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

    private String glueEndpoint;
    private String s3Endpoint;
    private String s3AccessKeyId;
    private String s3SecretAccessKey;
    private String s3SessionToken;
    private Boolean s3PathStyleAccess;
    private Boolean s3UseArnRegionEnabled;
    private Boolean s3AccelerationEnabled;
    private String dynamoDbEndpoint;
    private String httpClientType;
    private Boolean s3DualStackEnabled;
    private AwsProperties awsProperties;

    DefaultAwsClientFactory() {}

    @Override
    public S3Client s3() {
      return S3Client.builder()
          .httpClientBuilder(configureHttpClientBuilder(httpClientType))
          .applyMutation(builder -> configureEndpoint(builder, s3Endpoint))
          .applyMutation(builder -> awsProperties.applyS3Configuration(builder))
          .credentialsProvider(
              credentialsProvider(s3AccessKeyId, s3SecretAccessKey, s3SessionToken))
          .build();
    }

    @Override
    public GlueClient glue() {
      return GlueClient.builder()
          .httpClientBuilder(configureHttpClientBuilder(httpClientType))
          .applyMutation(builder -> configureEndpoint(builder, glueEndpoint))
          .build();
    }

    @Override
    public KmsClient kms() {
      return KmsClient.builder()
          .httpClientBuilder(configureHttpClientBuilder(httpClientType))
          .build();
    }

    @Override
    public DynamoDbClient dynamo() {
      return DynamoDbClient.builder()
          .httpClientBuilder(configureHttpClientBuilder(httpClientType))
          .applyMutation(builder -> configureEndpoint(builder, dynamoDbEndpoint))
          .build();
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.awsProperties = new AwsProperties(properties);
      this.glueEndpoint = properties.get(AwsProperties.GLUE_CATALOG_ENDPOINT);
      this.s3Endpoint = properties.get(AwsProperties.S3FILEIO_ENDPOINT);
      this.s3AccessKeyId = properties.get(AwsProperties.S3FILEIO_ACCESS_KEY_ID);
      this.s3SecretAccessKey = properties.get(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY);
      this.s3SessionToken = properties.get(AwsProperties.S3FILEIO_SESSION_TOKEN);
      this.s3PathStyleAccess =
          PropertyUtil.propertyAsBoolean(
              properties,
              AwsProperties.S3FILEIO_PATH_STYLE_ACCESS,
              AwsProperties.S3FILEIO_PATH_STYLE_ACCESS_DEFAULT);
      this.s3UseArnRegionEnabled =
          PropertyUtil.propertyAsBoolean(
              properties,
              AwsProperties.S3_USE_ARN_REGION_ENABLED,
              AwsProperties.S3_USE_ARN_REGION_ENABLED_DEFAULT);
      this.s3AccelerationEnabled =
          PropertyUtil.propertyAsBoolean(
              properties,
              AwsProperties.S3_ACCELERATION_ENABLED,
              AwsProperties.S3_ACCELERATION_ENABLED_DEFAULT);
      this.s3DualStackEnabled =
          PropertyUtil.propertyAsBoolean(
              properties,
              AwsProperties.S3_DUALSTACK_ENABLED,
              AwsProperties.S3_DUALSTACK_ENABLED_DEFAULT);

      ValidationException.check(
          (s3AccessKeyId == null) == (s3SecretAccessKey == null),
          "S3 client access key ID and secret access key must be set at the same time");
      this.dynamoDbEndpoint = properties.get(AwsProperties.DYNAMODB_ENDPOINT);
      this.httpClientType =
          PropertyUtil.propertyAsString(
              properties, AwsProperties.HTTP_CLIENT_TYPE, AwsProperties.HTTP_CLIENT_TYPE_DEFAULT);
    }
  }

  public static SdkHttpClient.Builder configureHttpClientBuilder(String httpClientType) {
    String clientType = httpClientType;
    if (Strings.isNullOrEmpty(clientType)) {
      clientType = AwsProperties.HTTP_CLIENT_TYPE_DEFAULT;
    }
    switch (clientType) {
      case AwsProperties.HTTP_CLIENT_TYPE_URLCONNECTION:
        return UrlConnectionHttpClient.builder();
      case AwsProperties.HTTP_CLIENT_TYPE_APACHE:
        return ApacheHttpClient.builder();
      default:
        throw new IllegalArgumentException("Unrecognized HTTP client type " + httpClientType);
    }
  }

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
      return DefaultCredentialsProvider.create();
    }
  }
}
