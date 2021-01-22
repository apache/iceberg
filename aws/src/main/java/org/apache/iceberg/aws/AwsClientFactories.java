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

import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

public class AwsClientFactories {

  private static final SdkHttpClient HTTP_CLIENT_DEFAULT = UrlConnectionHttpClient.create();
  private static final DefaultAwsClientFactory AWS_CLIENT_FACTORY_DEFAULT = new DefaultAwsClientFactory();

  private AwsClientFactories() {
  }

  public static AwsClientFactory defaultFactory() {
    return AWS_CLIENT_FACTORY_DEFAULT;
  }

  public static AwsClientFactory from(Map<String, String> properties) {
    if (properties.containsKey(AwsProperties.CLIENT_FACTORY)) {
      return loadClientFactory(properties.get(AwsProperties.CLIENT_FACTORY), properties);
    } else {
      return defaultFactory();
    }
  }

  private static AwsClientFactory loadClientFactory(String impl, Map<String, String> properties) {
    DynConstructors.Ctor<AwsClientFactory> ctor;
    try {
      ctor = DynConstructors.builder(AwsClientFactory.class).impl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(String.format(
          "Cannot initialize AwsClientFactory, missing no-arg constructor: %s", impl), e);
    }

    AwsClientFactory factory;
    try {
      factory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize AwsClientFactory, %s does not implement AwsClientFactory.", impl), e);
    }

    factory.initialize(properties);
    return factory;
  }

  static class DefaultAwsClientFactory implements AwsClientFactory {

    DefaultAwsClientFactory() {
    }

    @Override
    public S3Client s3() {
      return S3Client.builder().httpClient(HTTP_CLIENT_DEFAULT).build();
    }

    @Override
    public GlueClient glue() {
      return GlueClient.builder().httpClient(HTTP_CLIENT_DEFAULT).build();
    }

    @Override
    public KmsClient kms() {
      return KmsClient.builder().httpClient(HTTP_CLIENT_DEFAULT).build();
    }

    @Override
    public DynamoDbClient dynamo() {
      return DynamoDbClient.builder().httpClient(HTTP_CLIENT_DEFAULT).build();
    }

    @Override
    public void initialize(Map<String, String> properties) {
    }
  }
}
