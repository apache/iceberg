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
package org.apache.iceberg.aws.lambda.rest;

import java.util.Map;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.common.DynConstructors;
import software.amazon.awssdk.services.lambda.LambdaClient;

public class LambdaRESTInvokerAwsClientFactories {

  private LambdaRESTInvokerAwsClientFactories() {}

  public static LambdaRESTInvokerAwsClientFactory from(Map<String, String> properties) {
    String factoryImpl = properties.get(LambdaRESTInvokerProperties.CLIENT_FACTORY);
    if (factoryImpl == null) {
      LambdaRESTInvokerAwsClientFactory factory = new DefaultLambdaRESTInvokerAwsClientFactory();
      factory.initialize(properties);
      return factory;
    }

    DynConstructors.Ctor<LambdaRESTInvokerAwsClientFactory> ctor;
    try {
      ctor =
          DynConstructors.builder(LambdaRESTInvokerAwsClientFactory.class)
              .loader(LambdaRESTInvokerAwsClientFactories.class.getClassLoader())
              .hiddenImpl(factoryImpl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize LambdaRESTInvokerAwsClientFactory, missing no-arg constructor: %s",
              factoryImpl),
          e);
    }

    LambdaRESTInvokerAwsClientFactory factory;
    try {
      factory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot construct instance of: %s, does not implement LambdaHttpClientAwsClientFactory.",
              factoryImpl),
          e);
    }

    factory.initialize(properties);
    return factory;
  }

  static class DefaultLambdaRESTInvokerAwsClientFactory
      implements LambdaRESTInvokerAwsClientFactory {

    private HttpClientProperties httpClientProperties;
    private AwsClientProperties awsClientProperties;

    DefaultLambdaRESTInvokerAwsClientFactory() {}

    @Override
    public LambdaClient lambda() {
      return LambdaClient.builder()
          .applyMutation(httpClientProperties::applyHttpClientConfigurations)
          .applyMutation(awsClientProperties::applyClientRegionConfiguration)
          .build();
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.httpClientProperties = new HttpClientProperties(properties);
      this.awsClientProperties = new AwsClientProperties(properties);
    }
  }
}
