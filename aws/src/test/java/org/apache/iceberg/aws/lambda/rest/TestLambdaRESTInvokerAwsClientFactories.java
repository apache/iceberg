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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.lambda.LambdaClient;

public class TestLambdaRESTInvokerAwsClientFactories {

  @Test
  public void testLoadDefaultFactory() {
    LambdaRESTInvokerAwsClientFactory factory =
        LambdaRESTInvokerAwsClientFactories.from(ImmutableMap.of());
    Assertions.assertThat(factory)
        .isInstanceOf(
            LambdaRESTInvokerAwsClientFactories.DefaultLambdaRESTInvokerAwsClientFactory.class);
  }

  @Test
  public void testLoadCustomFactory() {
    LambdaRESTInvokerAwsClientFactory factory =
        LambdaRESTInvokerAwsClientFactories.from(
            ImmutableMap.of(
                LambdaRESTInvokerProperties.CLIENT_FACTORY, CustomFactory.class.getName()));
    Assertions.assertThat(factory).isInstanceOf(CustomFactory.class);
    Assertions.assertThat(factory.lambda()).isNull();
  }

  @Test
  public void testLoadCustomFactoryBadConstructor() {
    Assertions.assertThatThrownBy(
            () ->
                LambdaRESTInvokerAwsClientFactories.from(
                    ImmutableMap.of(
                        LambdaRESTInvokerProperties.CLIENT_FACTORY,
                        BadConstructorFactory.class.getName())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot initialize LambdaRESTInvokerAwsClientFactory, missing no-arg constructor: "
                + BadConstructorFactory.class.getName());
  }

  @Test
  public void testLoadCustomFactoryNotSubclass() {
    Assertions.assertThatThrownBy(
            () ->
                LambdaRESTInvokerAwsClientFactories.from(
                    ImmutableMap.of(
                        LambdaRESTInvokerProperties.CLIENT_FACTORY,
                        NotSubclassFactory.class.getName())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot construct instance of: "
                + NotSubclassFactory.class.getName()
                + ", does not implement LambdaHttpClientAwsClientFactory.");
  }

  public static class CustomFactory implements LambdaRESTInvokerAwsClientFactory {
    @Override
    public LambdaClient lambda() {
      return null;
    }

    @Override
    public void initialize(Map<String, String> properties) {}
  }

  public static class BadConstructorFactory implements LambdaRESTInvokerAwsClientFactory {

    private final String arg;

    public BadConstructorFactory(String arg) {
      this.arg = arg;
    }

    @Override
    public LambdaClient lambda() {
      return null;
    }

    @Override
    public void initialize(Map<String, String> properties) {}
  }

  public static class NotSubclassFactory {}
}
