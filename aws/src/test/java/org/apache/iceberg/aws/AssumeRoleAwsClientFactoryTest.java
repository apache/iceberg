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

import static org.apache.iceberg.aws.AwsProperties.CLIENT_ASSUME_ROLE_ARN;
import static org.apache.iceberg.aws.AwsProperties.CLIENT_ASSUME_ROLE_REGION;
import static org.apache.iceberg.aws.AwsProperties.CLIENT_ASSUME_ROLE_STS_REGIONAL_ENDPOINT_ENABLED;
import static org.apache.iceberg.aws.AwsProperties.CLIENT_FACTORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_REGION;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.StsServiceClientConfiguration;

public class AssumeRoleAwsClientFactoryTest {

  private static final String CURRENT_DEFAULT_REGION = System.getProperty(AWS_REGION.property());
  private static final Pattern STS_HOST =
      Pattern.compile("sts\\.(?<region>[^/]*)\\.amazonaws\\.com");

  @BeforeAll
  public static void init() {
    if (Objects.isNull(CURRENT_DEFAULT_REGION)) {
      System.setProperty(AWS_REGION.property(), "us-west-2");
    }
  }

  @AfterAll
  public static void cleanup() {
    if (Objects.isNull(CURRENT_DEFAULT_REGION)) {
      System.clearProperty(AWS_REGION.property());
    }
  }

  @ParameterizedTest
  @ArgumentsSource(RegionArgumentsProvider.class)
  public void testStsRegionalEndpoint(Region region) {
    Map<String, String> properties =
        ImmutableMap.of(
            CLIENT_ASSUME_ROLE_REGION,
            region.id(),
            CLIENT_ASSUME_ROLE_ARN,
            "arn:aws:iam::12345:root",
            CLIENT_FACTORY,
            AssumeRoleAwsClientFactory.class.getName(),
            CLIENT_ASSUME_ROLE_STS_REGIONAL_ENDPOINT_ENABLED,
            "true");
    AwsClientFactory awsClientFactory = AwsClientFactories.from(properties);

    assertThat(awsClientFactory).isInstanceOf(AssumeRoleAwsClientFactory.class);
    AssumeRoleAwsClientFactory assumeRoleAwsClientFactory =
        (AssumeRoleAwsClientFactory) awsClientFactory;
    StsClient stsClient = assumeRoleAwsClientFactory.stsClientBuilder().build();
    StsServiceClientConfiguration config = stsClient.serviceClientConfiguration();
    assertThat(config.region()).isEqualTo(region);
  }

  @Test
  public void testStsRegionalEndpointWithInterceptor() {
    String region = "us-east-2";
    Map<String, String> properties =
        ImmutableMap.of(
            CLIENT_ASSUME_ROLE_REGION,
            region,
            CLIENT_ASSUME_ROLE_ARN,
            "arn:aws:iam::12345:root",
            CLIENT_FACTORY,
            AssumeRoleAwsClientFactory.class.getName(),
            CLIENT_ASSUME_ROLE_STS_REGIONAL_ENDPOINT_ENABLED,
            "true");
    AwsClientFactory awsClientFactory = AwsClientFactories.from(properties);

    assertThat(awsClientFactory).isInstanceOf(AssumeRoleAwsClientFactory.class);
    AssumeRoleAwsClientFactory assumeRoleAwsClientFactory =
        (AssumeRoleAwsClientFactory) awsClientFactory;
    StsClientBuilder clientBuilder = assumeRoleAwsClientFactory.stsClientBuilder();
    AtomicReference<String> stsHost = new AtomicReference<>();

    StsClient stsClient = buildWithCustomInterceptor(clientBuilder, stsHost);

    catchThrowable(stsClient::getCallerIdentity);
    String stsEndpoint = stsHost.get();
    assertThat(stsEndpoint).contains(region);
    Matcher matcher = STS_HOST.matcher(stsEndpoint);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(1)).isEqualTo(region);
  }

  @Test
  public void testStsRegionalEndpointDisabledWithInterceptor() {
    String region = "us-east-2";
    Map<String, String> properties =
        ImmutableMap.of(
            CLIENT_ASSUME_ROLE_REGION,
            region,
            CLIENT_ASSUME_ROLE_ARN,
            "arn:aws:iam::12345:root",
            CLIENT_FACTORY,
            AssumeRoleAwsClientFactory.class.getName(),
            CLIENT_ASSUME_ROLE_STS_REGIONAL_ENDPOINT_ENABLED,
            "false");
    AwsClientFactory awsClientFactory = AwsClientFactories.from(properties);

    assertThat(awsClientFactory).isInstanceOf(AssumeRoleAwsClientFactory.class);
    AssumeRoleAwsClientFactory assumeRoleAwsClientFactory =
        (AssumeRoleAwsClientFactory) awsClientFactory;
    StsClientBuilder clientBuilder = assumeRoleAwsClientFactory.stsClientBuilder();
    AtomicReference<String> stsHost = new AtomicReference<>();

    StsClient stsClient = buildWithCustomInterceptor(clientBuilder, stsHost);

    catchThrowable(stsClient::getCallerIdentity);
    String stsEndpoint = stsHost.get();
    assertThat(stsEndpoint).doesNotContain(region);
    Matcher matcher = STS_HOST.matcher(stsEndpoint);
    assertThat(matcher.find()).isTrue();
    assertThat(Region.regions()).contains(Region.of(matcher.group(1)));
  }

  private StsClient buildWithCustomInterceptor(
      StsClientBuilder clientBuilder, AtomicReference<String> stsHost) {
    // using static credential provider to ensure region is not looked up in CI env
    return clientBuilder
        .overrideConfiguration(
            c ->
                c.addExecutionInterceptor(
                    new ExecutionInterceptor() {
                      @Override
                      public void beforeTransmission(
                          Context.BeforeTransmission context,
                          ExecutionAttributes executionAttributes) {
                        stsHost.set(
                            executionAttributes
                                .getAttribute(SdkExecutionAttribute.CLIENT_ENDPOINT)
                                .getHost());
                      }
                    }))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test1", "test2")))
        .build();
  }

  private static class RegionArgumentsProvider implements ArgumentsProvider {
    private final List<Region> regions = Region.regions();
    private final Random random = new Random(regions.size());

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(
          Arguments.of(regions.get(Math.abs(random.nextInt() % regions.size()))),
          Arguments.of(regions.get(Math.abs(random.nextInt() % regions.size()))),
          Arguments.of(regions.get(Math.abs(random.nextInt() % regions.size()))));
    }
  }
}
