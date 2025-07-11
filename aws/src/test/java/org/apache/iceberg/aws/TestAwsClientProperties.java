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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.VendedCredentialsProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class TestAwsClientProperties {

  @Test
  public void testApplyClientRegion() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsClientProperties.CLIENT_REGION, "us-east-1");
    AwsClientProperties awsClientProperties = new AwsClientProperties(properties);

    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<Region> regionArgumentCaptor = ArgumentCaptor.forClass(Region.class);

    awsClientProperties.applyClientRegionConfiguration(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).region(regionArgumentCaptor.capture());
    Region region = regionArgumentCaptor.getValue();
    assertThat(region.id())
        .as("region parameter should match what is set in CLIENT_REGION")
        .isEqualTo("us-east-1");
  }

  @Test
  public void testDefaultCredentialsConfiguration() {
    AwsClientProperties awsClientProperties = new AwsClientProperties();
    AwsCredentialsProvider credentialsProvider =
        awsClientProperties.credentialsProvider(null, null, null);

    assertThat(credentialsProvider)
        .as("Should use default credentials if nothing is set")
        .isInstanceOf(DefaultCredentialsProvider.class);
  }

  @Test
  public void testCreatesNewInstanceOfDefaultCredentialsConfiguration() {
    AwsClientProperties awsClientProperties = new AwsClientProperties();
    AwsCredentialsProvider credentialsProvider =
        awsClientProperties.credentialsProvider(null, null, null);
    AwsCredentialsProvider credentialsProvider2 =
        awsClientProperties.credentialsProvider(null, null, null);

    assertThat(credentialsProvider)
        .as("Should create a new instance in each call")
        .isNotSameAs(credentialsProvider2);
  }

  @Test
  public void testBasicCredentialsConfiguration() {
    AwsClientProperties awsClientProperties = new AwsClientProperties();
    // set access key id and secret access key
    AwsCredentialsProvider credentialsProvider =
        awsClientProperties.credentialsProvider("key", "secret", null);

    assertThat(credentialsProvider.resolveCredentials())
        .as("Should use basic credentials if access key ID and secret access key are set")
        .isInstanceOf(AwsBasicCredentials.class);
    assertThat(credentialsProvider.resolveCredentials().accessKeyId())
        .as("The access key id should be the same as the one set by tag ACCESS_KEY_ID")
        .isEqualTo("key");

    assertThat(credentialsProvider.resolveCredentials().secretAccessKey())
        .as("The secret access key should be the same as the one set by tag SECRET_ACCESS_KEY")
        .isEqualTo("secret");
  }

  @Test
  public void testSessionCredentialsConfiguration() {
    // set access key id, secret access key, and session token
    AwsClientProperties awsClientProperties = new AwsClientProperties();
    AwsCredentialsProvider credentialsProvider =
        awsClientProperties.credentialsProvider("key", "secret", "token");

    assertThat(credentialsProvider.resolveCredentials())
        .as("Should use session credentials if session token is set")
        .isInstanceOf(AwsSessionCredentials.class);
    assertThat(credentialsProvider.resolveCredentials().accessKeyId())
        .as("The access key id should be the same as the one set by tag ACCESS_KEY_ID")
        .isEqualTo("key");
    assertThat(credentialsProvider.resolveCredentials().secretAccessKey())
        .as("The secret access key should be the same as the one set by tag SECRET_ACCESS_KEY")
        .isEqualTo("secret");
  }

  @Test
  public void refreshCredentialsEndpoint() {
    AwsClientProperties awsClientProperties =
        new AwsClientProperties(
            ImmutableMap.of(
                CatalogProperties.URI,
                "http://localhost:1234/v1",
                AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT,
                "http://localhost:1234/v1/credentials"));

    assertThat(awsClientProperties.credentialsProvider("key", "secret", "token"))
        .isInstanceOf(VendedCredentialsProvider.class);
  }

  @Test
  public void refreshCredentialsEndpointSetButRefreshDisabled() {
    AwsClientProperties awsClientProperties =
        new AwsClientProperties(
            ImmutableMap.of(
                AwsClientProperties.REFRESH_CREDENTIALS_ENABLED,
                "false",
                AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT,
                "http://localhost:1234/v1/credentials"));

    assertThat(awsClientProperties.credentialsProvider("key", "secret", "token"))
        .isInstanceOf(StaticCredentialsProvider.class);
  }

  @Test
  public void refreshCredentialsEndpointWithOAuthToken() {
    AwsClientProperties awsClientProperties =
        new AwsClientProperties(
            ImmutableMap.of(
                AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT,
                "http://localhost:1234/v1/credentials",
                CatalogProperties.URI,
                "http://localhost:1234/v1/catalog",
                OAuth2Properties.TOKEN,
                "oauth-token"));

    AwsCredentialsProvider provider =
        awsClientProperties.credentialsProvider("key", "secret", "token");
    assertThat(provider).isInstanceOf(VendedCredentialsProvider.class);
    VendedCredentialsProvider vendedCredentialsProvider = (VendedCredentialsProvider) provider;
    assertThat(vendedCredentialsProvider)
        .extracting("properties")
        .isEqualTo(
            ImmutableMap.of(
                AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT,
                "http://localhost:1234/v1/credentials",
                "credentials.uri",
                "http://localhost:1234/v1/credentials",
                CatalogProperties.URI,
                "http://localhost:1234/v1/catalog",
                OAuth2Properties.TOKEN,
                "oauth-token"));
  }

  @Test
  public void refreshCredentialsEndpointWithOverridingOAuthToken() {
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            "http://localhost:1234/v1",
            AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT,
            "http://localhost:1234/v1/credentials",
            OAuth2Properties.TOKEN,
            "oauth-token",
            "client.credentials-provider.token",
            "specific-token");
    AwsClientProperties awsClientProperties = new AwsClientProperties(properties);

    Map<String, String> expectedProperties =
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .put("credentials.uri", "http://localhost:1234/v1/credentials")
            .build();

    AwsCredentialsProvider provider =
        awsClientProperties.credentialsProvider("key", "secret", "token");
    assertThat(provider).isInstanceOf(VendedCredentialsProvider.class);
    VendedCredentialsProvider vendedCredentialsProvider = (VendedCredentialsProvider) provider;
    assertThat(vendedCredentialsProvider).extracting("properties").isEqualTo(expectedProperties);
  }

  @Test
  public void refreshCredentialsEndpointWithRelativePath() {
    AwsClientProperties awsClientProperties =
        new AwsClientProperties(
            ImmutableMap.of(
                CatalogProperties.URI,
                "http://localhost:1234/v1",
                AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT,
                "/relative/credentials/endpoint",
                OAuth2Properties.TOKEN,
                "oauth-token"));

    AwsCredentialsProvider provider =
        awsClientProperties.credentialsProvider("key", "secret", "token");
    assertThat(provider).isInstanceOf(VendedCredentialsProvider.class);
    VendedCredentialsProvider vendedCredentialsProvider = (VendedCredentialsProvider) provider;
    assertThat(vendedCredentialsProvider)
        .extracting("properties")
        .isEqualTo(
            ImmutableMap.of(
                CatalogProperties.URI,
                "http://localhost:1234/v1",
                "credentials.uri",
                "http://localhost:1234/v1/relative/credentials/endpoint",
                AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT,
                "/relative/credentials/endpoint",
                OAuth2Properties.TOKEN,
                "oauth-token"));
  }
}
