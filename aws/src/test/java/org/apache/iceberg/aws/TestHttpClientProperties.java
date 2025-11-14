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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import java.util.Map;
import org.apache.iceberg.aws.HttpClientCache.ManagedHttpClient;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class TestHttpClientProperties {

  @Test
  public void testUrlHttpClientConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HttpClientProperties.CLIENT_TYPE, "urlconnection");
    HttpClientProperties httpProperties = new HttpClientProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<SdkHttpClient> httpClientCaptor = ArgumentCaptor.forClass(SdkHttpClient.class);

    httpProperties.applyHttpClientConfigurations(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).httpClient(httpClientCaptor.capture());
    SdkHttpClient capturedHttpClient = httpClientCaptor.getValue();

    assertThat(capturedHttpClient)
        .as("Should use managed SDK http client")
        .isInstanceOf(ManagedHttpClient.class);

    // Verify the underlying delegate is UrlConnectionHttpClient
    ManagedHttpClient managedClient = (ManagedHttpClient) capturedHttpClient;
    assertThat(managedClient.httpClient())
        .as("Underlying client should be UrlConnectionHttpClient")
        .isInstanceOf(UrlConnectionHttpClient.class);
  }

  @Test
  public void testApacheHttpClientConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HttpClientProperties.CLIENT_TYPE, "apache");
    HttpClientProperties httpProperties = new HttpClientProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<SdkHttpClient> httpClientCaptor = ArgumentCaptor.forClass(SdkHttpClient.class);

    httpProperties.applyHttpClientConfigurations(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).httpClient(httpClientCaptor.capture());
    SdkHttpClient capturedHttpClient = httpClientCaptor.getValue();

    assertThat(capturedHttpClient)
        .as("Should use managed SDK http client")
        .isInstanceOf(ManagedHttpClient.class);

    // Verify the underlying delegate is ApacheHttpClient
    ManagedHttpClient managedClient = (ManagedHttpClient) capturedHttpClient;
    assertThat(managedClient.httpClient())
        .as("Underlying client should be ApacheHttpClient")
        .isInstanceOf(ApacheHttpClient.class);
  }

  @Test
  public void testInvalidHttpClientType() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HttpClientProperties.CLIENT_TYPE, "test");
    HttpClientProperties httpProperties = new HttpClientProperties(properties);
    S3ClientBuilder s3ClientBuilder = S3Client.builder();

    assertThatThrownBy(() -> httpProperties.applyHttpClientConfigurations(s3ClientBuilder))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unrecognized HTTP client type test");
  }

  @Test
  public void testApacheHttpClientConfiguredAsSharedResource() {
    Map<String, String> properties = Maps.newHashMap();
    ApacheHttpClientConfigurations apacheConfig = ApacheHttpClientConfigurations.create(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);

    apacheConfig.configureHttpClientBuilder(mockS3ClientBuilder);

    // Verify that httpClient() is called with a managed client (as a shared resource)
    verify(mockS3ClientBuilder).httpClient(any(ManagedHttpClient.class));
  }

  @Test
  public void testUrlConnectionHttpClientConfiguredAsSharedResource() {
    Map<String, String> properties = Maps.newHashMap();
    UrlConnectionHttpClientConfigurations urlConfig =
        UrlConnectionHttpClientConfigurations.create(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);

    urlConfig.configureHttpClientBuilder(mockS3ClientBuilder);

    // Verify that httpClient() is called with a managed client (as a shared resource)
    verify(mockS3ClientBuilder).httpClient(any(ManagedHttpClient.class));
  }
}
