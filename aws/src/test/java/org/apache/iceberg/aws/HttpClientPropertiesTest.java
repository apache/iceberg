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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class HttpClientPropertiesTest {

  @Test
  public void testUrlHttpClientConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HttpClientProperties.CLIENT_TYPE, "urlconnection");
    HttpClientProperties httpClientProperties = new HttpClientProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<SdkHttpClient.Builder> httpClientBuilderCaptor =
        ArgumentCaptor.forClass(SdkHttpClient.Builder.class);

    httpClientProperties.applyHttpClientConfigurations(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).httpClientBuilder(httpClientBuilderCaptor.capture());
    SdkHttpClient.Builder capturedHttpClientBuilder = httpClientBuilderCaptor.getValue();

    Assertions.assertThat(capturedHttpClientBuilder instanceof UrlConnectionHttpClient.Builder)
        .withFailMessage("Should use url connection http client")
        .isTrue();
  }

  @Test
  public void testApacheHttpClientConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HttpClientProperties.CLIENT_TYPE, "apache");
    HttpClientProperties httpClientProperties = new HttpClientProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<SdkHttpClient.Builder> httpClientBuilderCaptor =
        ArgumentCaptor.forClass(SdkHttpClient.Builder.class);

    httpClientProperties.applyHttpClientConfigurations(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).httpClientBuilder(httpClientBuilderCaptor.capture());
    SdkHttpClient.Builder capturedHttpClientBuilder = httpClientBuilderCaptor.getValue();
    Assertions.assertThat(capturedHttpClientBuilder instanceof ApacheHttpClient.Builder)
        .withFailMessage("Should use apache http client")
        .isTrue();
  }

  @Test
  public void testInvalidHttpClientType() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HttpClientProperties.CLIENT_TYPE, "test");
    HttpClientProperties httpClientProperties = new HttpClientProperties(properties);
    S3ClientBuilder s3ClientBuilder = S3Client.builder();

    Assertions.assertThatThrownBy(
            () -> httpClientProperties.applyHttpClientConfigurations(s3ClientBuilder))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unrecognized HTTP client type test");
  }
}
