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

import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;

public class TestHttpClientConfigurations {
  @Test
  public void testUrlConnectionOverrideConfigurations() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_URLCONNECTION_SOCKET_TIMEOUT_MS, "90");
    properties.put(AwsProperties.HTTP_CLIENT_URLCONNECTION_CONNECTION_TIMEOUT_MS, "80");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_SOCKET_TIMEOUT_MS, "100");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_CONNECTION_TIMEOUT_MS, "200");
    AwsProperties awsProperties = new AwsProperties(properties);
    UrlConnectionHttpClientConfigurations urlConnectionHttpClientConfigurations =
        UrlConnectionHttpClientConfigurations.create(awsProperties.httpClientProperties());
    UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder =
        UrlConnectionHttpClient.builder();
    UrlConnectionHttpClient.Builder spyUrlConnectionHttpClientBuilder =
        Mockito.spy(urlConnectionHttpClientBuilder);

    urlConnectionHttpClientConfigurations.configureUrlConnectionHttpClientBuilder(
        spyUrlConnectionHttpClientBuilder);

    Mockito.verify(spyUrlConnectionHttpClientBuilder).socketTimeout(Duration.ofMillis(90));
    Mockito.verify(spyUrlConnectionHttpClientBuilder).connectionTimeout(Duration.ofMillis(80));
  }

  @Test
  public void testUrlConnectionDefaultConfigurations() {
    Map<String, String> properties = Maps.newHashMap();
    AwsProperties awsProperties = new AwsProperties(properties);
    UrlConnectionHttpClientConfigurations urlConnectionHttpClientConfigurations =
        UrlConnectionHttpClientConfigurations.create(awsProperties.httpClientProperties());
    UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder =
        UrlConnectionHttpClient.builder();
    UrlConnectionHttpClient.Builder spyUrlConnectionHttpClientBuilder =
        Mockito.spy(urlConnectionHttpClientBuilder);

    urlConnectionHttpClientConfigurations.configureUrlConnectionHttpClientBuilder(
        spyUrlConnectionHttpClientBuilder);

    Mockito.verify(spyUrlConnectionHttpClientBuilder, Mockito.never())
        .connectionTimeout(Mockito.any(Duration.class));
    Mockito.verify(spyUrlConnectionHttpClientBuilder, Mockito.never())
        .socketTimeout(Mockito.any(Duration.class));
  }

  @Test
  public void testApacheOverrideConfigurations() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_URLCONNECTION_SOCKET_TIMEOUT_MS, "90");
    properties.put(AwsProperties.HTTP_CLIENT_URLCONNECTION_CONNECTION_TIMEOUT_MS, "80");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_SOCKET_TIMEOUT_MS, "100");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_CONNECTION_TIMEOUT_MS, "200");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_CONNECTION_ACQUISITION_TIMEOUT_MS, "101");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_CONNECTION_MAX_IDLE_TIME_MS, "102");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_CONNECTION_TIME_TO_LIVE_MS, "103");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_EXPECT_CONTINUE_ENABLED, "true");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_MAX_CONNECTIONS, "104");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_TCP_KEEP_ALIVE_ENABLED, "true");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_USE_IDLE_CONNECTION_REAPER_ENABLED, "false");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        ApacheHttpClientConfigurations.create(awsProperties.httpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);

    Mockito.verify(spyApacheHttpClientBuilder).socketTimeout(Duration.ofMillis(100));
    Mockito.verify(spyApacheHttpClientBuilder).connectionTimeout(Duration.ofMillis(200));
    Mockito.verify(spyApacheHttpClientBuilder).connectionAcquisitionTimeout(Duration.ofMillis(101));
    Mockito.verify(spyApacheHttpClientBuilder).connectionMaxIdleTime(Duration.ofMillis(102));
    Mockito.verify(spyApacheHttpClientBuilder).connectionTimeToLive(Duration.ofMillis(103));
    Mockito.verify(spyApacheHttpClientBuilder).expectContinueEnabled(true);
    Mockito.verify(spyApacheHttpClientBuilder).maxConnections(104);
    Mockito.verify(spyApacheHttpClientBuilder).tcpKeepAlive(true);
    Mockito.verify(spyApacheHttpClientBuilder).useIdleConnectionReaper(false);
  }

  @Test
  public void testApacheDefaultConfigurations() {
    Map<String, String> properties = Maps.newHashMap();
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        ApacheHttpClientConfigurations.create(awsProperties.httpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);

    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .connectionTimeout(Mockito.any(Duration.class));
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .socketTimeout(Mockito.any(Duration.class));
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .connectionAcquisitionTimeout(Mockito.any(Duration.class));
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .connectionMaxIdleTime(Mockito.any(Duration.class));
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .connectionTimeToLive(Mockito.any(Duration.class));
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .expectContinueEnabled(Mockito.anyBoolean());
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never()).maxConnections(Mockito.anyInt());
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never()).tcpKeepAlive(Mockito.anyBoolean());
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .useIdleConnectionReaper(Mockito.anyBoolean());
  }
}
