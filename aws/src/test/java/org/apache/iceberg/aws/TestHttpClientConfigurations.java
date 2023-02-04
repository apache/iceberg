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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;

public class TestHttpClientConfigurations {
  @Test
  public void testUrlConnectionConnectionSocketTimeoutConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_URLCONNECTION_SOCKET_TIMEOUT_MS, "90");
    properties.put(AwsProperties.HTTP_CLIENT_URLCONNECTION_CONNECTION_TIMEOUT_MS, "80");
    AwsProperties awsProperties = new AwsProperties(properties);
    UrlConnectionHttpClientConfigurations urlConnectionHttpClientConfigurations =
        new UrlConnectionHttpClientConfigurations();
    urlConnectionHttpClientConfigurations.initialize(
        awsProperties.urlConnectionHttpClientProperties());
    UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder =
        UrlConnectionHttpClient.builder();
    UrlConnectionHttpClient.Builder spyUrlConnectionHttpClientBuilder =
        Mockito.spy(urlConnectionHttpClientBuilder);
    ArgumentCaptor<Duration> socketTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);
    ArgumentCaptor<Duration> connectionTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);

    urlConnectionHttpClientConfigurations.configureUrlConnectionHttpClientBuilder(
        spyUrlConnectionHttpClientBuilder);
    Mockito.verify(spyUrlConnectionHttpClientBuilder).socketTimeout(socketTimeoutCaptor.capture());
    Mockito.verify(spyUrlConnectionHttpClientBuilder)
        .connectionTimeout(connectionTimeoutCaptor.capture());

    Duration capturedSocketTimeout = socketTimeoutCaptor.getValue();
    Duration capturedConnectionTimeout = connectionTimeoutCaptor.getValue();

    Assert.assertEquals(
        "The configured socket timeout should be 90 ms", 90, capturedSocketTimeout.toMillis());
    Assert.assertEquals(
        "The configured connection timeout should be 80 ms",
        80,
        capturedConnectionTimeout.toMillis());
  }

  @Test
  public void testUrlConnectionConnectionTimeoutConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_URLCONNECTION_CONNECTION_TIMEOUT_MS, "80");
    AwsProperties awsProperties = new AwsProperties(properties);
    UrlConnectionHttpClientConfigurations urlConnectionHttpClientConfigurations =
        new UrlConnectionHttpClientConfigurations();
    urlConnectionHttpClientConfigurations.initialize(
        awsProperties.urlConnectionHttpClientProperties());
    UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder =
        UrlConnectionHttpClient.builder();
    UrlConnectionHttpClient.Builder spyUrlConnectionHttpClientBuilder =
        Mockito.spy(urlConnectionHttpClientBuilder);
    ArgumentCaptor<Duration> connectionTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);
    ArgumentCaptor<Duration> socketTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);

    urlConnectionHttpClientConfigurations.configureUrlConnectionHttpClientBuilder(
        spyUrlConnectionHttpClientBuilder);
    Mockito.verify(spyUrlConnectionHttpClientBuilder)
        .connectionTimeout(connectionTimeoutCaptor.capture());
    Mockito.verify(spyUrlConnectionHttpClientBuilder, Mockito.never())
        .socketTimeout(socketTimeoutCaptor.capture());

    Duration capturedConnectionTimeout = connectionTimeoutCaptor.getValue();

    Assert.assertEquals(
        "The configured connection timeout should be 80 ms",
        80,
        capturedConnectionTimeout.toMillis());
  }

  @Test
  public void testUrlConnectionSocketTimeoutConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_URLCONNECTION_SOCKET_TIMEOUT_MS, "90");
    AwsProperties awsProperties = new AwsProperties(properties);
    UrlConnectionHttpClientConfigurations urlConnectionHttpClientConfigurations =
        new UrlConnectionHttpClientConfigurations();
    urlConnectionHttpClientConfigurations.initialize(
        awsProperties.urlConnectionHttpClientProperties());
    UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder =
        UrlConnectionHttpClient.builder();
    UrlConnectionHttpClient.Builder spyUrlConnectionHttpClientBuilder =
        Mockito.spy(urlConnectionHttpClientBuilder);
    ArgumentCaptor<Duration> connectionTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);
    ArgumentCaptor<Duration> socketTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);

    urlConnectionHttpClientConfigurations.configureUrlConnectionHttpClientBuilder(
        spyUrlConnectionHttpClientBuilder);
    Mockito.verify(spyUrlConnectionHttpClientBuilder, Mockito.never())
        .connectionTimeout(connectionTimeoutCaptor.capture());
    Mockito.verify(spyUrlConnectionHttpClientBuilder).socketTimeout(socketTimeoutCaptor.capture());

    Duration capturedSocketTimeout = socketTimeoutCaptor.getValue();

    Assert.assertEquals(
        "The configured socket timeout should be 90 ms", 90, capturedSocketTimeout.toMillis());
  }

  @Test
  public void testUrlConnectionDefaultConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    AwsProperties awsProperties = new AwsProperties(properties);
    UrlConnectionHttpClientConfigurations urlConnectionHttpClientConfigurations =
        new UrlConnectionHttpClientConfigurations();
    urlConnectionHttpClientConfigurations.initialize(
        awsProperties.urlConnectionHttpClientProperties());
    UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder =
        UrlConnectionHttpClient.builder();
    UrlConnectionHttpClient.Builder spyUrlConnectionHttpClientBuilder =
        Mockito.spy(urlConnectionHttpClientBuilder);
    ArgumentCaptor<Duration> connectionTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);
    ArgumentCaptor<Duration> socketTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);

    urlConnectionHttpClientConfigurations.configureUrlConnectionHttpClientBuilder(
        spyUrlConnectionHttpClientBuilder);
    Mockito.verify(spyUrlConnectionHttpClientBuilder, Mockito.never())
        .connectionTimeout(connectionTimeoutCaptor.capture());
    Mockito.verify(spyUrlConnectionHttpClientBuilder, Mockito.never())
        .socketTimeout(socketTimeoutCaptor.capture());
  }

  @Test
  public void testApacheConnectionSocketTimeoutConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_SOCKET_TIMEOUT_MS, "100");
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_CONNECTION_TIMEOUT_MS, "200");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Duration> socketTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);
    ArgumentCaptor<Duration> connectionTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder).socketTimeout(socketTimeoutCaptor.capture());
    Mockito.verify(spyApacheHttpClientBuilder).connectionTimeout(connectionTimeoutCaptor.capture());

    Duration capturedSocketTimeout = socketTimeoutCaptor.getValue();
    Duration capturedConnectionTimeout = connectionTimeoutCaptor.getValue();

    Assert.assertEquals(
        "The configured socket timeout should be 100 ms", 100, capturedSocketTimeout.toMillis());
    Assert.assertEquals(
        "The configured connection timeout should be 200 ms",
        200,
        capturedConnectionTimeout.toMillis());
  }

  @Test
  public void testApacheConnectionTimeoutConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_CONNECTION_TIMEOUT_MS, "200");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Duration> connectionTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);
    ArgumentCaptor<Duration> socketTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder).connectionTimeout(connectionTimeoutCaptor.capture());
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .socketTimeout(socketTimeoutCaptor.capture());

    Duration capturedConnectionTimeout = connectionTimeoutCaptor.getValue();

    Assert.assertEquals(
        "The configured connection timeout should be 200 ms",
        200,
        capturedConnectionTimeout.toMillis());
  }

  @Test
  public void testApacheSocketTimeoutConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_SOCKET_TIMEOUT_MS, "100");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Duration> connectionTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);
    ArgumentCaptor<Duration> socketTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .connectionTimeout(connectionTimeoutCaptor.capture());
    Mockito.verify(spyApacheHttpClientBuilder).socketTimeout(socketTimeoutCaptor.capture());

    Duration capturedSocketTimeout = socketTimeoutCaptor.getValue();

    Assert.assertEquals(
        "The configured socket timeout should be 100 ms", 100, capturedSocketTimeout.toMillis());
  }

  @Test
  public void testApacheDefaultConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Duration> connectionTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);
    ArgumentCaptor<Duration> socketTimeoutCaptor = ArgumentCaptor.forClass(Duration.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .connectionTimeout(connectionTimeoutCaptor.capture());
    Mockito.verify(spyApacheHttpClientBuilder, Mockito.never())
        .socketTimeout(socketTimeoutCaptor.capture());
  }

  @Test
  public void testApacheConnectionAcquisitionTimeoutConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_CONNECTION_ACQUISITION_TIMEOUT_MS, "101");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Duration> connectionAcquisitionTimeoutCaptor =
        ArgumentCaptor.forClass(Duration.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder)
        .connectionAcquisitionTimeout(connectionAcquisitionTimeoutCaptor.capture());

    Duration capturedConnectionAcquisitionTimeout = connectionAcquisitionTimeoutCaptor.getValue();

    Assert.assertEquals(
        "The configured connection acquisition timeout should be 101 ms",
        101,
        capturedConnectionAcquisitionTimeout.toMillis());
  }

  @Test
  public void testApacheConnectionMaxIdleTimeConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_CONNECTION_MAX_IDLE_TIME_MS, "102");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Duration> connectionMaxIdleTimeCaptor = ArgumentCaptor.forClass(Duration.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder)
        .connectionMaxIdleTime(connectionMaxIdleTimeCaptor.capture());

    Duration capturedConnectionMaxIdleTime = connectionMaxIdleTimeCaptor.getValue();

    Assert.assertEquals(
        "The configured connection max idle time should be 102 ms",
        102,
        capturedConnectionMaxIdleTime.toMillis());
  }

  @Test
  public void testApacheConnectionTimeToLiveConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_CONNECTION_TIME_TO_LIVE_MS, "103");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Duration> connectionTimeToLiveCaptor = ArgumentCaptor.forClass(Duration.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder)
        .connectionTimeToLive(connectionTimeToLiveCaptor.capture());

    Duration capturedConnectionTimeToLive = connectionTimeToLiveCaptor.getValue();

    Assert.assertEquals(
        "The configured connection time to live should be 103 ms",
        103,
        capturedConnectionTimeToLive.toMillis());
  }

  @Test
  public void testApacheExpectContinueEnabledConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_EXPECT_CONTINUE_ENABLED, "true");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Boolean> expectContinueEnabledCaptor = ArgumentCaptor.forClass(Boolean.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder)
        .expectContinueEnabled(expectContinueEnabledCaptor.capture());

    Boolean capturedExpectContinueEnabled = expectContinueEnabledCaptor.getValue();

    Assert.assertTrue(
        "The configured expect continue enabled should be true", capturedExpectContinueEnabled);
  }

  @Test
  public void testApacheMaxConnectionsConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_MAX_CONNECTIONS, "104");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Integer> maxConnectionsCaptor = ArgumentCaptor.forClass(Integer.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder).maxConnections(maxConnectionsCaptor.capture());

    Integer capturedMaxConnections = maxConnectionsCaptor.getValue();

    Assert.assertEquals(
        "The configured max connections should be 104", 104, capturedMaxConnections.intValue());
  }

  @Test
  public void testApacheTcpKeepAliveConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_TCP_KEEP_ALIVE_ENABLED, "true");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Boolean> tcpKeepAliveCaptor = ArgumentCaptor.forClass(Boolean.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder).tcpKeepAlive(tcpKeepAliveCaptor.capture());

    Boolean capturedTcpKeepAlive = tcpKeepAliveCaptor.getValue();

    Assert.assertTrue("The configured tcp keep live enabled should be true", capturedTcpKeepAlive);
  }

  @Test
  public void testApacheUseIdleConnectionReaperConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_APACHE_USE_IDLE_CONNECTION_REAPER_ENABLED, "false");
    AwsProperties awsProperties = new AwsProperties(properties);
    ApacheHttpClientConfigurations apacheHttpClientConfigurations =
        new ApacheHttpClientConfigurations();
    apacheHttpClientConfigurations.initialize(awsProperties.apacheHttpClientProperties());
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    ApacheHttpClient.Builder spyApacheHttpClientBuilder = Mockito.spy(apacheHttpClientBuilder);
    ArgumentCaptor<Boolean> useIdleConnectionReaperCaptor = ArgumentCaptor.forClass(Boolean.class);

    apacheHttpClientConfigurations.configureApacheHttpClientBuilder(spyApacheHttpClientBuilder);
    Mockito.verify(spyApacheHttpClientBuilder)
        .useIdleConnectionReaper(useIdleConnectionReaperCaptor.capture());

    Boolean capturedUseIdleConnectionReaper = useIdleConnectionReaperCaptor.getValue();

    Assert.assertFalse(
        "The configured use idle connection reaper enabled should be false",
        capturedUseIdleConnectionReaper);
  }
}
