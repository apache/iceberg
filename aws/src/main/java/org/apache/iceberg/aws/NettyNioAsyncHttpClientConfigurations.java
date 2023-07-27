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
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

class NettyNioAsyncHttpClientConfigurations {

  private Long connectionMaxIdleTime;
  private Long connectionAcquisitionTimeout;
  private Long connectionTimeout;
  private Long connectionTimeToLive;
  private Integer maxConcurrency;
  private Long readTimeout;
  private Integer maxPendingConnectionAcquires;
  private Long writeTimeout;

  private NettyNioAsyncHttpClientConfigurations() {}

  public <T extends AwsAsyncClientBuilder> void configureHttpClientBuilder(T awsClientBuilder) {
    NettyNioAsyncHttpClient.Builder nettyNioAsyncHttpClientBuilder =
        NettyNioAsyncHttpClient.builder();
    configureNettyNioAsyncHttpClientBuilder(nettyNioAsyncHttpClientBuilder);
    awsClientBuilder.httpClientBuilder(nettyNioAsyncHttpClientBuilder);
  }

  private void initialize(Map<String, String> httpClientProperties) {
    this.connectionMaxIdleTime =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.NETTYNIO_CONNECTION_MAX_IDLE_TIME_MS);
    this.connectionAcquisitionTimeout =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.NETTYNIO_ACQUISITION_TIMEOUT_MS);
    this.connectionTimeout =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.NETTYNIO_CONNECTION_TIMEOUT_MS);
    this.connectionTimeToLive =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.NETTYNIO_CONNECTION_TIME_TO_LIVE_MS);
    this.maxConcurrency =
        PropertyUtil.propertyAsNullableInt(
            httpClientProperties, HttpClientProperties.NETTYNIO_MAX_CONCURRENCY);
    this.readTimeout =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.NETTYNIO_READ_TIMEOUT);
    this.maxPendingConnectionAcquires =
        PropertyUtil.propertyAsNullableInt(
            httpClientProperties, HttpClientProperties.NETTYNIO_MAX_PENDING_CONNECTION_ACQUIRES);
    this.writeTimeout =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.NETTYNIO_WRITE_TIMEOUT);
  }

  void configureNettyNioAsyncHttpClientBuilder(
      NettyNioAsyncHttpClient.Builder nettyNioHttpClientBuilder) {
    if (connectionMaxIdleTime != null) {
      nettyNioHttpClientBuilder.connectionMaxIdleTime(Duration.ofMillis(connectionMaxIdleTime));
    }
    if (connectionAcquisitionTimeout != null) {
      nettyNioHttpClientBuilder.connectionAcquisitionTimeout(
          Duration.ofMillis(connectionAcquisitionTimeout));
    }
    if (connectionTimeout != null) {
      nettyNioHttpClientBuilder.connectionTimeout(Duration.ofMillis(connectionTimeout));
    }
    if (connectionTimeToLive != null) {
      nettyNioHttpClientBuilder.connectionTimeToLive(Duration.ofMillis(connectionTimeToLive));
    }
    if (maxConcurrency != null) {
      nettyNioHttpClientBuilder.maxConcurrency(maxConcurrency);
    }
    if (readTimeout != null) {
      nettyNioHttpClientBuilder.readTimeout(Duration.ofMillis(readTimeout));
    }
    if (maxPendingConnectionAcquires != null) {
      nettyNioHttpClientBuilder.maxPendingConnectionAcquires(maxPendingConnectionAcquires);
    }
    if (writeTimeout != null) {
      nettyNioHttpClientBuilder.writeTimeout(Duration.ofMillis(writeTimeout));
    }
  }

  public static NettyNioAsyncHttpClientConfigurations create(
      Map<String, String> httpClientProperties) {
    NettyNioAsyncHttpClientConfigurations configurations =
        new NettyNioAsyncHttpClientConfigurations();
    configurations.initialize(httpClientProperties);
    return configurations;
  }
}
