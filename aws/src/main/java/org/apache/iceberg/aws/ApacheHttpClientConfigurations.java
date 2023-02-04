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
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

public class ApacheHttpClientConfigurations implements HttpClientConfigurations {

  private ApacheHttpClient.Builder builder;

  public ApacheHttpClientConfigurations() {
    this.builder = ApacheHttpClient.builder();
  }

  @Override
  public <T extends AwsSyncClientBuilder> void applyConfigurations(T clientBuilder) {
    clientBuilder.httpClientBuilder(builder);
  }

  @Override
  public ApacheHttpClientConfigurations withConnectionTimeoutMs(Long connectionTimeoutMs) {
    if (connectionTimeoutMs != null) {
      builder.connectionTimeout(Duration.ofMillis(connectionTimeoutMs));
    }
    return this;
  }

  @Override
  public ApacheHttpClientConfigurations withSocketTimeoutMs(Long socketTimeoutMs) {
    if (socketTimeoutMs != null) {
      builder.socketTimeout(Duration.ofMillis(socketTimeoutMs));
    }
    return this;
  }

  @Override
  public ApacheHttpClientConfigurations withConnectionAcquisitionTimeoutMs(
      Long connectionAcquisitionTimeoutMs) {
    if (connectionAcquisitionTimeoutMs != null) {
      builder.connectionAcquisitionTimeout(Duration.ofMillis(connectionAcquisitionTimeoutMs));
    }
    return this;
  }

  @Override
  public ApacheHttpClientConfigurations withConnectionMaxIdleTimeMs(Long connectionMaxIdleTimeMs) {
    if (connectionMaxIdleTimeMs != null) {
      builder.connectionMaxIdleTime(Duration.ofMillis(connectionMaxIdleTimeMs));
    }
    return this;
  }

  @Override
  public ApacheHttpClientConfigurations withConnectionTimeToLiveMs(Long timeToLiveMs) {
    if (timeToLiveMs != null) {
      builder.connectionTimeToLive(Duration.ofMillis(timeToLiveMs));
    }
    return this;
  }

  @Override
  public ApacheHttpClientConfigurations withExpectContinueEnabled(Boolean expectContinueEnabled) {
    if (expectContinueEnabled != null) {
      builder.expectContinueEnabled(expectContinueEnabled);
    }
    return this;
  }

  @Override
  public ApacheHttpClientConfigurations withMaxConnections(Integer maxConnections) {
    if (maxConnections != null) {
      builder.maxConnections(maxConnections);
    }
    return this;
  }

  @Override
  public ApacheHttpClientConfigurations withTcpKeepAliveEnabled(Boolean tcpKeepAlive) {
    if (tcpKeepAlive != null) {
      builder.tcpKeepAlive(tcpKeepAlive);
    }
    return this;
  }

  @Override
  public ApacheHttpClientConfigurations withUseIdleConnectionReaperEnabled(
      Boolean usedIdleConnectionReaper) {
    if (usedIdleConnectionReaper != null) {
      builder.useIdleConnectionReaper(usedIdleConnectionReaper);
    }
    return this;
  }
}
