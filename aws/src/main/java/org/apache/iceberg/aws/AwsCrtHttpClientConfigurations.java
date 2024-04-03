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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;

class AwsCrtHttpClientConfigurations {
  private Long connectionTimeoutMs;
  private Long connectionMaxIdleTimeMs;
  private Integer maxConcurrency;

  private AwsCrtHttpClientConfigurations() {}

  public <T extends AwsSyncClientBuilder> void configureHttpClientBuilder(T awsClientBuilder) {
    AwsCrtHttpClient.Builder httpClientBuilder = AwsCrtHttpClient.builder();
    configureAwsCrtHttpClientBuilder(httpClientBuilder);
    awsClientBuilder.httpClientBuilder(httpClientBuilder);
  }

  private void initialize(Map<String, String> httpClientProperties) {
    this.connectionTimeoutMs =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.AWS_CRT_CONNECTION_TIMEOUT_MS);
    this.connectionMaxIdleTimeMs =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.AWS_CRT_CONNECTION_MAX_IDLE_TIME_MS);
    this.maxConcurrency =
        PropertyUtil.propertyAsNullableInt(
            httpClientProperties, HttpClientProperties.AWS_CRT_MAX_CONCURRENCY);
  }

  @VisibleForTesting
  void configureAwsCrtHttpClientBuilder(AwsCrtHttpClient.Builder httpClientBuilder) {
    if (connectionTimeoutMs != null) {
      httpClientBuilder.connectionTimeout(Duration.ofMillis(connectionTimeoutMs));
    }
    if (connectionMaxIdleTimeMs != null) {
      httpClientBuilder.connectionMaxIdleTime(Duration.ofMillis(connectionMaxIdleTimeMs));
    }
    if (maxConcurrency != null) {
      httpClientBuilder.maxConcurrency(maxConcurrency);
    }
  }

  public static AwsCrtHttpClientConfigurations create(Map<String, String> properties) {
    AwsCrtHttpClientConfigurations configurations = new AwsCrtHttpClientConfigurations();
    configurations.initialize(properties);
    return configurations;
  }
}
