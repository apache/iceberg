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
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;

class CrtHttpAsyncClientConfigurations {
  private Integer maxConcurrency;
  private Long connectionTimeoutMs;

  private CrtHttpAsyncClientConfigurations() {}

  public <T extends S3CrtAsyncClientBuilder> void configureHttpAsyncClientBuilder(
      T s3CrtAsyncClientBuilder) {
    configureCrtHttpAsyncClientBuilder(s3CrtAsyncClientBuilder);
  }

  private void initialize(Map<String, String> httpAsyncClientProperties) {
    this.maxConcurrency =
        PropertyUtil.propertyAsInt(
            httpAsyncClientProperties,
            HttpAsyncClientProperties.CRT_MAX_CONCURRENCY,
            HttpAsyncClientProperties.CRT_MAX_CONCURRENCY_DEFAULT);
    this.connectionTimeoutMs =
        PropertyUtil.propertyAsNullableLong(
            httpAsyncClientProperties, HttpAsyncClientProperties.CRT_CONNECTION_TIMEOUT_MS);
  }

  @VisibleForTesting
  void configureCrtHttpAsyncClientBuilder(S3CrtAsyncClientBuilder crtBuilder) {
    if (connectionTimeoutMs != null) {
      crtBuilder.httpConfiguration(
          S3CrtHttpConfiguration.builder()
              .connectionTimeout(Duration.ofMillis(connectionTimeoutMs))
              .build());
    }
    if (maxConcurrency != null) {
      crtBuilder.maxConcurrency(maxConcurrency);
    }
  }

  public static CrtHttpAsyncClientConfigurations create(Map<String, String> properties) {
    CrtHttpAsyncClientConfigurations configurations = new CrtHttpAsyncClientConfigurations();
    configurations.initialize(properties);
    return configurations;
  }
}
