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
package org.apache.iceberg.aws.s3;

import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * Interface for configuring a custom {@link software.amazon.awssdk.metrics.MetricPublisher} on an
 * S3 client.
 *
 * <p>Implementations must provide a static {@code create(Map<String, String>)} factory method that
 * returns an instance of this interface.
 *
 * <p>Example implementation for Prometheus metrics:
 *
 * <pre>{@code
 * public class PrometheusMetricsConfigurations implements S3MetricsPublisherConfigurations {
 *   private MetricPublisher publisher;
 *
 *   public static PrometheusMetricsConfigurations create(Map<String, String> properties) {
 *     PrometheusMetricsConfigurations config = new PrometheusMetricsConfigurations();
 *     // Configure your Prometheus MetricPublisher using properties
 *     config.publisher = new PrometheusMetricPublisher(properties);
 *     return config;
 *   }
 *
 *   @Override
 *   public <T extends S3ClientBuilder> void configureS3ClientBuilder(T builder) {
 *     ClientOverrideConfiguration.Builder configBuilder =
 *         builder.overrideConfiguration() != null
 *             ? builder.overrideConfiguration().toBuilder()
 *             : ClientOverrideConfiguration.builder();
 *     builder.overrideConfiguration(configBuilder.addMetricPublisher(publisher).build());
 *   }
 * }
 * }</pre>
 *
 * <p>Then configure via property: {@code s3.metrics-publisher-impl=com.example.PrometheusMetricsConfigurations}
 */
public interface S3MetricsPublisherConfigurations {

  /**
   * Configure the S3 client builder with a custom metric publisher.
   *
   * @param builder the S3 client builder to configure
   * @param <T> the type of S3 client builder
   */
  <T extends S3ClientBuilder> void configureS3ClientBuilder(T builder);
}
