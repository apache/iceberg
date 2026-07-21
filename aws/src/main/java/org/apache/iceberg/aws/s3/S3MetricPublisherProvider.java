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

import java.io.Serializable;
import java.util.Map;
import software.amazon.awssdk.metrics.MetricPublisher;

/**
 * Interface for providing a configured {@link MetricPublisher} to S3FileIO.
 *
 * <p>Implementations must have a no-arg constructor and use {@link #initialize(Map)} to receive
 * catalog properties. This follows the same pattern as {@link
 * org.apache.iceberg.aws.AwsClientFactory}.
 *
 * <p>Because SDK MetricPublisher implementations (CloudWatchMetricPublisher,
 * LoggingMetricPublisher, EmfMetricLoggingPublisher) use static factories or builders rather than
 * public constructors, users implement this interface to wrap the appropriate builder/factory call
 * and pass any needed configuration from catalog properties.
 *
 * <p>Example:
 *
 * <pre>{@code
 * public class MyCloudWatchProvider implements S3MetricPublisherProvider {
 *   private MetricPublisher publisher;
 *
 *   @Override
 *   public void initialize(Map<String, String> properties) {
 *     publisher = CloudWatchMetricPublisher.builder()
 *         .namespace(properties.getOrDefault("s3.metrics.namespace", "Iceberg"))
 *         .build();
 *   }
 *
 *   @Override
 *   public MetricPublisher metricPublisher() {
 *     return publisher;
 *   }
 * }
 * }</pre>
 */
public interface S3MetricPublisherProvider extends Serializable {

  /**
   * Initialize the provider with catalog properties.
   *
   * @param properties catalog properties
   */
  default void initialize(Map<String, String> properties) {}

  /**
   * Return the configured {@link MetricPublisher}.
   *
   * @return a metric publisher instance
   */
  MetricPublisher metricPublisher();
}
