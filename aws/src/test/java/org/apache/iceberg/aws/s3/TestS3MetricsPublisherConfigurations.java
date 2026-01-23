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

import java.util.Map;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/** Test implementation of S3MetricsPublisherConfigurations for unit tests. */
public class TestS3MetricsPublisherConfigurations implements S3MetricsPublisherConfigurations {

  public static TestS3MetricsPublisherConfigurations create(Map<String, String> properties) {
    return new TestS3MetricsPublisherConfigurations();
  }

  @Override
  public <T extends S3ClientBuilder> void configureS3ClientBuilder(T builder) {
    ClientOverrideConfiguration.Builder configBuilder =
        builder.overrideConfiguration() != null
            ? builder.overrideConfiguration().toBuilder()
            : ClientOverrideConfiguration.builder();
    builder.overrideConfiguration(configBuilder.addMetricPublisher(new NoOpMetricPublisher()).build());
  }

  private static class NoOpMetricPublisher implements MetricPublisher {
    @Override
    public void publish(MetricCollection metricCollection) {}

    @Override
    public void close() {}
  }
}
