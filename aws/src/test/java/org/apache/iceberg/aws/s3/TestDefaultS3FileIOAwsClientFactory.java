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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

// disable loading (unexpected) profiles
@SetSystemProperty(key = "aws.sharedCredentialsFile", value = "target/missing/ignore")
@SetSystemProperty(key = "aws.configFile", value = "target/missing/ignore")
class TestDefaultS3FileIOAwsClientFactory {
  @Test
  void metricsPublisherCreateSync() {
    final var factory = new DefaultS3FileIOAwsClientFactory();
    factory.initialize(
        Map.of(
            "client.credentials-provider", TestCredentialProvider.class.getName(),
            "client.metrics-publisher", NoArgPublisher.class.getName(),
            "client.region", "us-east-1"));
    try (final var s3 = factory.s3()) {
      assertThat(NoArgPublisher.INSTANCE.get()).isNotNull();
    } finally {
      NoArgPublisher.INSTANCE.remove();
    }
  }

  @Test
  void metricsPublisherCreateMapSync() {
    final var factory = new DefaultS3FileIOAwsClientFactory();
    factory.initialize(
        Map.of(
            "client.credentials-provider",
            TestCredentialProvider.class.getName(),
            "client.metrics-publisher",
            MapArgPublisher.class.getName(),
            "client.metrics-publisher.test",
            "ok",
            "client.region",
            "us-east-1"));
    try (final var s3 = factory.s3()) {
      assertThat(MapArgPublisher.INSTANCE.get().args.get("test")).isEqualTo("ok");
    } finally {
      NoArgPublisher.INSTANCE.remove();
    }
  }

  @Test
  void metricsPublisherCreateASync() {
    final var factory = new DefaultS3FileIOAwsClientFactory();
    factory.initialize(
        Map.of(
            // CRT impl is still not equivalent to the default java one
            "s3.crt.enabled",
            "false",
            "client.credentials-provider",
            TestCredentialProvider.class.getName(),
            "client.metrics-publisher",
            NoArgPublisher.class.getName(),
            "client.region",
            "us-east-1"));
    try (final var s3 = factory.s3Async()) {
      assertThat(NoArgPublisher.INSTANCE.get()).isNotNull();
    } finally {
      NoArgPublisher.INSTANCE.remove();
    }
  }

  @Test
  void metricsPublisherCreateMapASync() {
    final var factory = new DefaultS3FileIOAwsClientFactory();
    factory.initialize(
        Map.of(
            // CRT impl is still not equivalent to the default java one
            "s3.crt.enabled", "false",
            "client.credentials-provider", TestCredentialProvider.class.getName(),
            "client.metrics-publisher", MapArgPublisher.class.getName(),
            "client.metrics-publisher.test", "ok",
            "client.region", "us-east-1"));
    try (final var s3 = factory.s3Async()) {
      assertThat(MapArgPublisher.INSTANCE.get().args.get("test")).isEqualTo("ok");
    } finally {
      NoArgPublisher.INSTANCE.remove();
    }
  }

  public static class TestCredentialProvider implements AwsCredentialsProvider {
    public static AwsCredentialsProvider create() {
      return new TestCredentialProvider();
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return AwsBasicCredentials.create("a", "b");
    }
  }

  public static class NoArgPublisher implements MetricPublisher {
    private static final ThreadLocal<NoArgPublisher> INSTANCE = new ThreadLocal<>();

    private NoArgPublisher() {
      INSTANCE.set(this);
    }

    public static NoArgPublisher create() {
      return new NoArgPublisher();
    }

    @Override
    public void publish(final MetricCollection metricCollection) {
      // no-op
    }

    @Override
    public void close() {
      // no-op
    }
  }

  public static class MapArgPublisher implements MetricPublisher {
    private static final ThreadLocal<MapArgPublisher> INSTANCE = new ThreadLocal<>();

    private final Map<String, String> args;

    private MapArgPublisher(final Map<String, String> args) {
      INSTANCE.set(this);
      this.args = args;
    }

    public static MapArgPublisher create(Map<String, String> args) {
      return new MapArgPublisher(args);
    }

    @Override
    public void publish(final MetricCollection metricCollection) {
      // no-op
    }

    @Override
    public void close() {
      // no-op
    }
  }
}
