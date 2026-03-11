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

import java.net.URI;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class SeaweedFSUtil {
  public static final String IMAGE = "chrislusf/seaweedfs";
  public static final String LATEST_TAG = "latest";
  public static final int S3_PORT = 8333;
  private static final String DEFAULT_ACCESS_KEY = "admin";
  private static final String DEFAULT_SECRET_KEY = "password";

  private SeaweedFSUtil() {}

  @SuppressWarnings("resource")
  public static GenericContainer<?> createContainer() {
    return createContainer(LATEST_TAG, null);
  }

  @SuppressWarnings("resource")
  public static GenericContainer<?> createContainer(String tag, AwsCredentials credentials) {
    String accessKey =
        credentials != null ? credentials.accessKeyId() : DEFAULT_ACCESS_KEY;
    String secretKey =
        credentials != null ? credentials.secretAccessKey() : DEFAULT_SECRET_KEY;

    GenericContainer<?> container =
        new GenericContainer<>(IMAGE + ":" + tag)
            .withExposedPorts(S3_PORT)
            .withCommand("mini -dir=/data")
            .withEnv("AWS_ACCESS_KEY_ID", accessKey)
            .withEnv("AWS_SECRET_ACCESS_KEY", secretKey)
            .waitingFor(
                new HttpWaitStrategy()
                    .forPort(S3_PORT)
                    .forPath("/status")
                    .forStatusCode(200));

    return container;
  }

  public static String getS3URL(GenericContainer<?> container) {
    return String.format(
        "http://%s:%d", container.getHost(), container.getMappedPort(S3_PORT));
  }

  public static S3Client createS3Client(GenericContainer<?> container) {
    return createS3Client(container, false);
  }

  public static S3Client createS3Client(
      GenericContainer<?> container, boolean legacyMd5PluginEnabled) {
    URI uri = URI.create(getS3URL(container));
    String accessKey =
        container.getEnvMap().getOrDefault("AWS_ACCESS_KEY_ID", DEFAULT_ACCESS_KEY);
    String secretKey =
        container.getEnvMap().getOrDefault("AWS_SECRET_ACCESS_KEY", DEFAULT_SECRET_KEY);

    S3ClientBuilder builder = S3Client.builder();
    if (legacyMd5PluginEnabled) {
      builder.addPlugin(LegacyMd5Plugin.create());
    }
    builder.credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
    builder.applyMutation(mutator -> mutator.endpointOverride(uri));
    builder.region(Region.US_EAST_1);
    builder.forcePathStyle(true); // OSX won't resolve subdomains
    return builder.build();
  }
}
