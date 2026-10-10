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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class ObjectStoreUtil {
  private static final String IMAGE = "rustfs/rustfs:1.0.0";
  private static final int S3_PORT = 9000;
  private static final String ACCESS_KEY_ENV = "RUSTFS_ACCESS_KEY";
  private static final String SECRET_KEY_ENV = "RUSTFS_SECRET_KEY";
  private static final AwsCredentials DEFAULT_CREDENTIALS =
      AwsBasicCredentials.create("admin", "password");

  private ObjectStoreUtil() {}

  public static GenericContainer<?> createContainer() {
    return createContainer(DEFAULT_CREDENTIALS);
  }

  public static GenericContainer<?> createContainer(AwsCredentials credentials) {
    var container = new GenericContainer<>(DockerImageName.parse(IMAGE));
    container.withExposedPorts(S3_PORT);
    container.withEnv(ACCESS_KEY_ENV, credentials.accessKeyId());
    container.withEnv(SECRET_KEY_ENV, credentials.secretAccessKey());
    container.withEnv("RUSTFS_OBS_LOG_STDOUT_ENABLED", "true");
    container.waitingFor(Wait.forHttp("/health/ready").forPort(S3_PORT));
    return container;
  }

  public static URI endpoint(GenericContainer<?> container) {
    return URI.create("http://" + container.getHost() + ":" + container.getMappedPort(S3_PORT));
  }

  public static S3Client createS3Client(GenericContainer<?> container) {
    URI uri = endpoint(container);
    S3ClientBuilder builder = S3Client.builder();
    builder.credentialsProvider(
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                container.getEnvMap().get(ACCESS_KEY_ENV),
                container.getEnvMap().get(SECRET_KEY_ENV))));
    builder.applyMutation(mutator -> mutator.endpointOverride(uri));
    builder.region(Region.US_EAST_1);
    builder.forcePathStyle(true); // OSX won't resolve subdomains
    return builder.build();
  }
}
