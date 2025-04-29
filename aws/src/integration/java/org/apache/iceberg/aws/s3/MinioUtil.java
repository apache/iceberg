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
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class MinioUtil {

  private MinioUtil() {}

  public static MinIOContainer createContainer() {
    return createContainer(null);
  }

  public static MinIOContainer createContainer(AwsCredentials credentials) {
    var container = new MinIOContainer(DockerImageName.parse("minio/minio:latest"));

    // this enables virtual-host-style requests. see
    // https://github.com/minio/minio/tree/master/docs/config#domain
    container.withEnv("MINIO_DOMAIN", "localhost");

    if (credentials != null) {
      container.withUserName(credentials.accessKeyId());
      container.withPassword(credentials.secretAccessKey());
    }

    return container;
  }

  public static S3Client createS3Client(MinIOContainer container) {
    URI uri = URI.create(container.getS3URL());
    S3ClientBuilder builder = S3Client.builder();
    builder.credentialsProvider(
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(container.getUserName(), container.getPassword())));
    builder.applyMutation(mutator -> mutator.endpointOverride(uri));
    builder.region(Region.US_EAST_1);
    builder.forcePathStyle(true); // OSX won't resolve subdomains
    return builder.build();
  }
}
