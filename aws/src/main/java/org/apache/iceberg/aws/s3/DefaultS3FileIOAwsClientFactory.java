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
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import software.amazon.awssdk.services.s3.S3Client;

class DefaultS3FileIOAwsClientFactory implements S3FileIOAwsClientFactory {
  private S3FileIOProperties s3FileIOProperties;
  private HttpClientProperties httpClientProperties;
  private AwsClientProperties awsClientProperties;

  DefaultS3FileIOAwsClientFactory() {
    this.s3FileIOProperties = new S3FileIOProperties();
    this.httpClientProperties = new HttpClientProperties();
    this.awsClientProperties = new AwsClientProperties();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.s3FileIOProperties = new S3FileIOProperties(properties);
    this.awsClientProperties = new AwsClientProperties(properties);
    this.httpClientProperties = new HttpClientProperties(properties);
  }

  @Override
  public S3Client s3() {
    return S3Client.builder()
        .applyMutation(awsClientProperties::applyClientRegionConfiguration)
        .applyMutation(httpClientProperties::applyHttpClientConfigurations)
        .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
        .applyMutation(s3FileIOProperties::applyServiceConfigurations)
        .applyMutation(
            s3ClientBuilder ->
                s3FileIOProperties.applyCredentialConfigurations(
                    awsClientProperties, s3ClientBuilder))
        .applyMutation(s3FileIOProperties::applySignerConfiguration)
        .applyMutation(s3FileIOProperties::applyS3AccessGrantsConfigurations)
        .build();
  }
}
