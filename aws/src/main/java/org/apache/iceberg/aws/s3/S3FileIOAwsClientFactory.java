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
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.services.s3.S3Client;

public class S3FileIOAwsClientFactory {
  /**
   * This property allows customizing AWS client factory implementation class. If this property is
   * set, only {@link S3FileIOAwsClientFactory} class will be instantiated. If this property wasn't
   * set, will load one of {@link org.apache.iceberg.aws.AwsClientFactory} factory classes to
   * provide backward compatibility.
   */
  public static final String CLIENT_FACTORY = "s3.client-factory-impl";

  private final S3FileIOProperties s3FileIOProperties;
  private final HttpClientProperties httpClientProperties;
  private final AwsClientProperties awsClientProperties;

  S3FileIOAwsClientFactory(
      S3FileIOProperties s3FileIOProperties,
      HttpClientProperties httpClientProperties,
      AwsClientProperties awsClientProperties) {
    this.s3FileIOProperties = s3FileIOProperties;
    this.httpClientProperties = httpClientProperties;
    this.awsClientProperties = awsClientProperties;
  }

  /**
   * Returns an instance of a client factory class based on properties. If the {@value
   * #CLIENT_FACTORY} is set, will return an instance of type {@link S3FileIOAwsClientFactory}.
   * Otherwise, will return an instance of type {@link org.apache.iceberg.aws.AwsClientFactory} for
   * backward compatibility. To provide
   *
   * @param properties catalog properties
   * @param <T> a java return type
   * @return factory class object
   */
  @SuppressWarnings("unchecked")
  public static <T> T getS3ClientFactoryImpl(Map<String, String> properties) {
    boolean useS3FileIO = PropertyUtil.propertyAsBoolean(properties, CLIENT_FACTORY, false);
    if (useS3FileIO) {
      return (T)
          new S3FileIOAwsClientFactory(
              new S3FileIOProperties(properties),
              new HttpClientProperties(properties),
              new AwsClientProperties(properties));
    }
    return (T) AwsClientFactories.from(properties);
  }

  S3Client s3() {
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
        .build();
  }
}
