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
package org.apache.iceberg.aws.s3.analyticsaccelerator;

import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.S3FileIOAwsClientFactories;
import org.apache.iceberg.aws.s3.DefaultS3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.s3.analyticsaccelerator.ObjectClientConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;

public class S3SeekableInputStreamFactorySupplier
    implements SerializableSupplier<S3SeekableInputStreamFactory> {

  private final S3FileIOProperties s3FileIOProperties;

  public S3SeekableInputStreamFactorySupplier(S3FileIOProperties s3FileIOProperties) {
    this.s3FileIOProperties = s3FileIOProperties;
  }

  @Override
  public S3SeekableInputStreamFactory get() {
    ConnectorConfiguration connectorConfiguration =
        new ConnectorConfiguration(s3FileIOProperties.s3AnalyticsacceleratorProperties());
    S3SeekableInputStreamConfiguration streamConfiguration =
        S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration);
    ObjectClientConfiguration objectClientConfiguration =
        ObjectClientConfiguration.fromConfiguration(connectorConfiguration);

    ObjectClient objectClient =
        new S3SdkObjectClient(createAsyncClient(), objectClientConfiguration);
    return new S3SeekableInputStreamFactory(objectClient, streamConfiguration);
  }

  private S3AsyncClient createAsyncClient() {
    Object clientFactory = S3FileIOAwsClientFactories.initialize(s3FileIOProperties.toMap());
    SerializableSupplier<S3AsyncClient> clientSupplier;
    if (clientFactory instanceof S3FileIOAwsClientFactory) {
      clientSupplier = ((S3FileIOAwsClientFactory) clientFactory)::s3Async;
    } else if (clientFactory instanceof AwsClientFactory) {
      clientSupplier = ((AwsClientFactory) clientFactory)::s3Async;
    } else {
      clientSupplier = () -> new DefaultS3FileIOAwsClientFactory().s3Async();
    }
    return clientSupplier.get();
  }
}
