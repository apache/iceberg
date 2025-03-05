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
  private final SerializableSupplier<S3AsyncClient> s3Async;

  public S3SeekableInputStreamFactorySupplier(
      S3FileIOProperties s3FileIOProperties, SerializableSupplier<S3AsyncClient> s3Async) {
    this.s3FileIOProperties = s3FileIOProperties;
    this.s3Async = s3Async;
  }

  @Override
  public S3SeekableInputStreamFactory get() {
    ConnectorConfiguration connectorConfiguration =
        new ConnectorConfiguration(s3FileIOProperties.s3AnalyticsacceleratorProperties());
    S3SeekableInputStreamConfiguration streamConfiguration =
        S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration);
    ObjectClientConfiguration objectClientConfiguration =
        ObjectClientConfiguration.fromConfiguration(connectorConfiguration);

    ObjectClient objectClient = new S3SdkObjectClient(s3Async.get(), objectClientConfiguration);
    return new S3SeekableInputStreamFactory(objectClient, streamConfiguration);
  }
}
