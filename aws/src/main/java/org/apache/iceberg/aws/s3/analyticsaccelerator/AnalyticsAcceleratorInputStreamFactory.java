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

import java.io.IOException;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.aws.s3.S3InputStreamFactory;
import org.apache.iceberg.aws.s3.S3URI;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

public class AnalyticsAcceleratorInputStreamFactory implements S3InputStreamFactory {
  private final S3SeekableInputStreamFactory seekableInputStreamFactory;

  public AnalyticsAcceleratorInputStreamFactory(
      S3SeekableInputStreamFactory seekableInputStreamFactory) {
    this.seekableInputStreamFactory = seekableInputStreamFactory;
  }

  @Override
  public SeekableInputStream createStream(
      S3Client client,
      S3URI uri,
      S3FileIOProperties properties,
      MetricsContext metrics,
      OpenStreamInformation openStreamInformation) {
    try {
      return new AnalyticsAcceleratorInputStream(
          seekableInputStreamFactory.createStream(
              software.amazon.s3.analyticsaccelerator.util.S3URI.of(uri.bucket(), uri.key()),
              openStreamInformation));
    } catch (IOException e) {
      throw new RuntimeIOException(
          "Failed to create analytics accelerator input stream for bucket: %s, key: %s - %s",
          uri.bucket(), uri.key(), e);
    }
  }
}
