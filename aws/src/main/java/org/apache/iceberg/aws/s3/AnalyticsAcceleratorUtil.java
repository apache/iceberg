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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.util.Pair;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.s3.analyticsaccelerator.ObjectClientConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

class AnalyticsAcceleratorUtil {

  private static final Cache<Pair<S3AsyncClient, S3FileIOProperties>, S3SeekableInputStreamFactory>
      STREAM_FACTORY_CACHE = Caffeine.newBuilder().maximumSize(100).build();

  private AnalyticsAcceleratorUtil() {}

  public static SeekableInputStream newStream(S3InputFile inputFile) {
    S3URI uri = S3URI.of(inputFile.uri().bucket(), inputFile.uri().key());
    HeadObjectResponse metadata = inputFile.getObjectMetadata();
    OpenStreamInformation openStreamInfo =
        OpenStreamInformation.builder()
            .objectMetadata(
                ObjectMetadata.builder()
                    .contentLength(metadata.contentLength())
                    .etag(metadata.eTag())
                    .build())
            .build();

    S3SeekableInputStreamFactory factory =
        STREAM_FACTORY_CACHE.get(
            Pair.of(inputFile.asyncClient(), inputFile.s3FileIOProperties()),
            AnalyticsAcceleratorUtil::createNewFactory);

    try {
      S3SeekableInputStream seekableInputStream = factory.createStream(uri, openStreamInfo);
      return new AnalyticsAcceleratorInputStreamWrapper(seekableInputStream);
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to create S3 analytics accelerator input stream for: %s", inputFile.uri());
    }
  }

  private static S3SeekableInputStreamFactory createNewFactory(
      Pair<S3AsyncClient, S3FileIOProperties> cacheKey) {
    ConnectorConfiguration connectorConfiguration =
        new ConnectorConfiguration(cacheKey.second().s3AnalyticsacceleratorProperties());
    S3SeekableInputStreamConfiguration streamConfiguration =
        S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration);
    ObjectClientConfiguration objectClientConfiguration =
        ObjectClientConfiguration.fromConfiguration(connectorConfiguration);

    ObjectClient objectClient = new S3SdkObjectClient(cacheKey.first(), objectClientConfiguration);
    return new S3SeekableInputStreamFactory(objectClient, streamConfiguration);
  }

  public static void cleanupCache(
      S3AsyncClient asyncClient, S3FileIOProperties s3FileIOProperties) {
    STREAM_FACTORY_CACHE.invalidate(Pair.of(asyncClient, s3FileIOProperties));
  }
}
