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

import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.metrics.MetricsContext;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

abstract class BaseS3File {
  private final S3Client client;
  private final S3InputStreamFactory inputStreamFactory;
  private final S3URI uri;
  private final S3FileIOProperties s3FileIOProperties;
  private HeadObjectResponse metadata;
  private final MetricsContext metrics;

  BaseS3File(
      S3Client client,
      S3InputStreamFactory inputStreamFactory,
      S3URI uri,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    this.client = client;
    this.inputStreamFactory = inputStreamFactory;
    this.uri = uri;
    this.s3FileIOProperties = s3FileIOProperties;
    this.metrics = metrics;
  }

  public String location() {
    return uri.location();
  }

  S3Client client() {
    return client;
  }

  S3InputStreamFactory inputStreamFactory() {
    return inputStreamFactory;
  }

  S3URI uri() {
    return uri;
  }

  public S3FileIOProperties s3FileIOProperties() {
    return s3FileIOProperties;
  }

  protected MetricsContext metrics() {
    return metrics;
  }

  /**
   * Note: this may be stale if file was deleted since metadata is cached for size/existence checks.
   *
   * @return flag
   */
  public boolean exists() {
    try {
      return getObjectMetadata() != null;
    } catch (NotFoundException e) {
      return false;
    }
  }

  protected HeadObjectResponse getObjectMetadata() throws S3Exception {
    try {
      if (metadata == null) {
        HeadObjectRequest.Builder requestBuilder =
            HeadObjectRequest.builder().bucket(uri().bucket()).key(uri().key());
        S3RequestUtil.configureEncryption(s3FileIOProperties, requestBuilder);
        metadata = client().headObject(requestBuilder.build());
      }
      return metadata;
    } catch (NoSuchKeyException e) {
      throw new NotFoundException(e, "Location does not exist: %s", uri().toString());
    }
  }

  @Override
  public String toString() {
    return uri.toString();
  }
}
