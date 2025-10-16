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
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.ManagedHttpClientRegistry;
import org.apache.iceberg.aws.S3FileIOAwsClientFactories;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

class PrefixedS3Client implements AutoCloseable {

  private final String storagePrefix;
  private final S3FileIOProperties s3FileIOProperties;
  private SerializableSupplier<S3Client> s3;
  private SerializableSupplier<S3AsyncClient> s3Async;
  private transient volatile S3Client s3Client;
  private transient volatile S3AsyncClient s3AsyncClient;
  private transient String httpClientKey;
  private transient S3FileIOAwsClientFactory clientFactory;

  PrefixedS3Client(
      String storagePrefix,
      Map<String, String> properties,
      SerializableSupplier<S3Client> s3,
      SerializableSupplier<S3AsyncClient> s3Async) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(storagePrefix), "Invalid storage prefix: null or empty");
    Preconditions.checkArgument(null != properties, "Invalid properties: null");
    this.storagePrefix = storagePrefix;
    this.s3 = s3;
    this.s3Async = s3Async;
    this.s3FileIOProperties = new S3FileIOProperties(properties);
    // Do not override s3 client if it was provided
    if (s3 == null) {
      Object factory = S3FileIOAwsClientFactories.initialize(properties);
      if (factory instanceof S3FileIOAwsClientFactory) {
        this.clientFactory = (S3FileIOAwsClientFactory) factory;
        this.s3 = clientFactory::s3;
      } else if (factory instanceof AwsClientFactory) {
        this.s3 = ((AwsClientFactory) factory)::s3;
      }
      if (s3FileIOProperties.isPreloadClientEnabled()) {
        s3();
      }
    }

    // Do not override s3Async client if it was provided
    if (s3Async == null) {
      Object asyncFactory = S3FileIOAwsClientFactories.initialize(properties);
      if (asyncFactory instanceof S3FileIOAwsClientFactory) {
        this.s3Async = ((S3FileIOAwsClientFactory) asyncFactory)::s3Async;
      }
      if (asyncFactory instanceof AwsClientFactory) {
        this.s3Async = ((AwsClientFactory) asyncFactory)::s3Async;
      }
    }
  }

  public String storagePrefix() {
    return storagePrefix;
  }

  public S3Client s3() {
    if (s3Client == null) {
      synchronized (this) {
        if (s3Client == null) {
          s3Client = s3.get();
          // Capture the HTTP client key after the client is created
          if (clientFactory != null && httpClientKey == null) {
            httpClientKey = clientFactory.httpClientKey();
          }
        }
      }
    }

    return s3Client;
  }

  public S3AsyncClient s3Async() {
    if (s3AsyncClient == null) {
      synchronized (this) {
        if (s3AsyncClient == null) {
          s3AsyncClient = s3Async.get();
        }
      }
    }

    return s3AsyncClient;
  }

  public S3FileIOProperties s3FileIOProperties() {
    return s3FileIOProperties;
  }

  String httpClientKey() {
    return httpClientKey;
  }

  @Override
  public void close() {
    if (null != s3Client) {
      s3Client.close();
    }

    if (null != s3AsyncClient) {
      // cleanup usage in analytics accelerator if any
      if (s3FileIOProperties().isS3AnalyticsAcceleratorEnabled()) {
        AnalyticsAcceleratorUtil.cleanupCache(s3AsyncClient, s3FileIOProperties);
      }
      s3AsyncClient.close();
    }

    // Release the HTTP client from the registry
    if (httpClientKey != null) {
      ManagedHttpClientRegistry.getInstance().releaseClient(httpClientKey);
    }
  }
}
