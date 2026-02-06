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
package org.apache.iceberg.aliyun.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObjectSummary;
import org.apache.iceberg.aliyun.TestUtility;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class OSSIntegrationExtension implements AliyunOSSExtension {
  // Aliyun access key pair.
  private String accessKeyId;
  private String accessKeySecret;

  // Aliyun OSS configure values.
  private String ossEndpoint;
  private String ossBucket;
  private String ossKey;

  private volatile OSS lazyClient = null;

  @Override
  public String testBucketName() {
    return ossBucket;
  }

  @Override
  public String keyPrefix() {
    return ossKey;
  }

  @Override
  public void start() {
    this.accessKeyId = TestUtility.accessKeyId();
    this.accessKeySecret = TestUtility.accessKeySecret();

    this.ossEndpoint = TestUtility.ossEndpoint();
    this.ossBucket = TestUtility.ossBucket();
    this.ossKey = TestUtility.ossKey();
  }

  @Override
  public void stop() {
    if (lazyClient != null) {
      lazyClient.shutdown();
      lazyClient = null;
    }
  }

  @Override
  public OSS createOSSClient() {
    Preconditions.checkNotNull(ossEndpoint, "OSS endpoint cannot be null");
    Preconditions.checkNotNull(accessKeyId, "OSS access key id cannot be null");
    Preconditions.checkNotNull(accessKeySecret, "OSS access secret cannot be null");

    return new OSSClientBuilder().build(ossEndpoint, accessKeyId, accessKeySecret);
  }

  @Override
  public void setUpBucket(String bucket) {
    Preconditions.checkArgument(
        ossClient().doesBucketExist(bucket),
        "Bucket %s does not exist, please create it firstly.",
        bucket);
  }

  @Override
  public void tearDownBucket(String bucket) {
    int maxKeys = 200;
    String nextContinuationToken = null;
    ListObjectsV2Result objectListingResult;
    do {
      ListObjectsV2Request listObjectsV2Request =
          new ListObjectsV2Request(bucket)
              .withMaxKeys(maxKeys)
              .withPrefix(ossKey)
              .withContinuationToken(nextContinuationToken);
      objectListingResult = ossClient().listObjectsV2(listObjectsV2Request);

      for (OSSObjectSummary s : objectListingResult.getObjectSummaries()) {
        ossClient().deleteObject(bucket, s.getKey());
      }

      nextContinuationToken = objectListingResult.getNextContinuationToken();
    } while (objectListingResult.isTruncated());
  }

  private OSS ossClient() {
    if (lazyClient == null) {
      synchronized (OSSIntegrationExtension.class) {
        if (lazyClient == null) {
          lazyClient = createOSSClient();
        }
      }
    }

    return lazyClient;
  }
}
