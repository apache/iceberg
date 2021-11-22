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
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * It's used for integration test.
 */
public class OSSIntegrationTestRule implements AliyunOSSTestRule {
  private String endpoint;
  private String accessKey;
  private String accessSecret;
  private String testBucketName;
  private String keyPrefix;

  private OSS lazyClient = null;

  @Override
  public String testBucketName() {
    return testBucketName;
  }

  @Override
  public String keyPrefix() {
    return keyPrefix;
  }

  @Override
  public void start() {
    endpoint = AliyunTestUtility.testOSSEndpoint();
    accessKey = AliyunTestUtility.testAccessKeyId();
    accessSecret = AliyunTestUtility.testAccessKeySecret();
    testBucketName = AliyunTestUtility.testBucketName();
    keyPrefix = AliyunTestUtility.testOssKeyPrefix();
    if (keyPrefix == null) {
      keyPrefix = String.format("iceberg-oss-testing-%s", UUID.randomUUID());
    }
  }

  @Override
  public void stop() {
    if (lazyClient != null) {
      lazyClient.shutdown();
    }
  }

  @Override
  public OSS createOSSClient() {
    Preconditions.checkNotNull(endpoint, "OSS endpoint cannot be null");
    Preconditions.checkNotNull(accessKey, "OSS access key cannot be null");
    Preconditions.checkNotNull(accessSecret, "OSS access secret cannot be null");

    return new OSSClientBuilder().build(endpoint, accessKey, accessSecret);
  }

  @Override
  public void setUpBucket(String bucket) {
    Preconditions.checkArgument(
        ossClient().doesBucketExist(bucket),
        "Bucket %s does not exist, please create it firstly.", bucket);
  }

  @Override
  public void tearDownBucket(String bucket) {
    ObjectListing objectListing = ossClient().listObjects(
        new ListObjectsRequest(bucket)
            .withPrefix(keyPrefix)
    );

    for (OSSObjectSummary s : objectListing.getObjectSummaries()) {
      ossClient().deleteObject(bucket, s.getKey());
    }
  }

  private OSS ossClient() {
    if (lazyClient == null) {
      synchronized (OSSIntegrationTestRule.class) {
        if (lazyClient == null) {
          lazyClient = createOSSClient();
        }
      }
    }

    return lazyClient;
  }
}
