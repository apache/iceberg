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
package org.apache.iceberg.huaweicloud.obs;

import com.obs.services.IObsClient;
import com.obs.services.ObsClient;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import java.io.IOException;
import org.apache.iceberg.huaweicloud.TestUtility;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class OBSIntegrationTestRule implements HuaweicloudOBSTestRule {
  // Huaweicloud access key pair.
  private String accessKeyId;
  private String accessKeySecret;

  // Huaweicloud OBS configure values.
  private String obsEndpoint;
  private String obsBucket;
  private String obsKey;

  private volatile IObsClient lazyClient = null;

  @Override
  public String testBucketName() {
    return obsBucket;
  }

  @Override
  public String keyPrefix() {
    return obsKey;
  }

  @Override
  public void start() {
    this.accessKeyId = TestUtility.accessKeyId();
    this.accessKeySecret = TestUtility.accessKeySecret();

    this.obsEndpoint = TestUtility.obsEndpoint();
    this.obsBucket = TestUtility.obsBucket();
    this.obsKey = TestUtility.obsKey();
  }

  @Override
  public void stop() throws IOException {
    if (lazyClient != null) {
      lazyClient.close();
      lazyClient = null;
    }
  }

  @Override
  public IObsClient createOBSClient() {
    Preconditions.checkNotNull(obsEndpoint, "OBS endpoint cannot be null");
    Preconditions.checkNotNull(accessKeyId, "OBS access key id cannot be null");
    Preconditions.checkNotNull(accessKeySecret, "OBS access secret cannot be null");

    return new ObsClient(accessKeyId, accessKeySecret, obsEndpoint);
  }

  @Override
  public void setUpBucket(String bucket) {
    Preconditions.checkArgument(
        obsClient().headBucket(bucket),
        "Bucket %s does not exist, please create it firstly.",
        bucket);
  }

  @Override
  public void tearDownBucket(String bucket) {
    int maxKeys = 200;
    String nextContinuationToken = null;
    ObjectListing objectListingResult;
    do {
      ListObjectsRequest listObjectsV2Request = new ListObjectsRequest(bucket);
      listObjectsV2Request.setMaxKeys(maxKeys);
      listObjectsV2Request.setPrefix(obsKey);
      objectListingResult = obsClient().listObjects(listObjectsV2Request);
      for (ObsObject s : objectListingResult.getObjects()) {
        obsClient().deleteObject(bucket, s.getObjectKey());
      }
    } while (objectListingResult.isTruncated());
  }

  private IObsClient obsClient() {
    if (lazyClient == null) {
      synchronized (OBSIntegrationTestRule.class) {
        if (lazyClient == null) {
          lazyClient = createOBSClient();
        }
      }
    }

    return lazyClient;
  }
}
