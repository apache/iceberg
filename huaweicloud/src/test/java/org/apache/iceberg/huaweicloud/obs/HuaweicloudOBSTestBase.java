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
import org.apache.iceberg.huaweicloud.TestUtility;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

public abstract class HuaweicloudOBSTestBase {
  @ClassRule public static final HuaweicloudOBSTestRule OBS_TEST_RULE = TestUtility.initialize();

  private final SerializableSupplier<IObsClient> obsClient = OBS_TEST_RULE::createOBSClient;
  private final String bucketName = OBS_TEST_RULE.testBucketName();
  private final String keyPrefix = OBS_TEST_RULE.keyPrefix();

  @Before
  public void before() {
    OBS_TEST_RULE.setUpBucket(bucketName);
  }

  @After
  public void after() {
    OBS_TEST_RULE.tearDownBucket(bucketName);
  }

  protected String location(String key) {
    return String.format("obs://%s/%s%s", bucketName, keyPrefix, key);
  }

  protected SerializableSupplier<IObsClient> obsClient() {
    return obsClient;
  }
}
