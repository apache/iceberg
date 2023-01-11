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
import java.io.IOException;
import java.util.UUID;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public interface HuaweicloudOBSTestRule extends TestRule {
  UUID RANDOM_UUID = UUID.randomUUID();

  /** Returns a specific bucket name for testing purpose. */
  default String testBucketName() {
    return String.format("obs-testing-bucket-%s", RANDOM_UUID);
  }

  @Override
  default Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        start();
        try {
          base.evaluate();
        } finally {
          stop();
        }
      }
    };
  }

  /**
   * Returns the common key prefix for those newly created objects in test cases. For example, we
   * set the test bucket to be 'obs-testing-bucket' and the key prefix to be 'iceberg-objects/',
   * then the produced objects in test cases will be:
   *
   * <pre>
   *   obs://obs-testing-bucket/iceberg-objects/a.dat
   *   obs://obs-testing-bucket/iceberg-objects/b.dat
   *   ...
   * </pre>
   */
  String keyPrefix();

  /**
   * Start the Huaweicloud Object storage services application that the OBS client could connect to.
   */
  void start();

  /** Stop the Huaweicloud object storage services. */
  void stop() throws IOException;

  /** Returns an newly created {@link ObsClient} client. */
  IObsClient createOBSClient();

  /**
   * Preparation work of bucket for the test case, for example we need to check the existence of
   * specific bucket.
   */
  void setUpBucket(String bucket);

  /** Clean all the objects that created from this test suite in the bucket. */
  void tearDownBucket(String bucket);
}
