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
import java.util.UUID;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * API for test Aliyun Object Storage Service (OSS) which is either local mock http server or remote
 * aliyun oss server
 *
 * <p>This API includes start,stop OSS service, create OSS client, setup bucket and teardown bucket.
 */
public interface AliyunOSSExtension extends BeforeAllCallback, AfterAllCallback {
  UUID RANDOM_UUID = java.util.UUID.randomUUID();

  /** Returns a specific bucket name for testing purpose. */
  default String testBucketName() {
    return String.format("oss-testing-bucket-%s", RANDOM_UUID);
  }

  @Override
  default void afterAll(ExtensionContext context) throws Exception {
    stop();
  }

  @Override
  default void beforeAll(ExtensionContext context) throws Exception {
    start();
  }

  /**
   * Returns the common key prefix for those newly created objects in test cases. For example, we
   * set the test bucket to be 'oss-testing-bucket' and the key prefix to be 'iceberg-objects/',
   * then the produced objects in test cases will be:
   *
   * <pre>
   *   oss://oss-testing-bucket/iceberg-objects/a.dat
   *   oss://oss-testing-bucket/iceberg-objects/b.dat
   *   ...
   * </pre>
   */
  String keyPrefix();

  /** Start the Aliyun Object storage services application that the OSS client could connect to. */
  void start();

  /** Stop the Aliyun object storage services. */
  void stop();

  /** Returns an newly created {@link OSS} client. */
  OSS createOSSClient();

  /**
   * Preparation work of bucket for the test case, for example we need to check the existence of
   * specific bucket.
   */
  void setUpBucket(String bucket);

  /** Clean all the objects that created from this test suite in the bucket. */
  void tearDownBucket(String bucket);
}
