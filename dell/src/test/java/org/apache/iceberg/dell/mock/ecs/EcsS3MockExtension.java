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
package org.apache.iceberg.dell.mock.ecs;

import static org.assertj.core.api.Assertions.assertThat;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.ObjectKey;
import com.emc.object.s3.request.DeleteObjectsRequest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.common.testutils.CustomExtension;
import org.apache.iceberg.dell.DellClientFactories;
import org.apache.iceberg.dell.DellProperties;
import org.apache.iceberg.dell.mock.MockDellClientFactory;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Extension of ECS S3 mock.
 *
 * <p>Use environment parameter to specify use mock client or real client.
 */
public class EcsS3MockExtension implements CustomExtension {

  /** Object ID generator */
  private static final AtomicInteger ID = new AtomicInteger(0);

  // Config fields
  private final boolean autoCreateBucket;

  // Setup fields
  private Map<String, String> clientProperties;
  private String bucket;
  private boolean mock;

  // State fields during test
  private S3Client client;
  private boolean bucketCreated;

  private static final ThreadLocal<EcsS3MockExtension> TEST_EXTENSION_FOR_MOCK_CLIENT =
      new ThreadLocal<>();

  private EcsS3MockExtension(boolean autoCreateBucket) {
    this.autoCreateBucket = autoCreateBucket;
  }

  public static EcsS3MockExtension create() {
    return new EcsS3MockExtension(true);
  }

  public static EcsS3MockExtension manualCreateBucket() {
    return new EcsS3MockExtension(false);
  }

  /** Load rule from thread local and check bucket */
  public static EcsS3MockExtension extension(String id) {
    EcsS3MockExtension rule = TEST_EXTENSION_FOR_MOCK_CLIENT.get();
    assertThat(rule).isNotNull().extracting(EcsS3MockExtension::bucket).isEqualTo(id);
    return rule;
  }

  @Override
  public void before(ExtensionContext context) {
    bucket = "test-" + UUID.randomUUID();
    if (System.getenv(DellProperties.ECS_S3_ENDPOINT) == null) {
      mock = true;
      Map<String, String> properties = new LinkedHashMap<>();
      properties.put(DellProperties.CLIENT_FACTORY, MockDellClientFactory.class.getName());
      properties.put(MockDellClientFactory.ID_KEY, bucket);
      clientProperties = properties;
      client = new MockS3Client();
      TEST_EXTENSION_FOR_MOCK_CLIENT.set(this);
    } else {
      mock = false;
      Map<String, String> properties = new LinkedHashMap<>();
      properties.put(
          DellProperties.ECS_S3_ACCESS_KEY_ID, System.getenv(DellProperties.ECS_S3_ACCESS_KEY_ID));
      properties.put(
          DellProperties.ECS_S3_SECRET_ACCESS_KEY,
          System.getenv(DellProperties.ECS_S3_SECRET_ACCESS_KEY));
      properties.put(DellProperties.ECS_S3_ENDPOINT, System.getenv(DellProperties.ECS_S3_ENDPOINT));
      clientProperties = properties;
      client = DellClientFactories.from(properties).ecsS3();
      if (autoCreateBucket) {
        createBucket();
      }
    }
  }

  @Override
  public void after(ExtensionContext context) {
    if (mock) {
      // clean up
      TEST_EXTENSION_FOR_MOCK_CLIENT.set(null);
    } else {
      if (bucketCreated) {
        deleteBucket();
      }
    }

    client().destroy();
  }

  public void createBucket() {
    // create test bucket for this unit test
    client().createBucket(bucket);
    bucketCreated = true;
  }

  private void deleteBucket() {
    if (!client().bucketExists(bucket)) {
      return;
    }

    // clean up test bucket of this unit test
    while (true) {
      ListObjectsResult result = client().listObjects(bucket);
      if (result.getObjects().isEmpty()) {
        break;
      }

      List<ObjectKey> keys =
          result.getObjects().stream()
              .map(it -> new ObjectKey(it.getKey()))
              .collect(Collectors.toList());
      client().deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keys));
    }

    client().deleteBucket(bucket);
  }

  public Map<String, String> clientProperties() {
    return clientProperties;
  }

  public S3Client client() {
    return client;
  }

  public String bucket() {
    return bucket;
  }

  public String randomObjectName() {
    return "test-" + ID.getAndIncrement() + "-" + UUID.randomUUID();
  }
}
