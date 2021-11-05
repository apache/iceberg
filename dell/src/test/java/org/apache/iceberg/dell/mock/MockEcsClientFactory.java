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

package org.apache.iceberg.dell.mock;

import com.emc.object.s3.S3Client;
import java.util.Map;
import org.apache.iceberg.dell.EcsClientFactory;
import org.apache.iceberg.dell.EcsClientProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class MockEcsClientFactory implements EcsClientFactory {

  public static final Map<String, String> MOCK_ECS_CLIENT_PROPERTIES = ImmutableMap.of(
      EcsClientProperties.ECS_CLIENT_FACTORY,
      MockEcsClientFactory.class.getName()
  );

  @Override
  public S3Client createS3Client(Map<String, String> properties) {
    return new MockS3Client();
  }
}
