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
import org.apache.iceberg.dell.DellClientFactory;
import org.apache.iceberg.dell.mock.ecs.EcsS3MockRule;

/** Provide client which initialized by {@link EcsS3MockRule} */
public class MockDellClientFactory implements DellClientFactory {

  public static final String ID_KEY = "mock.dell.client.factory.id";

  /** Use ID to avoid using client in other instance. */
  private String id;

  @Override
  public S3Client ecsS3() {
    return EcsS3MockRule.rule(id).client();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.id = properties.get(ID_KEY);
  }
}
