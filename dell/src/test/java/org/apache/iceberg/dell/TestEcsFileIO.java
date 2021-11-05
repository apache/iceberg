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

package org.apache.iceberg.dell;

import java.util.Map;
import org.apache.iceberg.dell.mock.EcsS3MockRule;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class TestEcsFileIO {

  @Rule
  public EcsS3MockRule rule = EcsS3MockRule.manualCreateBucket();

  @Test
  public void testEcsFileIOSerializationRoundTrip() {
    try (EcsFileIO instance1 = new EcsFileIO()) {
      Map<String, String> input = ImmutableMap.<String, String>builder()
          .putAll(rule.clientProperties())
          .put("key1", "value1")
          .put("key2", "value2")
          .build();
      instance1.initialize(input);
      try (EcsFileIO instance2 = SerializationUtil.deserializeFromBytes(
          SerializationUtil.serializeToBytes(instance1))) {
        Assert.assertEquals("The properties should be equals", instance1.properties(), instance2.properties());
        Assert.assertNotSame("Client instance is different", instance1.client(), instance2.client());
      }
    }
  }
}
