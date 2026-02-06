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
package org.apache.iceberg.dell.ecs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestPropertiesSerDesUtil {

  @Test
  public void testPropertiesSerDes() {
    Map<String, String> properties = ImmutableMap.of("a", "a", "b", "b");
    byte[] byteValue = PropertiesSerDesUtil.toBytes(properties);
    Map<String, String> result =
        PropertiesSerDesUtil.read(byteValue, PropertiesSerDesUtil.currentVersion());
    assertThat(properties).as("Ser/Des will return the same content.").isEqualTo(result);
  }
}
