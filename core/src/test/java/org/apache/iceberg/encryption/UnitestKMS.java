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
package org.apache.iceberg.encryption;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class UnitestKMS extends MemoryMockKMS {
  public static final String MASTER_KEY_NAME1 = "keyA";
  public static final byte[] MASTER_KEY1 = "0123456789012345".getBytes(StandardCharsets.UTF_8);
  public static final String MASTER_KEY_NAME2 = "keyB";
  public static final byte[] MASTER_KEY2 = "1123456789012345".getBytes(StandardCharsets.UTF_8);

  @Override
  public void initialize(Map<String, String> properties) {
    masterKeys =
        ImmutableMap.of(
            MASTER_KEY_NAME1, MASTER_KEY1,
            MASTER_KEY_NAME2, MASTER_KEY2);
  }
}
