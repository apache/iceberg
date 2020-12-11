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

package org.apache.iceberg.util.conf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.SerializableUtil.deserialize;
import static org.apache.iceberg.SerializableUtil.serialize;

public class TestJsonMapSerde {
  String jsonString;
  Map<String, String> map;
  MapSerde serde;

  @Before
  public void before() {
    map = new HashMap<>();
    map.put("hello", "World");
    map.put("foo", "baR");
    jsonString = "{\"foo\":\"baR\", \"hello\":     \"World\"  }"; // supposed to be wonky
    serde = JsonMapSerde.INSTANCE;
  }

  @Test
  public void serializedDeserialize() throws IOException {
    byte[] bytes = serde.toBytes(map);
    Map<String, String> actual = serde.fromBytes(ByteBuffer.wrap(bytes));
    Assert.assertEquals(map, actual);
  }

  @Test
  public void fromString() throws IOException {
    byte[] bytes = jsonString.getBytes(StandardCharsets.UTF_8);
    Map<String, String> actual = serde.fromBytes(ByteBuffer.wrap(bytes));
    Assert.assertEquals(map, actual);
  }

  @Test
  public void testSerializable()
      throws IOException, CloneNotSupportedException, ClassNotFoundException {
    byte[] serialized = serialize(serde);
    Object deserialized = deserialize(serialized);
  }
}
