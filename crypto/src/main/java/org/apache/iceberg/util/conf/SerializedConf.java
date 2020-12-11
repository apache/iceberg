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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a wrapper for a {@code Map<String, String>} that gets serialized and deserialized into a byte
 * buffer.
 */
public class SerializedConf extends Conf {
  private static final Logger log = LoggerFactory.getLogger(SerializedConf.class);
  private final Map<String, String> innerMap;
  private final MapSerde serde;

  protected SerializedConf(MapSerde serde) {
    this(serde, new HashMap<>());
  }

  protected SerializedConf(MapSerde serde, ByteBuffer bytes) {
    this(serde, fromBytes(serde, bytes));
  }

  private SerializedConf(MapSerde serde, Map<String, String> innerMap) {
    super(""); // no namespace
    this.serde = serde;
    this.innerMap = innerMap;
  }

  @Override
  protected String get(String key) {
    return innerMap.get(key);
  }

  @Override
  protected void set(String key, String value) {
    innerMap.put(key, value);
  }

  private static Map<String, String> fromBytes(MapSerde serde, ByteBuffer bytes) {
    try {
      return serde.fromBytes(bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public byte[] toBytes() {
    try {
      return serde.toBytes(innerMap);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static SerializedConf of(MapSerde serde, ByteBuffer bytes) {
    return new SerializedConf(serde, bytes);
  }

  public static SerializedConf empty(MapSerde serde) {
    return new SerializedConf(serde);
  }
}
