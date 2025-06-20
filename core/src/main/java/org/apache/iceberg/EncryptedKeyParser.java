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
package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.encryption.BaseEncryptedKey;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.JsonUtil;

public class EncryptedKeyParser {
  private EncryptedKeyParser() {}

  private static final String KEY_ID = "key-id";
  private static final String KEY_METADATA = "encrypted-key-metadata";
  private static final String ENCRYPTED_BY_ID = "encrypted-by-id";
  private static final String PROPERTIES = "properties";

  public static String toJson(EncryptedKey key) {
    return toJson(key, true);
  }

  public static String toJson(EncryptedKey key, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(key, gen), pretty);
  }

  static void toJson(EncryptedKey key, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(KEY_ID, key.keyId());
    generator.writeStringField(
        KEY_METADATA,
        Base64.getEncoder().encodeToString(ByteBuffers.toByteArray(key.encryptedKeyMetadata())));

    JsonUtil.writeStringFieldIfPresent(ENCRYPTED_BY_ID, key.encryptedById(), generator);

    if (key.properties() != null && !key.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, key.properties(), generator);
    }

    generator.writeEndObject();
  }

  public static EncryptedKey fromJson(String json) {
    return JsonUtil.parse(json, EncryptedKeyParser::fromJson);
  }

  static EncryptedKey fromJson(JsonNode node) {
    String keyId = JsonUtil.getString(KEY_ID, node);
    ByteBuffer keyMetadata =
        ByteBuffer.wrap(Base64.getDecoder().decode(JsonUtil.getString(KEY_METADATA, node)));
    String encryptedById = JsonUtil.getStringOrNull(ENCRYPTED_BY_ID, node);

    Map<String, String> properties;
    if (node.has(PROPERTIES)) {
      properties = JsonUtil.getStringMap(PROPERTIES, node);
    } else {
      properties = ImmutableMap.of();
    }

    return new BaseEncryptedKey(keyId, keyMetadata, encryptedById, properties);
  }
}
