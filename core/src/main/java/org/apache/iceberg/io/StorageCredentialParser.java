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
package org.apache.iceberg.io;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class StorageCredentialParser {
  private static final String PREFIX = "prefix";
  private static final String CONFIG = "config";

  private StorageCredentialParser() {}

  static String toJson(StorageCredential credential) {
    return toJson(credential, false);
  }

  static String toJson(StorageCredential credential, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(credential, gen), pretty);
  }

  static void toJson(StorageCredential credential, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(credential != null, "Invalid storage credential: null");

    gen.writeStartObject();
    gen.writeStringField(PREFIX, credential.prefix());
    JsonUtil.writeStringMap(CONFIG, credential.config(), gen);
    gen.writeEndObject();
  }

  static StorageCredential fromJson(String json) {
    return JsonUtil.parse(json, StorageCredentialParser::fromJson);
  }

  static StorageCredential fromJson(JsonNode json) {
    Preconditions.checkArgument(json != null, "Cannot parse storage credential from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse storage credential from non-object: %s", json);

    String prefix = JsonUtil.getString(PREFIX, json);
    Map<String, String> config = JsonUtil.getStringMap(CONFIG, json);
    return StorageCredential.create(prefix, config);
  }
}
