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
package org.apache.iceberg.rest.credentials;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class CredentialParser {
  private static final String PREFIX = "prefix";
  private static final String CONFIG = "config";

  private CredentialParser() {}

  public static String toJson(Credential credential) {
    return toJson(credential, false);
  }

  public static String toJson(Credential credential, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(credential, gen), pretty);
  }

  public static void toJson(Credential credential, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != credential, "Invalid credential: null");

    gen.writeStartObject();

    gen.writeStringField(PREFIX, credential.prefix());
    JsonUtil.writeStringMap(CONFIG, credential.config(), gen);

    gen.writeEndObject();
  }

  public static Credential fromJson(String json) {
    return JsonUtil.parse(json, CredentialParser::fromJson);
  }

  public static Credential fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse credential from null object");
    String prefix = JsonUtil.getString(PREFIX, json);
    Map<String, String> config = JsonUtil.getStringMap(CONFIG, json);
    return ImmutableCredential.builder().prefix(prefix).config(config).build();
  }
}
