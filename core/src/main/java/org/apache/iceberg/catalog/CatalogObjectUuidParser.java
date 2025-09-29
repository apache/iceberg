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
package org.apache.iceberg.catalog;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class CatalogObjectUuidParser {
  private static final String UUID = "uuid";
  private static final String TYPE = "type";

  private CatalogObjectUuidParser() {}

  public static String toJson(CatalogObjectUuid catalogObjectUuid) {
    return toJson(catalogObjectUuid, false);
  }

  public static String toJsonPretty(CatalogObjectUuid catalogObjectUuid) {
    return toJson(catalogObjectUuid, true);
  }

  private static String toJson(CatalogObjectUuid catalogObjectUuid, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(catalogObjectUuid, gen), pretty);
  }

  public static void toJson(CatalogObjectUuid catalogObjectUuid, JsonGenerator gen)
      throws IOException {
    Preconditions.checkNotNull(catalogObjectUuid, "Invalid catalog object uuid: null");

    gen.writeStartObject();
    gen.writeStringField(UUID, catalogObjectUuid.uuid());
    gen.writeStringField(TYPE, catalogObjectUuid.type().type());
    gen.writeEndObject();
  }

  public static CatalogObjectUuid fromJson(String json) {
    return JsonUtil.parse(json, CatalogObjectUuidParser::fromJson);
  }

  public static CatalogObjectUuid fromJson(JsonNode json) {
    Preconditions.checkNotNull(json, "Cannot parse catalog object uuid from null object");

    String uuid = JsonUtil.getString(UUID, json);
    CatalogObjectType type = CatalogObjectType.fromType(JsonUtil.getString(TYPE, json));

    return new CatalogObjectUuid(uuid, type);
  }
}
