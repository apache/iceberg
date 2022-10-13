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
package org.apache.iceberg.rest.responses;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.ImmutableNamespaceWithProperties;
import org.apache.iceberg.rest.NamespaceWithProperties;
import org.apache.iceberg.rest.NamespaceWithPropertiesParser;
import org.apache.iceberg.util.JsonUtil;

public class GetNamespaceResponseParser {

  private GetNamespaceResponseParser() {}

  public static String toJson(GetNamespaceResponse response) {
    return toJson(response, false);
  }

  public static String toJson(GetNamespaceResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(GetNamespaceResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid namespace creation response: null");

    NamespaceWithPropertiesParser.toJson(
        ImmutableNamespaceWithProperties.builder()
            .namespace(response.namespace())
            .properties(response.properties())
            .build(),
        gen);
  }

  public static GetNamespaceResponse fromJson(String json) {
    return JsonUtil.parse(json, GetNamespaceResponseParser::fromJson);
  }

  public static GetNamespaceResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse namespace creation response from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse namespace creation response from non-object: %s", json);

    NamespaceWithProperties namespaceWithProperties = NamespaceWithPropertiesParser.fromJson(json);
    return ImmutableGetNamespaceResponse.newBuilder()
        .namespace(namespaceWithProperties.namespace())
        .properties(namespaceWithProperties.properties())
        .build();
  }
}
