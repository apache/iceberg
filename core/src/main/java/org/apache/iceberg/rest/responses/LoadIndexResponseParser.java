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
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexMetadataParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class LoadIndexResponseParser {

  private static final String METADATA_LOCATION = "metadata-location";
  private static final String METADATA = "metadata";
  private static final String CONFIG = "config";

  private LoadIndexResponseParser() {}

  public static String toJson(LoadIndexResponse response) {
    return toJson(response, false);
  }

  public static String toJson(LoadIndexResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(LoadIndexResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid load index response: null");

    gen.writeStartObject();

    if (null != response.metadataLocation()) {
      gen.writeStringField(METADATA_LOCATION, response.metadataLocation());
    }

    gen.writeFieldName(METADATA);
    IndexMetadataParser.toJson(response.metadata(), gen);

    if (!response.config().isEmpty()) {
      JsonUtil.writeStringMap(CONFIG, response.config(), gen);
    }

    gen.writeEndObject();
  }

  public static LoadIndexResponse fromJson(String json) {
    return JsonUtil.parse(json, LoadIndexResponseParser::fromJson);
  }

  public static LoadIndexResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse load index response from null object");

    String metadataLocation = null;
    if (json.hasNonNull(METADATA_LOCATION)) {
      metadataLocation = JsonUtil.getString(METADATA_LOCATION, json);
    }

    IndexMetadata metadata =
        IndexMetadataParser.fromJson(metadataLocation, JsonUtil.get(METADATA, json));

    LoadIndexResponse.Builder builder =
        LoadIndexResponse.builder().withMetadata(metadata).withMetadataLocation(metadataLocation);

    if (json.hasNonNull(CONFIG)) {
      builder.addAllConfig(JsonUtil.getStringMap(CONFIG, json));
    }

    return builder.build();
  }
}
