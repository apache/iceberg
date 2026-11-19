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
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class UnregisterTableResponseParser {

  private static final String METADATA_LOCATION = "metadata-location";
  private static final String METADATA = "metadata";

  private UnregisterTableResponseParser() {}

  public static String toJson(UnregisterTableResponse response) {
    return toJson(response, false);
  }

  public static String toJson(UnregisterTableResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(UnregisterTableResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != response, "Invalid unregister table response: null");

    gen.writeStartObject();

    gen.writeStringField(METADATA_LOCATION, response.metadataLocation());

    gen.writeFieldName(METADATA);
    TableMetadataParser.toJson(response.metadata(), gen);

    gen.writeEndObject();
  }

  public static UnregisterTableResponse fromJson(String json) {
    return JsonUtil.parse(json, UnregisterTableResponseParser::fromJson);
  }

  public static UnregisterTableResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse unregister table response from null object");

    String metadataLocation = JsonUtil.getString(METADATA_LOCATION, json);
    TableMetadata metadata = TableMetadataParser.fromJson(JsonUtil.get(METADATA, json));

    if (null == metadata.metadataFileLocation()) {
      metadata = TableMetadata.buildFrom(metadata).withMetadataLocation(metadataLocation).build();
    }

    return ImmutableUnregisterTableResponse.builder()
        .metadataLocation(metadataLocation)
        .metadata(metadata)
        .build();
  }
}
