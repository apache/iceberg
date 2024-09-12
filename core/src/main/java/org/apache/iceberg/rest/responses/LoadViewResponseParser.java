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
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.CredentialParser;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;

public class LoadViewResponseParser {

  private static final String METADATA_LOCATION = "metadata-location";
  private static final String METADATA = "metadata";
  private static final String CONFIG = "config";
  private static final String STORAGE_CREDENTIALS = "storage-credentials";

  private LoadViewResponseParser() {}

  public static String toJson(LoadViewResponse response) {
    return toJson(response, false);
  }

  public static String toJson(LoadViewResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(LoadViewResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid load view response: null");

    gen.writeStartObject();

    gen.writeStringField(METADATA_LOCATION, response.metadataLocation());

    gen.writeFieldName(METADATA);
    ViewMetadataParser.toJson(response.metadata(), gen);

    if (!response.config().isEmpty()) {
      JsonUtil.writeStringMap(CONFIG, response.config(), gen);
    }

    if (!response.credentials().isEmpty()) {
      gen.writeArrayFieldStart(STORAGE_CREDENTIALS);
      for (Credential credential : response.credentials()) {
        CredentialParser.toJson(credential, gen);
      }

      gen.writeEndArray();
    }

    gen.writeEndObject();
  }

  public static LoadViewResponse fromJson(String json) {
    return JsonUtil.parse(json, LoadViewResponseParser::fromJson);
  }

  public static LoadViewResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse load view response from null object");

    String metadataLocation = JsonUtil.getString(METADATA_LOCATION, json);
    ViewMetadata metadata = ViewMetadataParser.fromJson(JsonUtil.get(METADATA, json));

    if (null == metadata.metadataFileLocation()) {
      metadata = ViewMetadata.buildFrom(metadata).setMetadataLocation(metadataLocation).build();
    }

    ImmutableLoadViewResponse.Builder builder =
        ImmutableLoadViewResponse.builder().metadataLocation(metadataLocation).metadata(metadata);

    if (json.has(CONFIG)) {
      builder.config(JsonUtil.getStringMap(CONFIG, json));
    }

    if (json.hasNonNull(STORAGE_CREDENTIALS)) {
      JsonNode credentials = JsonUtil.get(STORAGE_CREDENTIALS, json);
      Preconditions.checkArgument(
          credentials.isArray(), "Cannot parse credentials from non-array: %s", credentials);

      credentials.forEach(
          cred ->
              Optional.ofNullable(CredentialParser.fromJson(cred))
                  .ifPresent(builder::addCredentials));
    }

    return builder.build();
  }
}
