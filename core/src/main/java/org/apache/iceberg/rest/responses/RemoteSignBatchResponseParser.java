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
import java.net.URI;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.SignStatus;
import org.apache.iceberg.rest.requests.RemoteSignRequestParser;
import org.apache.iceberg.util.JsonUtil;

public class RemoteSignBatchResponseParser {

  private static final String RESULTS = "results";
  private static final String STATUS = "status";
  private static final String URI_FIELD = "uri";
  private static final String HEADERS = "headers";

  private RemoteSignBatchResponseParser() {}

  public static String toJson(RemoteSignBatchResponse response) {
    return toJson(response, false);
  }

  public static String toJson(RemoteSignBatchResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(RemoteSignBatchResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != response, "Invalid remote sign batch response: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(RESULTS);
    for (RemoteSignBatchResult result : response.results()) {
      toJson(result, gen);
    }

    gen.writeEndArray();

    gen.writeEndObject();
  }

  private static void toJson(RemoteSignBatchResult result, JsonGenerator gen) throws IOException {
    gen.writeStartObject();

    gen.writeStringField(STATUS, result.status().status());
    if (SignStatus.COMPLETED == result.status()) {
      gen.writeStringField(URI_FIELD, result.uri().toString());
      RemoteSignRequestParser.headersToJson(HEADERS, result.headers(), gen);
    } else {
      ErrorResponseParser.writeError(result.error(), gen);
    }

    gen.writeEndObject();
  }

  public static RemoteSignBatchResponse fromJson(String json) {
    return JsonUtil.parse(json, RemoteSignBatchResponseParser::fromJson);
  }

  public static RemoteSignBatchResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse remote sign batch response from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse remote sign batch response from non-object: %s", json);

    JsonNode results = JsonUtil.get(RESULTS, json);
    Preconditions.checkArgument(
        results.isArray(), "Cannot parse results from non-array: %s", results);

    ImmutableRemoteSignBatchResponse.Builder response = ImmutableRemoteSignBatchResponse.builder();
    for (JsonNode result : results) {
      response.addResults(resultFromJson(result));
    }

    return response.build();
  }

  private static RemoteSignBatchResult resultFromJson(JsonNode json) {
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse batch result from non-object: %s", json);

    SignStatus status = SignStatus.fromName(JsonUtil.getString(STATUS, json));
    ImmutableRemoteSignBatchResult.Builder result =
        ImmutableRemoteSignBatchResult.builder().status(status);
    if (SignStatus.COMPLETED == status) {
      result
          .uri(URI.create(JsonUtil.getString(URI_FIELD, json)))
          .headers(RemoteSignRequestParser.headersFromJson(HEADERS, json));
    } else {
      result.error(ErrorResponseParser.fromJson(json));
    }

    return result.build();
  }
}
