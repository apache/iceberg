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
package org.apache.iceberg.aws.s3.signer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class S3SignResponseParser {

  private static final String URI = "uri";
  private static final String HEADERS = "headers";

  private S3SignResponseParser() {}

  public static String toJson(S3SignResponse request) {
    return toJson(request, false);
  }

  public static String toJson(S3SignResponse request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(S3SignResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid s3 sign response: null");

    gen.writeStartObject();

    gen.writeStringField(URI, response.uri().toString());
    S3SignRequestParser.headersToJson(HEADERS, response.headers(), gen);

    gen.writeEndObject();
  }

  public static S3SignResponse fromJson(String json) {
    return JsonUtil.parse(json, S3SignResponseParser::fromJson);
  }

  public static S3SignResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse s3 sign response from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse s3 sign response from non-object: %s", json);

    java.net.URI uri = java.net.URI.create(JsonUtil.getString(URI, json));
    Map<String, List<String>> headers = S3SignRequestParser.headersFromJson(HEADERS, json);

    return ImmutableS3SignResponse.builder().uri(uri).headers(headers).build();
  }
}
