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
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequestParser;

/**
 * @deprecated since 1.11.0, will be removed in 1.12.0; use {@link RemoteSignRequestParser} instead.
 */
@Deprecated
public class S3SignRequestParser {

  private S3SignRequestParser() {}

  public static String toJson(S3SignRequest request) {
    return RemoteSignRequestParser.toJson(request, false);
  }

  public static String toJson(S3SignRequest request, boolean pretty) {
    return RemoteSignRequestParser.toJson(request, pretty);
  }

  public static void toJson(S3SignRequest request, JsonGenerator gen) throws IOException {
    RemoteSignRequestParser.toJson(request, gen);
  }

  public static S3SignRequest fromJson(String json) {
    RemoteSignRequest request = RemoteSignRequestParser.fromJson(json);
    return ImmutableS3SignRequest.builder().from(request).build();
  }

  public static S3SignRequest fromJson(JsonNode json) {
    RemoteSignRequest request = RemoteSignRequestParser.fromJson(json);
    return ImmutableS3SignRequest.builder().from(request).build();
  }

  public static void headersToJson(
      String property, Map<String, List<String>> headers, JsonGenerator gen) throws IOException {
    RemoteSignRequestParser.headersToJson(property, headers, gen);
  }

  public static Map<String, List<String>> headersFromJson(String property, JsonNode json) {
    return RemoteSignRequestParser.headersFromJson(property, json);
  }
}
