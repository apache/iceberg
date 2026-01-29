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
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponseParser;

/**
 * @deprecated since 1.11.0, will be removed in 1.12.0; use {@link RemoteSignResponseParser}
 *     instead.
 */
@Deprecated
public class S3SignResponseParser {

  private S3SignResponseParser() {}

  public static String toJson(S3SignResponse response) {
    return RemoteSignResponseParser.toJson(response, false);
  }

  public static String toJson(S3SignResponse response, boolean pretty) {
    return RemoteSignResponseParser.toJson(response, pretty);
  }

  public static void toJson(S3SignResponse response, JsonGenerator gen) throws IOException {
    RemoteSignResponseParser.toJson(response, gen);
  }

  public static S3SignResponse fromJson(String json) {
    RemoteSignResponse result = RemoteSignResponseParser.fromJson(json);
    return ImmutableS3SignResponse.builder().from(result).build();
  }

  public static S3SignResponse fromJson(JsonNode json) {
    RemoteSignResponse result = RemoteSignResponseParser.fromJson(json);
    return ImmutableS3SignResponse.builder().from(result).build();
  }
}
