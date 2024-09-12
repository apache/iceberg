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
import java.time.Instant;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class CredentialParser {
  private static final String TYPE = "type";
  private static final String S3 = "s3";
  private static final String GCS = "gcs";
  private static final String ADLS = "adls";
  private static final String ACCESS_KEY_ID = "access-key-id";
  private static final String SECRET_ACCESS_KEY = "secret-access-key";
  private static final String GCS_TOKEN = "token";
  private static final String ADLS_TOKEN = "sas-token";
  private static final String S3_TOKEN = "session-token";
  private static final String EXPIRES_AT = "expires-at-ms";
  private static final String SCHEME = "scheme";

  private CredentialParser() {}

  public static String toJson(Credential credentials) {
    return toJson(credentials, false);
  }

  public static String toJson(Credential credentials, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(credentials, gen), pretty);
  }

  public static void toJson(Credential credentials, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != credentials, "Invalid credential: null");

    gen.writeStartObject();

    if (credentials instanceof S3Credential) {
      S3Credential s3 = (S3Credential) credentials;
      gen.writeStringField(TYPE, S3);
      gen.writeStringField(SCHEME, s3.scheme());
      gen.writeStringField(ACCESS_KEY_ID, s3.accessKeyId());
      gen.writeStringField(SECRET_ACCESS_KEY, s3.secretAccessKey());
      gen.writeStringField(S3_TOKEN, s3.sessionToken());
      gen.writeNumberField(EXPIRES_AT, s3.expiresAt().toEpochMilli());
    } else if (credentials instanceof GcsCredential) {
      GcsCredential gcs = (GcsCredential) credentials;
      gen.writeStringField(TYPE, GCS);
      gen.writeStringField(SCHEME, gcs.scheme());
      gen.writeStringField(GCS_TOKEN, gcs.token());
      gen.writeNumberField(EXPIRES_AT, gcs.expiresAt().toEpochMilli());
    } else if (credentials instanceof AdlsCredential) {
      AdlsCredential adls = (AdlsCredential) credentials;
      gen.writeStringField(TYPE, ADLS);
      gen.writeStringField(SCHEME, adls.scheme());
      gen.writeStringField(ADLS_TOKEN, adls.sasToken());
      gen.writeNumberField(EXPIRES_AT, adls.expiresAt().toEpochMilli());
    }

    gen.writeEndObject();
  }

  public static Credential fromJson(String json) {
    return JsonUtil.parse(json, CredentialParser::fromJson);
  }

  public static Credential fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse credential from null object");
    String credentialType = JsonUtil.getString(TYPE, json);
    String scheme = JsonUtil.getString(SCHEME, json);
    switch (credentialType) {
      case S3:
        return ImmutableS3Credential.builder()
            .scheme(scheme)
            .accessKeyId(JsonUtil.getString(ACCESS_KEY_ID, json))
            .secretAccessKey(JsonUtil.getString(SECRET_ACCESS_KEY, json))
            .sessionToken(JsonUtil.getString(S3_TOKEN, json))
            .expiresAt(Instant.ofEpochMilli(JsonUtil.getLong(EXPIRES_AT, json)))
            .build();
      case GCS:
        return ImmutableGcsCredential.builder()
            .scheme(scheme)
            .token(JsonUtil.getString(GCS_TOKEN, json))
            .expiresAt(Instant.ofEpochMilli(JsonUtil.getLong(EXPIRES_AT, json)))
            .build();
      case ADLS:
        return ImmutableAdlsCredential.builder()
            .scheme(scheme)
            .sasToken(JsonUtil.getString(ADLS_TOKEN, json))
            .expiresAt(Instant.ofEpochMilli(JsonUtil.getLong(EXPIRES_AT, json)))
            .build();

      default:
        return null;
    }
  }
}
