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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

class TestStorageCredentialParser {

  @Test
  void nullAndEmptyCheck() {
    assertThatThrownBy(() -> StorageCredentialParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage credential: null");

    assertThatThrownBy(() -> StorageCredentialParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse storage credential from null object");
  }

  @Test
  void invalidOrMissingFields() throws Exception {
    assertThatThrownBy(() -> StorageCredentialParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: prefix");

    assertThatThrownBy(() -> StorageCredentialParser.fromJson("{\"prefix\": \"y\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing map: config");

    assertThatThrownBy(
            () ->
                StorageCredentialParser.fromJson("{\"prefix\": \"\", \"config\": {\"x\": \"23\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid prefix: must be non-empty");

    assertThatThrownBy(
            () -> StorageCredentialParser.fromJson("{\"prefix\": \"s3\", \"config\": {}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid config: must be non-empty");

    JsonNode arrayNode = JsonUtil.mapper().readTree("[]");
    assertThatThrownBy(() -> StorageCredentialParser.fromJson(arrayNode))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse storage credential from non-object: []");
  }

  @Test
  void s3Credential() {
    StorageCredential credential =
        StorageCredential.create(
            "s3://custom-uri",
            ImmutableMap.of(
                "s3.access-key-id",
                "keyId",
                "s3.secret-access-key",
                "accessKey",
                "s3.session-token",
                "sessionToken"));

    String expectedJson =
        "{\"prefix\":\"s3://custom-uri\",\"config\":{"
            + "\"s3.access-key-id\":\"keyId\","
            + "\"s3.secret-access-key\":\"accessKey\","
            + "\"s3.session-token\":\"sessionToken\"}}";

    String json = StorageCredentialParser.toJson(credential);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(StorageCredentialParser.fromJson(json)).isEqualTo(credential);

    String prettyJson = StorageCredentialParser.toJson(credential, true);
    assertThat(StorageCredentialParser.fromJson(prettyJson)).isEqualTo(credential);
  }

  @Test
  void gcsCredential() {
    StorageCredential credential =
        StorageCredential.create(
            "gs://custom-uri",
            ImmutableMap.of("gcs.oauth2.token", "gcsToken", "gcs.oauth2.token-expires-at", "1000"));

    String expectedJson =
        "{\"prefix\":\"gs://custom-uri\",\"config\":{"
            + "\"gcs.oauth2.token\":\"gcsToken\","
            + "\"gcs.oauth2.token-expires-at\":\"1000\"}}";

    String json = StorageCredentialParser.toJson(credential);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(StorageCredentialParser.fromJson(json)).isEqualTo(credential);

    String prettyJson = StorageCredentialParser.toJson(credential, true);
    assertThat(StorageCredentialParser.fromJson(prettyJson)).isEqualTo(credential);
  }

  @Test
  void adlsCredential() {
    StorageCredential credential =
        StorageCredential.create(
            "abfs://custom-uri",
            ImmutableMap.of(
                "adls.sas-token.account",
                "sasToken",
                "adls.auth.shared-key.account.key",
                "accountKey"));

    String expectedJson =
        "{\"prefix\":\"abfs://custom-uri\",\"config\":{"
            + "\"adls.sas-token.account\":\"sasToken\","
            + "\"adls.auth.shared-key.account.key\":\"accountKey\"}}";

    String json = StorageCredentialParser.toJson(credential);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(StorageCredentialParser.fromJson(json)).isEqualTo(credential);

    String prettyJson = StorageCredentialParser.toJson(credential, true);
    assertThat(StorageCredentialParser.fromJson(prettyJson)).isEqualTo(credential);
  }
}
