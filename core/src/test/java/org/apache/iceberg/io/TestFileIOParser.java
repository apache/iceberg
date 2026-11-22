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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.StringWriter;
import org.apache.iceberg.TestCatalogUtil.TestFileIONoArg;
import org.apache.iceberg.TestCatalogUtil.TestFileIOWithStorageCredentials;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

class TestFileIOParser {

  @Test
  void nullCheck() throws Exception {
    assertThatThrownBy(() -> FileIOParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid FileIO: null");

    StringWriter writer = new StringWriter();
    try (JsonGenerator generator = JsonUtil.factory().createGenerator(writer)) {
      assertThatThrownBy(() -> FileIOParser.toJson(null, generator))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid FileIO: null");

      assertThatThrownBy(() -> FileIOParser.toJson(new TestFileIOWithStorageCredentials(), null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid JSON generator: null");
    }

    assertThatThrownBy(() -> FileIOParser.fromJson((JsonNode) null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse FileIO from null object");
  }

  @Test
  void invalidJson() throws Exception {
    JsonNode arrayNode = JsonUtil.mapper().readTree("[]");
    assertThatThrownBy(() -> FileIOParser.fromJson(arrayNode, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse FileIO from non-object: []");

    assertThatThrownBy(() -> FileIOParser.fromJson("{}", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: io-impl");

    assertThatThrownBy(() -> FileIOParser.fromJson("{\"io-impl\": \"foo\"}", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing map: properties");

    String invalidArrayJson =
        String.format(
            "{\"io-impl\":\"%s\",\"properties\":{},\"storage-credentials\":\"invalid\"}",
            TestFileIOWithStorageCredentials.class.getName());
    assertThatThrownBy(() -> FileIOParser.fromJson(invalidArrayJson, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse storage credentials from non-array: \"invalid\"");

    String nullCredentialsJson =
        String.format(
            "{\"io-impl\":\"%s\",\"properties\":{},\"storage-credentials\":null}",
            TestFileIOWithStorageCredentials.class.getName());
    assertThatThrownBy(() -> FileIOParser.fromJson(nullCredentialsJson, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse storage credentials from non-array: null");
  }

  @Test
  void unsupportedProperties() {
    assertThatThrownBy(() -> FileIOParser.toJson(new TestFileIONoArg()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Cannot serialize FileIO: %s does not expose configuration properties",
                TestFileIONoArg.class.getName()));
  }

  @Test
  void roundTripWithoutCredentials() {
    TestFileIOWithStorageCredentials fileIO = new TestFileIOWithStorageCredentials();
    fileIO.initialize(ImmutableMap.of("k1", "v1", "k2", "v2"));

    String json = FileIOParser.toJson(fileIO);
    assertThat(json).doesNotContain("storage-credentials");

    try (FileIO deserialized = FileIOParser.fromJson(json)) {
      assertThat(deserialized).isInstanceOf(TestFileIOWithStorageCredentials.class);
      assertThat(deserialized.properties()).isEqualTo(fileIO.properties());
      assertThat(((SupportsStorageCredentials) deserialized).credentials()).isEmpty();
    }
  }

  @Test
  void roundTripWithCredentials() {
    TestFileIOWithStorageCredentials fileIO = new TestFileIOWithStorageCredentials();
    fileIO.initialize(ImmutableMap.of("k1", "v1", "k2", "v2"));

    StorageCredential cred1 =
        StorageCredential.create(
            "s3://bucket1",
            ImmutableMap.of("s3.access-key-id", "key1", "s3.secret-access-key", "secret1"));
    StorageCredential cred2 =
        StorageCredential.create(
            "gs://bucket2",
            ImmutableMap.of("gcs.oauth2.token", "token2", "gcs.oauth2.token-expires-at", "1000"));
    fileIO.setCredentials(ImmutableList.of(cred1, cred2));

    String json = FileIOParser.toJson(fileIO, true);
    assertThat(json).contains("storage-credentials");

    try (FileIO deserialized = FileIOParser.fromJson(json)) {
      assertThat(deserialized).isInstanceOf(TestFileIOWithStorageCredentials.class);
      assertThat(deserialized.properties()).isEqualTo(fileIO.properties());
      assertThat(((SupportsStorageCredentials) deserialized).credentials())
          .containsExactly(cred1, cred2);
    }
  }

  @Test
  void emptyCredentialsOmitted() {
    TestFileIOWithStorageCredentials fileIO = new TestFileIOWithStorageCredentials();
    fileIO.initialize(ImmutableMap.of("k1", "v1"));
    fileIO.setCredentials(ImmutableList.of());

    String json = FileIOParser.toJson(fileIO);
    assertThat(json).doesNotContain("storage-credentials");
  }

  @Test
  void backwardCompatibility() {
    String legacyJson =
        String.format(
            "{\"io-impl\":\"%s\",\"properties\":{\"k1\":\"v1\"}}",
            TestFileIOWithStorageCredentials.class.getName());

    try (FileIO deserialized = FileIOParser.fromJson(legacyJson)) {
      assertThat(deserialized).isInstanceOf(TestFileIOWithStorageCredentials.class);
      assertThat(deserialized.properties()).isEqualTo(ImmutableMap.of("k1", "v1"));
      assertThat(((SupportsStorageCredentials) deserialized).credentials()).isEmpty();
    }
  }
}
