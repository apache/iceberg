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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.encryption.BaseEncryptedKey;
import org.apache.iceberg.encryption.EncryptedKey;
import org.junit.jupiter.api.Test;

public class TestEncryptedKeyParser {
  private static final byte[] KEY_BYTES = "key".getBytes(StandardCharsets.UTF_8);
  private static final String BASE64_KEY = Base64.getEncoder().encodeToString(KEY_BYTES);

  @Test
  public void testMinimalMetadataFromJson() {
    EncryptedKey key =
        EncryptedKeyParser.fromJson(
            "{\"key-id\": \"a\", \"encrypted-key-metadata\": \"" + BASE64_KEY + "\"}");

    assertThat(key.keyId()).isEqualTo("a");
    assertThat(key.encryptedKeyMetadata()).isEqualTo(ByteBuffer.wrap(KEY_BYTES));
    assertThat(key.encryptedById()).isNull();
    assertThat(key.properties()).isEmpty();
  }

  @Test
  public void testRoundTrip() {
    EncryptedKey key =
        new BaseEncryptedKey("a", ByteBuffer.wrap(KEY_BYTES), "b", Map.of("test", "value"));

    EncryptedKey actual = EncryptedKeyParser.fromJson(EncryptedKeyParser.toJson(key));

    assertThat(actual.keyId()).isEqualTo(key.keyId());
    assertThat(actual.encryptedKeyMetadata()).isEqualTo(key.encryptedKeyMetadata());
    assertThat(actual.encryptedById()).isEqualTo(key.encryptedById());
    assertThat(actual.properties()).isEqualTo(key.properties());
  }

  @Test
  public void testMissingKeyId() {
    assertThatThrownBy(
            () ->
                EncryptedKeyParser.fromJson("{\"encrypted-key-metadata\": \"" + BASE64_KEY + "\"}"))
        .hasMessage("Cannot parse missing string: key-id")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testMissingKeyMetadata() {
    assertThatThrownBy(() -> EncryptedKeyParser.fromJson("{\"key-id\": \"a\"}"))
        .hasMessage("Cannot parse missing string: encrypted-key-metadata")
        .isInstanceOf(IllegalArgumentException.class);
  }
}
