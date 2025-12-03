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
package org.apache.iceberg.encryption;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestBaseEncryptedKeySerialization {

  @Test
  public void testSerialization() throws Exception {
    byte[] keyBytes = "key".getBytes(StandardCharsets.UTF_8);
    EncryptedKey key =
        new BaseEncryptedKey("a", ByteBuffer.wrap(keyBytes), "b", Map.of("test", "value"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ObjectOutputStream writer = new ObjectOutputStream(out)) {
      writer.writeObject(key);
    }

    EncryptedKey result;
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    try (ObjectInputStream reader = new ObjectInputStream(in)) {
      result = (EncryptedKey) reader.readObject();
    }

    assertThat(result.keyId()).isEqualTo(key.keyId());
    assertThat(result.encryptedById()).isEqualTo(key.encryptedById());
    assertThat(result.encryptedKeyMetadata()).isEqualTo(key.encryptedKeyMetadata());
    assertThat(result.properties()).isEqualTo(key.properties());
  }
}
