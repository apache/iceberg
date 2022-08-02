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
package org.apache.iceberg.encryption.kms;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.encryption.Ciphers;
import org.apache.iceberg.encryption.KmsClient;

/** For testing and demonstrations; not for use in production. */
public abstract class MemoryMockKMS implements KmsClient {

  protected Map<String, byte[]> masterKeys;

  @Override
  public String wrapKey(ByteBuffer key, String wrappingKeyId) {
    byte[] wrappingKey = masterKeys.get(wrappingKeyId);
    if (null == wrappingKey) {
      throw new RuntimeException(
          "Cannot wrap, because wrapping key " + wrappingKeyId + " is not found");
    }
    Ciphers.AesGcmEncryptor keyEncryptor = new Ciphers.AesGcmEncryptor(wrappingKey);
    byte[] encryptedKey = keyEncryptor.encrypt(key.array(), null);
    return Base64.getEncoder().encodeToString(encryptedKey);
  }

  @Override
  public ByteBuffer unwrapKey(String wrappedKey, String wrappingKeyId) {
    byte[] encryptedKey = Base64.getDecoder().decode(wrappedKey);
    byte[] wrappingKey = masterKeys.get(wrappingKeyId);
    if (null == wrappingKey) {
      throw new RuntimeException(
          "Cannot unwrap, because wrapping key " + wrappingKeyId + " is not found");
    }
    Ciphers.AesGcmDecryptor keyDecryptor = new Ciphers.AesGcmDecryptor(wrappingKey);
    byte[] key = keyDecryptor.decrypt(encryptedKey, null);
    return ByteBuffer.wrap(key);
  }
}
