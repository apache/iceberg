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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * a minimum client interface to connect to a key management service (KMS).
 */
public interface KmsClient extends Serializable {

  /**
   * Wrap a secret key, using a wrapping/master key which is stored in KMS and referenced by an ID.
   * Wrapping means encryption of the secret key with the master key, and adding optional metadata
   * that allows KMS to decrypt the secret key in an unwrap call.
   *
   * @param key            a secret key being wrapped
   * @param wrappingKeyId  a key ID that represents a wrapping key stored in KMS
   * @param context        additional authenticated data that can be used to ensure integrity
   * @return               wrapped key material
   */
  String wrapKey(ByteBuffer key, String wrappingKeyId, Map<String, String> context);

  /**
   * Some KMS systems support generation of secret keys inside the KMS server.
   *
   * @return true if KMS server supports key generation and KmsClient implementation
   * is interested to leverage this capability. Otherwise, return false - Iceberg will
   * then generate secret keys locally (using the SecureRandom mechanism) and call
   * wrapKey to wrap them in KMS.
   */
  boolean supportsKeyGeneration();

  /**
   * Generate a new secret key in the KMS server, and wrap it using a wrapping/master key
   * which is stored in KMS and referenced by an ID. This method will be called only if
   * supportsKeyGeneration returns true.
   *
   * @param wrappingKeyId   a key ID that represents a wrapping key stored in KMS
   * @param context         additional authenticated data that can be used to ensure integrity
   * @return                a pair of plaintext and wrapped key encrypted with the given wrappingKeyId
   */
  KeyGenerationResult generateKey(String wrappingKeyId, Map<String, String> context);

  /**
   * Unwrap a secret key, using a wrapping/master key which is stored in KMS and referenced by an ID.
   *
   * @param wrappedKey      wrapped key material (encrypted key and optional KMS metadata, returned by
   *                        the wrapKey method)
   * @param wrappingKeyId   a key ID that represents a wrapping key stored in KMS
   * @param context         additional authenticated data that can be used to ensure integrity
   * @return                plaintext key bytes
   */
  ByteBuffer unwrapKey(String wrappedKey, String wrappingKeyId, Map<String, String> context);

  /**
   * Returns the algorithm used by the provider when generating keys, such as AES_256_GCM
   */
  KeyGenerationAlgorithm keyGenerationAlgorithm();

  /**
   * Returns the spec of the key, such as AES_128
   */
  KeySpec keySpec();

  /**
   * Initialize key provider from catalog and other properties
   *
   * @param properties catalog properties
   */
  void initialize(Map<String, String> properties);

  enum KeyGenerationAlgorithm {
    AES_256_GCM
  }

  enum KeySpec {
    AES_128,
    AES_256
  }

  class KeyGenerationResult {
    private final ByteBuffer key;
    private final String wrappedKey;

    public KeyGenerationResult(ByteBuffer key, String wrappedKey) {
      this.key = key;
      this.wrappedKey = wrappedKey;
    }

    public ByteBuffer key() {
      return key;
    }

    public String wrappedKey() {
      return wrappedKey;
    }
  }
}
