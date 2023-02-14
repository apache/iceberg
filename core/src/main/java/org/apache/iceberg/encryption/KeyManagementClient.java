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

import java.io.Closeable;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

/** A minimum client interface to connect to a key management service (KMS). */
interface KeyManagementClient extends Serializable, Closeable {

  /**
   * Wrap a secret key, using a wrapping/master key which is stored in KMS and referenced by an ID.
   * Wrapping means encryption of the secret key with the master key, and adding optional
   * KMS-specific metadata that allows the KMS to decrypt the secret key in an unwrapping call.
   *
   * @param key a secret key being wrapped
   * @param wrappingKeyId a key ID that represents a wrapping key stored in KMS
   * @return wrapped key material
   */
  ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId);

  /**
   * Some KMS systems support generation of secret keys inside the KMS server.
   *
   * @return true if KMS server supports key generation and KeyManagementClient implementation is
   *     interested to leverage this capability. Otherwise, return false - Iceberg will then
   *     generate secret keys locally (using the SecureRandom mechanism) and call {@link
   *     #wrapKey(ByteBuffer, String)} to wrap them in KMS.
   */
  default boolean supportsKeyGeneration() {
    return false;
  }

  /**
   * Generate a new secret key in the KMS server, and wrap it using a wrapping/master key which is
   * stored in KMS and referenced by an ID. This method will be called only if supportsKeyGeneration
   * returns true.
   *
   * @param wrappingKeyId a key ID that represents a wrapping key stored in KMS
   * @return key in two forms: raw, and wrapped with the given wrappingKeyId
   */
  default KeyGenerationResult generateKey(String wrappingKeyId) {
    throw new UnsupportedOperationException("Key generation is not supported in this KmsClient");
  }

  /**
   * Unwrap a secret key, using a wrapping/master key which is stored in KMS and referenced by an
   * ID.
   *
   * @param wrappedKey wrapped key material (encrypted key and optional KMS metadata, returned by
   *     the wrapKey method)
   * @param wrappingKeyId a key ID that represents a wrapping key stored in KMS
   * @return raw key bytes
   */
  ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId);

  /**
   * Initialize the KMS client with given properties.
   *
   * @param properties kms client properties
   */
  void initialize(Map<String, String> properties);

  /**
   * Close KMS Client to release underlying resources, this could be triggered in different threads
   * when KmsClient is shared by multiple encryption managers.
   */
  @Override
  default void close() {}

  /**
   * For KMS systems that support key generation, this class keeps the key generation result - the
   * raw secret key, and its wrap.
   */
  class KeyGenerationResult {
    private final ByteBuffer key;
    private final ByteBuffer wrappedKey;

    KeyGenerationResult(ByteBuffer key, ByteBuffer wrappedKey) {
      this.key = key;
      this.wrappedKey = wrappedKey;
    }

    public ByteBuffer key() {
      return key;
    }

    public ByteBuffer wrappedKey() {
      return wrappedKey;
    }
  }
}
