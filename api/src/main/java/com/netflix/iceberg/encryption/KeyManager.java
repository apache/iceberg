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

package com.netflix.iceberg.encryption;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;

/**
 * Pluggable module for managing encryption keys that encrypt and decrypt table data files.
 */
public interface KeyManager extends Serializable {

  /**
   * Get the encryption key that the given metadata points to.
   */
  PhysicalEncryptionKey getEncryptionKey(EncryptionKeyMetadata encryptionMetadata);

  /**
   * Get all of the keys according to the provided key metadatas.
   * <p>
   * Implement this if the backing key store can support batch requests. The default implementation
   * fetches each key in succession.
   */
  default Iterable<PhysicalEncryptionKey> getEncryptionKeys(
      Iterable<EncryptionKeyMetadata> encryptionMetadatas) {
    return Iterables.transform(encryptionMetadatas, this::getEncryptionKey);
  }

  /**
   * Create and store a new encryption key for the given file path.
   */
  PhysicalEncryptionKey createAndStoreEncryptionKey(String path);

  /**
   * Create and store encryption keys for the given file paths.
   * <p>
   * Implement this if the backing key store can support batch requests. The default implementation
   * fetches each key in succession.
   */
  default Map<String, PhysicalEncryptionKey> createAndStoreEncryptionKeys(Iterable<String> paths) {
    Map<String, PhysicalEncryptionKey> keys = Maps.newHashMap();
    for (String path: paths) {
      keys.put(path, createAndStoreEncryptionKey(path));
    }
    return keys;
  }
}
