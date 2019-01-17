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

import com.netflix.iceberg.Schema;
import com.netflix.iceberg.types.Types;

/**
 * Information about how a table data file is decrypted.
 */
public interface FileEncryptionMetadata {

  Schema SCHEMA = new Schema(
      Types.NestedField.required(10000, "key_metadata", Types.BinaryType.get()),
      Types.NestedField.required(10001, "cipher_algorithm", Types.StringType.get()));

  static Schema schema() {
    return SCHEMA;
  }

  /**
   * Pointer to the encryption key that decrypts this file.
   * <p>
   * This is not the encryption key itself, but rather a reference to the encryption key that
   * lives in other backing store.
   */
  EncryptionKeyMetadata keyMetadata();

  /**
   * Cipher algorithm used to decrypt the file with this file's encryption key.
   */
  String cipherAlgorithm();

  FileEncryptionMetadata copy();
}
