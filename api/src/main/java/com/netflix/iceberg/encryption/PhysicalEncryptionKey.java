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

import java.nio.ByteBuffer;

/**
 * The encryption key used to read and write encrypted files.
 * <p>
 * This key is never persisted in Iceberg, and is used in Iceberg clients (e.g. Spark, Pig, Hive)
 * to encrypt and decrypt table data files.
 */
public interface PhysicalEncryptionKey {

  /**
   * The secret key material used for decrypting the data file.
   */
  ByteBuffer secretKeyBytes();

  /**
   * Key algorithm to use to convert the secret key bytes into a cryptographic KeyMaterial.
   */
  String keyAlgorithm();

  /**
   * The initialization vector bytes to use with this encryption key when generating ciphers.
   */
  ByteBuffer iv();

  /**
   * Metadata about the encryption key.
   */
  EncryptionKeyMetadata keyMetadata();
}
