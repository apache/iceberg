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

import org.apache.iceberg.encryption.EncryptedKey;

/**
 * A file that may be encrypted using an {@link EncryptedKey}.
 *
 * <p>The encrypted key is either referenced by its ID from the table metadata encryption keys, or
 * stored directly with the file's metadata that describe the file. If neither is present, the file
 * is not encrypted.
 */
public interface FileWithEncryptedKey {

  /** Returns the location of the file. */
  String location();

  /** Returns the length of the file in bytes, or null if unknown. */
  Long length();

  /** Returns the ID of the encrypted key in table metadata, or null if not set. */
  String keyId();

  /** Returns the encrypted key stored directly with the file, or null if not set. */
  EncryptedKey encryptedKey();
}
