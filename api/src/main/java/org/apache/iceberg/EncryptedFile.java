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

import java.nio.ByteBuffer;
import org.apache.iceberg.encryption.EncryptionManager;

/**
 * A file that may be encrypted. If it is encrypted, its encrypted key metadata is tracked in the
 * table metadata encryption keys and is referenced by a key ID.
 */
public interface EncryptedFile {

  /** Location of the file. */
  String location();

  /** Returns the encryption key ID for this file, or null if the file is not encrypted. */
  String encryptionKeyID();

  /** Decrypt and return the file key metadata */
  ByteBuffer decryptKeyMetadata(EncryptionManager em);
}
