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

public interface ManifestListFile {

  /** Location of manifest list file. */
  String location();

  /** Snapshot ID of the manifest list. */
  long snapshotId();

  /** The manifest list key metadata is encrypted. Returns the ID of the encryption key */
  String metadataEncryptionKeyID();

  /** Returns the encrypted manifest list key metadata */
  ByteBuffer encryptedKeyMetadata();

  /** Decrypt and return the encrypted key metadata */
  ByteBuffer decryptKeyMetadata(EncryptionManager em);
}
