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
 * The top-level file that a {@link Snapshot} points at. For v3 and earlier this is a manifest list
 * (see {@link ManifestListFile}); for v4+ it is a root manifest carrying a mix of data-file entries
 * and leaf-manifest entries.
 */
public interface SnapshotFile {

  /** Location of the snapshot file. */
  String location();

  /** The snapshot file key metadata can be encrypted. Returns ID of encryption key. */
  String encryptionKeyID();

  /** Decrypt and return the snapshot file key metadata. */
  ByteBuffer decryptKeyMetadata(EncryptionManager em);
}
