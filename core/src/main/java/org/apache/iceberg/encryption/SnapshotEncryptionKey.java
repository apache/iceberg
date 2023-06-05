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
import java.util.Base64;

/**
 * Encryption keys and metadata required for decrypting the manifest list files in snapshots of
 * encrypted tables.
 */
public class SnapshotEncryptionKey implements Serializable {
  private final String id;
  private final String keyPayload;
  private final ByteBuffer keyPayloadBytes;
  private final String encryptionKeyID;

  public SnapshotEncryptionKey(String id, String keyPayload, String encryptionKeyID) {
    this.id = id;
    this.keyPayload = keyPayload;
    this.keyPayloadBytes = ByteBuffer.wrap(Base64.getDecoder().decode(keyPayload));
    this.encryptionKeyID = encryptionKeyID;
  }

  public String id() {
    return id;
  }

  public String keyPayload() {
    return keyPayload;
  }

  public ByteBuffer keyPayloadBytes() {
    return keyPayloadBytes;
  }

  public String encryptionKeyID() {
    return encryptionKeyID;
  }
}
