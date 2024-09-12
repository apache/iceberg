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

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.util.ByteBuffers;

class BaseManifestListFile implements ManifestListFile, Serializable {
  private final String location;
  private final long snapshotId;
  private final String keyMetadataKeyID;
  // stored as a byte array to be Serializable
  private final byte[] encryptedKeyMetadata;

  BaseManifestListFile(
      String location, long snapshotId, String keyMetadataKeyID, ByteBuffer encryptedKeyMetadata) {
    this.location = location;
    this.snapshotId = snapshotId;
    this.encryptedKeyMetadata = ByteBuffers.toByteArray(encryptedKeyMetadata);
    this.keyMetadataKeyID = keyMetadataKeyID;
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public long snapshotId() {
    return snapshotId;
  }

  @Override
  public String keyMetadataKeyId() {
    return keyMetadataKeyID;
  }

  @Override
  public ByteBuffer encryptedKeyMetadata() {
    if (encryptedKeyMetadata == null) {
      return null;
    }

    return ByteBuffer.wrap(encryptedKeyMetadata);
  }

  @Override
  public ByteBuffer decryptKeyMetadata(EncryptionManager em) {
    return EncryptionUtil.decryptSnapshotKeyMetadata(this, em);
  }
}
