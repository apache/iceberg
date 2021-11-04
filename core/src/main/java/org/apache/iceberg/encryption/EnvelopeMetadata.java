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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Envelope encryption metadata used to encrypt and decrypt data.
 * All fields (except for cleartext KEKs and DEKs) are serialized and stored in Iceberg key_metadata columns.
 */
public class EnvelopeMetadata implements EncryptionKeyMetadata {

  private final String kekId;
  private final String wrappedDek;
  private final EncryptionAlgorithm algorithm;

  // in-memory-only fields, set after object construction
  private byte[] dek;

  /**
   * Constructor with non-secret fields, that can be stored.
   * Secret fields (such as encryption keys) are set separately for in-memory sharing.
   * @param kekId ID of the "key encryption key", used to wrap the "data encryption key" in KMS
   * @param wrappedDek wrapped "data encryption key" (see {@link KmsClient#wrapKey(ByteBuffer, String)})
   * @param algorithm encryption algorithm
   */
  public EnvelopeMetadata(String kekId, String wrappedDek, EncryptionAlgorithm algorithm) {
    this.kekId = Preconditions.checkNotNull(kekId,
            "Cannot construct envelope metadata because KEK ID is not specified");
    this.wrappedDek = wrappedDek;
    this.algorithm = Preconditions.checkNotNull(algorithm,
              "Cannot construct envelope metadata because encryption algorithm is not specified");
  }

  public String kekId() {
    return kekId;
  }

  public ByteBuffer dek() {
    return dek == null ? null : ByteBuffer.wrap(dek);
  }

  public String wrappedDek() {
    return wrappedDek;
  }

  public EncryptionAlgorithm algorithm() {
    return algorithm;
  }

  public void setDek(ByteBuffer dekBuffer) {
    this.dek = dekBuffer == null ? null : dekBuffer.array();
  }

  @Override
  public ByteBuffer buffer() {
    return EnvelopeMetadataParser.toJson(this);
  }

  @Override
  public EncryptionKeyMetadata copy() {
    EnvelopeMetadata metadata = new EnvelopeMetadata(kekId(), wrappedDek(), algorithm());
    metadata.setDek(dek());
    return metadata;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    EnvelopeMetadata metadata = (EnvelopeMetadata) other;
    return Objects.equals(kekId, metadata.kekId) &&
        Objects.equals(wrappedDek, metadata.wrappedDek) &&
        algorithm == metadata.algorithm &&
        Arrays.equals(dek, metadata.dek);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(kekId, algorithm);
    result = 31 * result + Objects.hashCode(wrappedDek);
    result = 31 * result + Arrays.hashCode(dek);
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("kekId", kekId)
        .add("wrappedDek", wrappedDek)
        .add("algorithm", algorithm)
        .add("dek", dek == null ? "null" : "(redacted)")
        .toString();
  }
}
