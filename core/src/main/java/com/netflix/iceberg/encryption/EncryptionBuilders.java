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

import com.google.common.base.Preconditions;
import com.netflix.iceberg.util.ByteBuffers;

import java.nio.ByteBuffer;

public class EncryptionBuilders {

  public static EncryptionKeyMetadataBuilder encryptionKeyMetadataBuilder() {
    return new EncryptionKeyMetadataBuilder();
  }

  public static PhysicalEncryptionKeyBuilder physicalEncryptionKeyBuilder() {
    return new PhysicalEncryptionKeyBuilder();
  }

  public static final class EncryptionKeyMetadataBuilder {
    private byte[] keyMetadata;
    private String cipherAlgorithm;
    private String keyAlgorithm;

    public EncryptionKeyMetadataBuilder keyMetadata(ByteBuffer keyMetadata) {
      return keyMetadata(ByteBuffers.toByteArray(
          Preconditions.checkNotNull(keyMetadata, "Key metadata should not be null.")));
    }

    public EncryptionKeyMetadataBuilder keyMetadata(byte[] keyMetadata) {
      this.keyMetadata = Preconditions.checkNotNull(
          keyMetadata, "Key metadata should not be null.");
      return this;
    }

    public EncryptionKeyMetadataBuilder cipherAlgorithm(String cipherAlgorithm) {
      this.cipherAlgorithm = Preconditions.checkNotNull(
          cipherAlgorithm, "Cipher algorithm should not be null.");
      return this;
    }

    public EncryptionKeyMetadataBuilder keyAlgorithm(String keyAlgorithm) {
      this.keyAlgorithm = keyAlgorithm;
      return this;
    }

    public EncryptionKeyMetadata build() {
      return new GenericEncryptionKeyMetadata(
          Preconditions.checkNotNull(keyMetadata, "Key metadata must be specified."),
          Preconditions.checkNotNull(cipherAlgorithm, "Cipher algorithm must be specified."),
          Preconditions.checkNotNull(keyAlgorithm, "Key algorithm must be spcified."));
    }
  }

  public static final class PhysicalEncryptionKeyBuilder {
    private EncryptionKeyMetadata keyMetadata;
    private byte[] iv;
    private byte[] secretKeyBytes;

    public PhysicalEncryptionKeyBuilder keyMetadata(EncryptionKeyMetadata keyMetadata) {
      this.keyMetadata = keyMetadata;
      return this;
    }

    public PhysicalEncryptionKeyBuilder iv(byte[] iv) {
      this.iv = Preconditions.checkNotNull(iv, "Iv should not be null.");
      return this;
    }

    public PhysicalEncryptionKeyBuilder iv(ByteBuffer iv) {
      return iv(ByteBuffers.toByteArray(
          Preconditions.checkNotNull(iv, "Iv should not be null.")));
    }

    public PhysicalEncryptionKeyBuilder secretKeyBytes(byte[] secretKeyBytes) {
      this.secretKeyBytes = Preconditions.checkNotNull(
          secretKeyBytes, "Secret key bytes should not be null.");
      return this;
    }

    public PhysicalEncryptionKeyBuilder secretKeyBytes(ByteBuffer secretKeyBytes) {
      return secretKeyBytes(ByteBuffers.toByteArray(
          Preconditions.checkNotNull(secretKeyBytes, "Secret key bytes should not be null.")));
    }

    public PhysicalEncryptionKey build() {
      return new GenericPhysicalEncryptionKey(
          Preconditions.checkNotNull(iv, "Iv must be provided."),
          Preconditions.checkNotNull(secretKeyBytes, "Secret key bytes must be provided."),
          Preconditions.checkNotNull(keyMetadata, "Key metadata must be provided."));
    }
  }

  private EncryptionBuilders() {}
}
