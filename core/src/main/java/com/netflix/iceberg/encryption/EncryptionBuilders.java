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

  public static EncryptionKeyMetadata of(ByteBuffer keyMetadata) {
    return GenericEncryptionKeyMetadata.of(
        ByteBuffers.toByteArray(
            Preconditions.checkNotNull(keyMetadata, "Key metadata should not be null.")));
  }

  public static EncryptionKeyMetadata of(byte[] keyMetadata) {
    return GenericEncryptionKeyMetadata.of(
        Preconditions.checkNotNull(
            keyMetadata, "Key metadata should not be null."));
  }

  public static FileEncryptionMetadataBuilder newFileEncryptionMetadataBuilder() {
    return new FileEncryptionMetadataBuilder();
  }

  public static final class FileEncryptionMetadataBuilder {
    private EncryptionKeyMetadata keyMetadata;
    private String cipherAlgorithm;

    public FileEncryptionMetadataBuilder keyMetadata(EncryptionKeyMetadata keyMetadata) {
      this.keyMetadata = GenericEncryptionKeyMetadata.of(
          ByteBuffers.toByteArray(
              Preconditions.checkNotNull(
                  keyMetadata, "Key metadata should not be null.").keyMetadata()));
      return this;
    }

    public FileEncryptionMetadataBuilder keyMetadata(ByteBuffer keyMetadata) {
      this.keyMetadata = GenericEncryptionKeyMetadata.of(
          ByteBuffers.toByteArray(
              Preconditions.checkNotNull(keyMetadata, "Key metadata should not be null.")));
      return this;
    }

    public FileEncryptionMetadataBuilder keyMetadata(byte[] keyMetadata) {
      return keyMetadata(ByteBuffer.wrap(
          Preconditions.checkNotNull(keyMetadata, "Key metadata should not be null.")));
    }

    public FileEncryptionMetadata build() {
      return new GenericFileEncryptionMetadata(
          Preconditions.checkNotNull(keyMetadata, "Key metadata must be specified."),
          Preconditions.checkNotNull(cipherAlgorithm, "Cipher algorithm cannot be null."));
    }
  }

  public static final class PhysicalEncryptionKeyBuilder {
    private EncryptionKeyMetadata keyMetadata;
    private String keyAlgorithm;
    private byte[] iv;
    private byte[] secretKeyBytes;

    public PhysicalEncryptionKeyBuilder keyMetadata(EncryptionKeyMetadata keyMetadata) {
      this.keyMetadata = GenericEncryptionKeyMetadata.of(
          ByteBuffers.toByteArray(
              Preconditions.checkNotNull(
                  keyMetadata, "Key metadata should not be null.").keyMetadata()));
      return this;
    }

    public PhysicalEncryptionKeyBuilder keyMetadata(byte[] keyMetadata) {
      this.keyMetadata = GenericEncryptionKeyMetadata.of(
          Preconditions.checkNotNull(
              keyMetadata, "Key metadata should not be null."));
      return this;
    }

    public PhysicalEncryptionKeyBuilder keyMetadata(ByteBuffer keyMetadata) {
      return keyMetadata(
          ByteBuffers.toByteArray(
              Preconditions.checkNotNull(keyMetadata, "Key metadata should not be null.")));
    }

    public PhysicalEncryptionKeyBuilder keyAlgorithm(String keyAlgorithm) {
      this.keyAlgorithm = Preconditions.checkNotNull(
          keyAlgorithm, "Key algorithm should not be null.");
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
          Preconditions.checkNotNull(keyMetadata, "Key metadata must be provided."),
          Preconditions.checkNotNull(keyAlgorithm, "Key algorithm must be provided."));
    }
  }

  private EncryptionBuilders() {}
}
