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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Barebone encryption parameters, one object per content file. Carries the file encryption key
 * (later, will be extended with column keys and AAD prefix). Applicable only to formats with native
 * encryption support (Parquet and ORC).
 */
public class NativeFileCryptoParameters {
  private ByteBuffer fileKey;
  private EncryptionAlgorithm fileEncryptionAlgorithm;

  private NativeFileCryptoParameters(
      ByteBuffer fileKey, EncryptionAlgorithm fileEncryptionAlgorithm) {
    Preconditions.checkState(fileKey != null, "File encryption key is not supplied");
    this.fileKey = fileKey;
    this.fileEncryptionAlgorithm = fileEncryptionAlgorithm;
  }

  /**
   * Creates the builder.
   *
   * @param fileKey per-file encryption key. For example, used as "footer key" DEK in Parquet
   *     encryption.
   */
  public static Builder create(ByteBuffer fileKey) {
    return new Builder(fileKey);
  }

  public static class Builder {
    private ByteBuffer fileKey;
    private EncryptionAlgorithm fileEncryptionAlgorithm;

    private Builder(ByteBuffer fileKey) {
      this.fileKey = fileKey;
    }

    public Builder encryptionAlgorithm(EncryptionAlgorithm encryptionAlgorithm) {
      this.fileEncryptionAlgorithm = encryptionAlgorithm;
      return this;
    }

    public NativeFileCryptoParameters build() {
      return new NativeFileCryptoParameters(fileKey, fileEncryptionAlgorithm);
    }
  }

  public ByteBuffer fileKey() {
    return fileKey;
  }

  public EncryptionAlgorithm encryptionAlgorithm() {
    return fileEncryptionAlgorithm;
  }
}
