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

/** {@link EncryptionKeyMetadata} for use with format-native encryption. */
public interface NativeEncryptionKeyMetadata extends EncryptionKeyMetadata {
  /** Encryption key as a {@link ByteBuffer} */
  ByteBuffer encryptionKey();

  /** Additional authentication data as a {@link ByteBuffer} */
  ByteBuffer aadPrefix();

  /** Encrypted file length */
  default Long fileLength() {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement fileLength");
  }

  /**
   * Copy this key metadata and set the file length.
   *
   * @param length length of the encrypted file in bytes
   * @return a copy of this key metadata (key and AAD) with the file length
   */
  default NativeEncryptionKeyMetadata copyWithLength(long length) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement copyWithLength");
  }
}
