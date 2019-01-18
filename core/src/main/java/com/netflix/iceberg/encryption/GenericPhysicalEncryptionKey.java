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

import java.nio.ByteBuffer;

class GenericPhysicalEncryptionKey implements PhysicalEncryptionKey {

  private final byte[] secretKeyBytes;
  private final byte[] iv;
  private final EncryptionKeyMetadata encryptionKeyMetadata;

  public GenericPhysicalEncryptionKey(
      byte[] secretKeyBytes,
      byte[] iv,
      EncryptionKeyMetadata encryptionKeyMetadata) {
    this.secretKeyBytes = secretKeyBytes;
    this.iv = iv;
    this.encryptionKeyMetadata = encryptionKeyMetadata;
  }

  @Override
  public ByteBuffer secretKeyBytes() {
    return ByteBuffer.wrap(secretKeyBytes).asReadOnlyBuffer();
  }

  @Override
  public ByteBuffer iv() {
    return ByteBuffer.wrap(iv).asReadOnlyBuffer();
  }

  @Override
  public EncryptionKeyMetadata keyMetadata() {
    return encryptionKeyMetadata;
  }

}
