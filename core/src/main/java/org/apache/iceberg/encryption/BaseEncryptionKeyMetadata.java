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
import org.apache.iceberg.util.ByteBuffers;

class BaseEncryptionKeyMetadata implements EncryptionKeyMetadata {

  public static EncryptionKeyMetadata fromKeyMetadata(ByteBuffer keyMetadata) {
    if (keyMetadata == null) {
      return EncryptionKeyMetadata.empty();
    }
    return new BaseEncryptionKeyMetadata(keyMetadata);
  }

  public static EncryptionKeyMetadata fromByteArray(byte[] keyMetadata) {
    if (keyMetadata == null) {
      return EncryptionKeyMetadata.empty();
    }
    return fromKeyMetadata(ByteBuffer.wrap(keyMetadata));
  }

  private final ByteBuffer keyMetadata;

  private BaseEncryptionKeyMetadata(ByteBuffer keyMetadata) {
    this.keyMetadata = keyMetadata;
  }

  @Override
  public ByteBuffer buffer() {
    return keyMetadata;
  }

  @Override
  public EncryptionKeyMetadata copy() {
    return new BaseEncryptionKeyMetadata(ByteBuffers.copy(keyMetadata));
  }
}
