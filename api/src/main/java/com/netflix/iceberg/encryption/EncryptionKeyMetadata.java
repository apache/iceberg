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

/**
 * Light typedef over a ByteBuffer that indicates that the given bytes represent metadata about
 * an encrypted data file's encryption key.
 * <p>
 * This is preferred over passing a ByteBuffer directly in order to be more explicit.
 */
public interface EncryptionKeyMetadata {

  EncryptionKeyMetadata EMPTY = new EncryptionKeyMetadata() {
    @Override
    public ByteBuffer buffer() {
      return null;
    }

    @Override
    public EncryptionKeyMetadata copy() {
      return this;
    }
  };

  static EncryptionKeyMetadata empty() {
    return EMPTY;
  }

  /**
   * Opaque blob representing metadata about a file's encryption key.
   */
  ByteBuffer buffer();

  EncryptionKeyMetadata copy();
}
