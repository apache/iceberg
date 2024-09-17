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

/**
 * This class keeps a wrapped (KMS-encrypted) version of the keys used to encrypt manifest list key
 * metadata. These keys have an ID and a creation timestamp.
 */
public class WrappedEncryptionKey implements Serializable {
  private final String keyID;
  private final ByteBuffer wrappedKey;
  private final long timestamp;

  public WrappedEncryptionKey(String keyID, ByteBuffer wrappedKey, long timestamp) {
    this.keyID = keyID;
    this.wrappedKey = wrappedKey;
    this.timestamp = timestamp;
  }

  public String id() {
    return keyID;
  }

  public ByteBuffer wrappedKey() {
    return wrappedKey;
  }

  public long timestamp() {
    return timestamp;
  }
}
