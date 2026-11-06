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
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class BaseFileWithEncryptedKey implements FileWithEncryptedKey, Serializable {
  private final String location;
  private final Long length;
  private final String keyId;
  private final EncryptedKey encryptedKey;

  BaseFileWithEncryptedKey(String location, Long length, String keyId, EncryptedKey encryptedKey) {
    Preconditions.checkArgument(location != null, "Invalid file location: null");
    Preconditions.checkArgument(
        keyId == null || encryptedKey == null,
        "Cannot set both key ID and encrypted key for file: %s",
        location);
    this.location = location;
    this.length = length;
    this.keyId = keyId;
    this.encryptedKey = encryptedKey;
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public Long length() {
    return length;
  }

  @Override
  public String keyId() {
    return keyId;
  }

  @Override
  public EncryptedKey encryptedKey() {
    return encryptedKey;
  }
}
