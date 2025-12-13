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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class BaseEncryptedKey implements EncryptedKey {
  private final String keyId;
  private final ByteBuffer keyMetadata;
  private final String encryptedById;
  private final Map<String, String> properties;

  public BaseEncryptedKey(
      String keyId, ByteBuffer keyMetadata, String encryptedById, Map<String, String> properties) {
    Preconditions.checkArgument(keyId != null, "Key id cannot be null");
    Preconditions.checkArgument(keyMetadata != null, "Encrypted key metadata cannot be null");
    this.keyId = keyId;
    this.keyMetadata = keyMetadata;
    this.encryptedById = encryptedById;
    this.properties = properties;
  }

  @Override
  public String keyId() {
    return keyId;
  }

  @Override
  public ByteBuffer encryptedKeyMetadata() {
    return keyMetadata;
  }

  @Override
  public String encryptedById() {
    return encryptedById;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }
}
