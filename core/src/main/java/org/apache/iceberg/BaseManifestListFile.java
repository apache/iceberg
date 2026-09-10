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

import java.nio.ByteBuffer;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;

/**
 * @deprecated since 1.12.0. Will be removed in 2.0.0; use {@link BaseFileWithKeyId} instead.
 */
@Deprecated
class BaseManifestListFile extends BaseFileWithKeyId implements ManifestListFile {

  BaseManifestListFile(String location, String encryptionKeyID) {
    super(location, encryptionKeyID);
  }

  @Override
  public String encryptionKeyID() {
    return keyId();
  }

  @Override
  public ByteBuffer decryptKeyMetadata(EncryptionManager em) {
    return EncryptionUtil.decryptManifestListKeyMetadata(this, em);
  }
}
