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
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * Encryption manager that performs envelope encryption.
 * <p>
 */
public class EnvelopeEncryptionManager implements EncryptionManager {

  private final EnvelopeEncryptionSpec envelopeEncryptionSpec;
  private final EnvelopeKeyManager keyManager;
  private final boolean pushdown;

  public EnvelopeEncryptionManager(
          boolean pushdown,
          EnvelopeEncryptionSpec spec,
          EnvelopeKeyManager keyManager) {
    this.envelopeEncryptionSpec = spec;
    this.keyManager = keyManager;
    this.pushdown = pushdown;
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    EnvelopeMetadata metadata = EnvelopeMetadataParser.fromJson(encrypted.keyMetadata().buffer());
    keyManager.decrypt(metadata);

    return new EnvelopeEncryptedInputFile(encrypted.encryptedInputFile(), metadata, pushdown);
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile rawOutput) {
    EnvelopeMetadata metadata = keyManager.generate(envelopeEncryptionSpec.dataFileConfig());
    if (pushdown) { // Relevant only for data files
      Map<String, ByteBuffer> columnDeks = null;
      if (metadata.columnMetadata() != null && !metadata.columnMetadata().isEmpty()) {
        columnDeks = new HashMap<>();
        for (EnvelopeMetadata columnMetadata : metadata.columnMetadata()) {
          columnDeks.put(columnMetadata.originalColumnName(), columnMetadata.dek());
        }
      }

      NativeFileEncryptParameters nativeEncryptParams = NativeFileEncryptParameters.create(metadata.dek())
              .columnKeys(columnDeks)
              .build();
      return new NativeEncryptedOutputFile(rawOutput, metadata, nativeEncryptParams);
    }

    return EncryptedFiles.encryptedOutput(new EnvelopeEncryptedOutputFile(rawOutput, metadata), metadata);
  }

  @Override
  public EncryptedOutputFile encryptManifestFile(OutputFile rawOutput) {
    EnvelopeMetadata metadata = keyManager.generate(envelopeEncryptionSpec.manifestFileConfig());
    return EncryptedFiles.encryptedOutput(new EnvelopeEncryptedOutputFile(rawOutput, metadata), metadata);
  }

  @Override
  public EncryptedOutputFile encryptManifestList(OutputFile rawOutput) {
    EnvelopeMetadata metadata = keyManager.generate(envelopeEncryptionSpec.manifestListConfig());
    return EncryptedFiles.encryptedOutput(new EnvelopeEncryptedOutputFile(rawOutput, metadata), metadata);
  }
}
