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
import java.security.SecureRandom;
import java.util.Map;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

public class DefaultEncryptionManager implements EncryptionManager {
  private final KeyManagementClient kmsClient;
  private String tableKeyId;
  private int dataKeyLength;
  private boolean kmsGeneratedKeys;

  private transient volatile SecureRandom workerRNG = null;

  /**
   * @param tableKeyId table encryption key id
   * @param kmsClient Client of KMS used to wrap/unwrap keys in envelope encryption
   * @param encryptionProperties encryption properties
   */
  public DefaultEncryptionManager(
      String tableKeyId, KeyManagementClient kmsClient, Map<String, String> encryptionProperties) {
    Preconditions.checkNotNull(
        tableKeyId,
        "Cannot create EnvelopeEncryptionManager because table encryption key ID is not specified");
    Preconditions.checkNotNull(
        kmsClient, "Cannot create EnvelopeEncryptionManager because kmsClient is null");
    Preconditions.checkNotNull(
        encryptionProperties,
        "Cannot create EnvelopeEncryptionManager because encryptionProperties are not passed");
    this.tableKeyId = tableKeyId;
    this.kmsClient = kmsClient;
    this.kmsGeneratedKeys = kmsClient.supportsKeyGeneration();

    this.dataKeyLength =
        PropertyUtil.propertyAsInt(
            encryptionProperties,
            EncryptionProperties.ENCRYPTION_DEK_LENGTH,
            EncryptionProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile rawOutput) {
    if (null == workerRNG) {
      createSecureRandomGenerator();
    }

    ByteBuffer fileDek = ByteBuffer.allocate(dataKeyLength);
    workerRNG.nextBytes(fileDek.array());

    ByteBuffer aadPrefix = ByteBuffer.allocate(EncryptionProperties.ENCRYPTION_AAD_LENGTH_DEFAULT);
    workerRNG.nextBytes(aadPrefix.array());

    KeyMetadata fileEnvelopeMetadata = new KeyMetadata(fileDek, null, aadPrefix);

    return new BaseEncryptedOutputFile(rawOutput, fileEnvelopeMetadata);
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    if (encrypted.keyMetadata() == null || encrypted.keyMetadata().buffer() == null) {
      throw new RuntimeException(
          "Unencrypted file " + encrypted.encryptedInputFile().location() + " in encrypted table");
    }

    // Native decryption: simply return the input file. Parquet decryption will get the key from key
    // metadata.
    return encrypted.encryptedInputFile();
  }

  private void createSecureRandomGenerator() {
    workerRNG = new SecureRandom();
  }
}
