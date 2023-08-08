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

public class StandardEncryptionManager implements EncryptionManager {
  private final KeyManagementClient kmsClient;
  private String tableKeyId;
  private int dataKeyLength;

  private transient volatile SecureRandom workerRNG = null;

  /**
   * @param tableKeyId table encryption key id
   * @param kmsClient Client of KMS used to wrap/unwrap keys in envelope encryption
   * @param encryptionProperties encryption properties
   */
  public StandardEncryptionManager(
      String tableKeyId,
      int dataKeyLength,
      KeyManagementClient kmsClient,
      Map<String, String> encryptionProperties) {
    Preconditions.checkNotNull(tableKeyId, "Invalid encryption key ID: null");
    Preconditions.checkNotNull(kmsClient, "Invalid KMS client: null");
    Preconditions.checkNotNull(encryptionProperties, "Invalid encryption properties: null");
    this.tableKeyId = tableKeyId;
    this.kmsClient = kmsClient;

    this.dataKeyLength = dataKeyLength;
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile rawOutput) {
    lazyCreateRNG();

    ByteBuffer fileDek = ByteBuffer.allocate(dataKeyLength);
    workerRNG.nextBytes(fileDek.array());

    ByteBuffer aadPrefix = ByteBuffer.allocate(EncryptionProperties.ENCRYPTION_AAD_LENGTH_DEFAULT);
    workerRNG.nextBytes(aadPrefix.array());

    KeyMetadata encryptionMetadata = new KeyMetadata(fileDek, aadPrefix);

    return new BaseEncryptedOutputFile(
        new AesGcmOutputFile(rawOutput, fileDek.array(), aadPrefix.array()),
        encryptionMetadata,
        rawOutput);
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    if (encrypted.keyMetadata().buffer() == null) {
      throw new SecurityException(
          "Unencrypted file " + encrypted.encryptedInputFile().location() + " in encrypted table");
    }

    KeyMetadata keyMetadata = KeyMetadata.parse(encrypted.keyMetadata().buffer());

    byte[] fileDek = keyMetadata.encryptionKey().array();
    byte[] aadPrefix = keyMetadata.aadPrefix().array();

    return new AesGcmInputFile(encrypted.encryptedInputFile(), fileDek, aadPrefix);
  }

  private void lazyCreateRNG() {
    if (this.workerRNG == null) {
      this.workerRNG = new SecureRandom();
    }
  }

  public ByteBuffer wrapKey(ByteBuffer secretKey) {
    return kmsClient.wrapKey(secretKey, tableKeyId);
  }

  public ByteBuffer unwrapKey(ByteBuffer wrappedSecretKey) {
    return kmsClient.unwrapKey(wrappedSecretKey, tableKeyId);
  }
}
