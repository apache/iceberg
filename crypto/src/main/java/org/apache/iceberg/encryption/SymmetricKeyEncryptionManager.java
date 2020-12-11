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

import org.apache.iceberg.encryption.crypto.SymmetricKeyCryptoFormat;
import org.apache.iceberg.encryption.crypto.SymmetricKeyDecryptedInputFile;
import org.apache.iceberg.encryption.crypto.SymmetricKeyEncryptedOutputFile;
import org.apache.iceberg.encryption.dekprovider.DekProvider;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.conf.MapSerde;
import org.apache.iceberg.util.conf.SerializedConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SymmetricKeyEncryptionManager<K extends KekId> implements EncryptionManager {
  private static final Logger log = LoggerFactory.getLogger(SymmetricKeyEncryptionManager.class);
  private final DekProvider<K> dekProvider;
  private final K kekIdForWriting;
  private final SymmetricKeyCryptoFormat symmetricKeyCryptoFormat;
  private final MapSerde serde;

  public SymmetricKeyEncryptionManager(
      DekProvider<K> dekProvider,
      K kekIdForWriting,
      SymmetricKeyCryptoFormat symmetricKeyCryptoFormat,
      MapSerde serde) {
    this.dekProvider = dekProvider;
    this.kekIdForWriting = kekIdForWriting;
    this.symmetricKeyCryptoFormat = symmetricKeyCryptoFormat;
    this.serde = serde;
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    EncryptionKeyMetadata encryptionKeyMetadata = encrypted.keyMetadata();
    SerializedConf conf = SerializedConf.of(serde, encryptionKeyMetadata.buffer());

    Dek encryptedDek = Dek.load(conf);
    K kekId = dekProvider.loadKekId(conf);
    Dek plaintextDek = dekProvider.getPlaintextDek(kekId, encryptedDek);
    return new SymmetricKeyDecryptedInputFile(
        encrypted.encryptedInputFile(),
        symmetricKeyCryptoFormat,
        plaintextDek.plaintextDek(),
        plaintextDek.iv());
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile plaintextOutputFile) {
    Dek dek =
        dekProvider.getNewDek(
            kekIdForWriting,
            symmetricKeyCryptoFormat.dekLength(),
            symmetricKeyCryptoFormat.ivLength());

    OutputFile encryptingOutputFile =
        new SymmetricKeyEncryptedOutputFile(
            plaintextOutputFile, symmetricKeyCryptoFormat, dek.plaintextDek(), dek.iv());
    SerializedConf conf = SerializedConf.empty(serde);
    dek.dump(conf);
    kekIdForWriting.dump(conf);
    BasicEncryptionKeyMetadata basicEncryptionKeyMetadata =
        new BasicEncryptionKeyMetadata(conf.toBytes());

    return new BasicEncryptedOutputFile(encryptingOutputFile, basicEncryptionKeyMetadata);
  }
}
