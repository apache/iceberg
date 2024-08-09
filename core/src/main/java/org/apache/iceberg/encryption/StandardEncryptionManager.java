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
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ByteBuffers;

public class StandardEncryptionManager implements EncryptionManager {
  public static final int KEK_ID_LENGTH = 16;

  private final transient KeyManagementClient kmsClient;
  private final String tableKeyId;
  private final int dataKeyLength;
  private final long writerKekTimeout;
  private Map<String, KeyEncryptionKey> kekCache;
  private transient volatile SecureRandom lazyRNG = null;

  /**
   * @deprecated will be removed in 2.0.0. use {@link #StandardEncryptionManager(String, int,
   *     KeyManagementClient, long)} instead.
   */
  @Deprecated
  public StandardEncryptionManager(
      String tableKeyId, int dataKeyLength, KeyManagementClient kmsClient) {
    this(tableKeyId, dataKeyLength, kmsClient, CatalogProperties.WRITER_KEK_TIMEOUT_MS_DEFAULT);
  }

  /**
   * @param tableKeyId table encryption key id
   * @param dataKeyLength length of data encryption key (16/24/32 bytes)
   * @param kmsClient Client of KMS used to wrap/unwrap keys in envelope encryption
   * @param writerKekTimeout timeout of kek (key encryption key) cache entries
   */
  public StandardEncryptionManager(
      String tableKeyId, int dataKeyLength, KeyManagementClient kmsClient, long writerKekTimeout) {
    Preconditions.checkNotNull(tableKeyId, "Invalid encryption key ID: null");
    Preconditions.checkArgument(
        dataKeyLength == 16 || dataKeyLength == 24 || dataKeyLength == 32,
        "Invalid data key length: %s (must be 16, 24, or 32)",
        dataKeyLength);
    Preconditions.checkNotNull(kmsClient, "Invalid KMS client: null");
    this.tableKeyId = tableKeyId;
    this.kmsClient = kmsClient;
    this.dataKeyLength = dataKeyLength;
    this.writerKekTimeout = writerKekTimeout;
    this.kekCache = Maps.newHashMap();
  }

  @Override
  public NativeEncryptionOutputFile encrypt(OutputFile plainOutput) {
    return new StandardEncryptedOutputFile(plainOutput, dataKeyLength);
  }

  @Override
  public NativeEncryptionInputFile decrypt(EncryptedInputFile encrypted) {
    // this input file will lazily parse key metadata in case the file is not an AES GCM stream.
    if (encrypted instanceof NativeEncryptionInputFile) {
      return (NativeEncryptionInputFile) encrypted;
    }

    return new StandardDecryptedInputFile(encrypted);
  }

  @Override
  public Iterable<InputFile> decrypt(Iterable<EncryptedInputFile> encrypted) {
    return Iterables.transform(encrypted, this::decrypt);
  }

  private SecureRandom workerRNG() {
    if (this.lazyRNG == null) {
      this.lazyRNG = new SecureRandom();
    }

    return lazyRNG;
  }

  public ByteBuffer wrapKey(ByteBuffer secretKey) {
    if (kmsClient == null) {
      throw new IllegalStateException(
          "Cannot wrap key after called after serialization (missing KMS client)");
    }

    return kmsClient.wrapKey(secretKey, tableKeyId);
  }

  public ByteBuffer unwrapKey(ByteBuffer wrappedSecretKey) {
    if (kmsClient == null) {
      throw new IllegalStateException("Cannot unwrap key after serialization (missing KMS client)");
    }

    return kmsClient.unwrapKey(wrappedSecretKey, tableKeyId);
  }

  void addKekCache(Map<String, KeyEncryptionKey> wrappedKekCache) {
    for (Map.Entry<String, KeyEncryptionKey> entry : wrappedKekCache.entrySet()) {
      KeyEncryptionKey wrappedKek = entry.getValue();
      KeyEncryptionKey cachedKek = kekCache.get(entry.getKey());

      if (cachedKek != null) {
        Preconditions.checkState(
            cachedKek.wrappedKey().equals(wrappedKek.wrappedKey()),
            "Cached kek wrap differs from newly added for %s",
            entry.getKey());
      } else {
        ByteBuffer encryptedKEK =
            ByteBuffer.wrap(Base64.getDecoder().decode(wrappedKek.wrappedKey()));
        // Unwrap the key in KMS
        byte[] kekBytes = unwrapKey(encryptedKEK).array();

        kekCache.put(
            entry.getKey(),
            new KeyEncryptionKey(
                wrappedKek.id(), kekBytes, wrappedKek.wrappedKey(), wrappedKek.timestamp()));
      }
    }
  }

  KeyEncryptionKey currentKEK() {
    if (kekCache.isEmpty()) {
      KeyEncryptionKey keyEncryptionKey = generateNewKEK();
      kekCache.put(keyEncryptionKey.id(), keyEncryptionKey);

      return keyEncryptionKey;
    }

    KeyEncryptionKey lastKek = lastKek();
    long timeNow = System.currentTimeMillis();
    if (timeNow - lastKek.timestamp() > writerKekTimeout) {
      KeyEncryptionKey keyEncryptionKey = generateNewKEK();
      kekCache.put(keyEncryptionKey.id(), keyEncryptionKey);

      return keyEncryptionKey;
    }

    return lastKek;
  }

  private KeyEncryptionKey lastKek() {
    int counter = 0;
    KeyEncryptionKey result = null;
    for (Map.Entry<String, KeyEncryptionKey> entry : kekCache.entrySet()) {
      if (counter == 0) {
        result = entry.getValue();
      } else {
        if (entry.getValue().timestamp() > result.timestamp()) {
          result = entry.getValue();
        }
      }

      counter++;
    }

    return result;
  }

  private KeyEncryptionKey generateNewKEK() {
    byte[] newKek = new byte[dataKeyLength];
    workerRNG().nextBytes(newKek);
    String wrappedNewKek =
        Base64.getEncoder().encodeToString(wrapKey(ByteBuffer.wrap(newKek)).array());

    byte[] idBytes = new byte[KEK_ID_LENGTH];
    workerRNG().nextBytes(idBytes);
    String kekID = Base64.getEncoder().encodeToString(idBytes);

    return new KeyEncryptionKey(kekID, newKek, wrappedNewKek, System.currentTimeMillis());
  }

  KeyEncryptionKey keyEncryptionKey(String kekID) {
    KeyEncryptionKey result = kekCache.get(kekID);
    Preconditions.checkState(result != null, "Key encryption key %s not found", kekID);

    return result;
  }

  Map<String, KeyEncryptionKey> kekCache() {
    return kekCache;
  }

  private class StandardEncryptedOutputFile implements NativeEncryptionOutputFile {
    private final OutputFile plainOutputFile;
    private final int dataKeyLength;
    private StandardKeyMetadata lazyKeyMetadata = null;
    private OutputFile lazyEncryptingOutputFile = null;

    StandardEncryptedOutputFile(OutputFile plainOutputFile, int dataKeyLength) {
      this.plainOutputFile = plainOutputFile;
      this.dataKeyLength = dataKeyLength;
    }

    @Override
    public StandardKeyMetadata keyMetadata() {
      if (null == lazyKeyMetadata) {
        byte[] fileDek = new byte[dataKeyLength];
        workerRNG().nextBytes(fileDek);

        byte[] aadPrefix = new byte[TableProperties.ENCRYPTION_AAD_LENGTH_DEFAULT];
        workerRNG().nextBytes(aadPrefix);

        this.lazyKeyMetadata = new StandardKeyMetadata(fileDek, aadPrefix);
      }

      return lazyKeyMetadata;
    }

    @Override
    public OutputFile encryptingOutputFile() {
      if (null == lazyEncryptingOutputFile) {
        this.lazyEncryptingOutputFile =
            new AesGcmOutputFile(
                plainOutputFile,
                ByteBuffers.toByteArray(keyMetadata().encryptionKey()),
                ByteBuffers.toByteArray(keyMetadata().aadPrefix()));
      }

      return lazyEncryptingOutputFile;
    }

    @Override
    public OutputFile plainOutputFile() {
      return plainOutputFile;
    }
  }

  private static class StandardDecryptedInputFile implements NativeEncryptionInputFile {
    private final EncryptedInputFile encryptedInputFile;
    private StandardKeyMetadata lazyKeyMetadata = null;
    private AesGcmInputFile lazyDecryptedInputFile = null;

    private StandardDecryptedInputFile(EncryptedInputFile encryptedInputFile) {
      this.encryptedInputFile = encryptedInputFile;
    }

    @Override
    public InputFile encryptedInputFile() {
      return encryptedInputFile.encryptedInputFile();
    }

    @Override
    public StandardKeyMetadata keyMetadata() {
      if (null == lazyKeyMetadata) {
        this.lazyKeyMetadata = StandardKeyMetadata.castOrParse(encryptedInputFile.keyMetadata());
      }

      return lazyKeyMetadata;
    }

    private AesGcmInputFile decrypted() {
      if (null == lazyDecryptedInputFile) {
        this.lazyDecryptedInputFile =
            new AesGcmInputFile(
                encryptedInputFile.encryptedInputFile(),
                ByteBuffers.toByteArray(keyMetadata().encryptionKey()),
                ByteBuffers.toByteArray(keyMetadata().aadPrefix()));
      }

      return lazyDecryptedInputFile;
    }

    @Override
    public long getLength() {
      return decrypted().getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return decrypted().newStream();
    }

    @Override
    public String location() {
      return decrypted().location();
    }

    @Override
    public boolean exists() {
      return decrypted().exists();
    }
  }
}
