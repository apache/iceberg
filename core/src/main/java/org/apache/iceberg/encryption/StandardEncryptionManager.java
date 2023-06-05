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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ByteBuffers;

public class StandardEncryptionManager implements EncryptionManager {
  private final String tableKeyId;
  private final int dataKeyLength;
  private final long writerKekTimeout;

  // a holder class of metadata that is not available after serialization
  private static class KeyManagementMetadata {
    private final KeyManagementClient kmsClient;
    private final Map<String, WrappedEncryptionKey> encryptionKeys;
    private WrappedEncryptionKey currentEncryptionKey;

    private KeyManagementMetadata(KeyManagementClient kmsClient) {
      this.kmsClient = kmsClient;
      this.encryptionKeys = Maps.newLinkedHashMap();
      this.currentEncryptionKey = null;
    }
  }

  private final transient KeyManagementMetadata keyData;

  private transient volatile SecureRandom lazyRNG = null;

  /**
   * @deprecated will be removed in 2.0.0. use {@link #StandardEncryptionManager(String, int, List,
   *     KeyManagementClient, long)} instead.
   */
  @Deprecated
  public StandardEncryptionManager(
      String tableKeyId, int dataKeyLength, KeyManagementClient kmsClient) {
    this(
        tableKeyId,
        dataKeyLength,
        ImmutableList.of(),
        kmsClient,
        CatalogProperties.WRITER_KEK_TIMEOUT_SEC_DEFAULT);
  }

  /**
   * @param tableKeyId table encryption key id
   * @param dataKeyLength length of data encryption key (16/24/32 bytes)
   * @param kmsClient Client of KMS used to wrap/unwrap keys in envelope encryption
   * @param writerKekTimeout timeout of kek (key encryption key) cache entries
   */
  StandardEncryptionManager(
      String tableKeyId,
      int dataKeyLength,
      List<WrappedEncryptionKey> keys,
      KeyManagementClient kmsClient,
      long writerKekTimeout) {
    Preconditions.checkNotNull(tableKeyId, "Invalid encryption key ID: null");
    Preconditions.checkArgument(
        dataKeyLength == 16 || dataKeyLength == 24 || dataKeyLength == 32,
        "Invalid data key length: %s (must be 16, 24, or 32)",
        dataKeyLength);
    Preconditions.checkNotNull(kmsClient, "Invalid KMS client: null");
    this.tableKeyId = tableKeyId;
    this.keyData = new KeyManagementMetadata(kmsClient);
    this.dataKeyLength = dataKeyLength;
    this.writerKekTimeout = writerKekTimeout;

    for (WrappedEncryptionKey key : keys) {
      keyData.encryptionKeys.put(key.id(), key);

      if (keyData.currentEncryptionKey == null
          || keyData.currentEncryptionKey.timestamp() < key.timestamp()) {
        keyData.currentEncryptionKey = key;
      }
    }
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
    if (keyData == null) {
      throw new IllegalStateException(
          "Cannot wrap key after called after serialization (missing KMS client)");
    }

    return keyData.kmsClient.wrapKey(secretKey, tableKeyId);
  }

  public ByteBuffer unwrapKey(ByteBuffer wrappedSecretKey) {
    if (keyData == null) {
      throw new IllegalStateException("Cannot unwrap key after serialization (missing KMS client)");
    }

    return keyData.kmsClient.unwrapKey(wrappedSecretKey, tableKeyId);
  }

  public String currentSnapshotKeyId() {
    if (keyData == null) {
      throw new IllegalStateException("Cannot return the current snapshot key after serialization");
    }

    if (keyData.currentEncryptionKey == null
        || keyData.currentEncryptionKey.timestamp()
            < System.currentTimeMillis() - writerKekTimeout) {
      createNewEncryptionKey();
    }

    return keyData.currentEncryptionKey.id();
  }

  public ByteBuffer unwrapKey(String keyId) {
    if (keyData == null) {
      throw new IllegalStateException("Cannot unwrap key after serialization (missing KMS client)");
    }

    WrappedEncryptionKey cachedKey = keyData.encryptionKeys.get(keyId);
    ByteBuffer key = cachedKey.key();

    if (key == null) {
      key = unwrapKey(cachedKey.wrappedKey());
      cachedKey.setUnwrappedKey(key);
    }

    return key;
  }

  Collection<WrappedEncryptionKey> keys() {
    if (keyData == null) {
      throw new IllegalStateException("Cannot return the current keys after serialization");
    }

    return keyData.encryptionKeys.values();
  }

  private ByteBuffer newKey() {
    byte[] newKey = new byte[dataKeyLength];
    workerRNG().nextBytes(newKey);
    return ByteBuffer.wrap(newKey);
  }

  private String newKeyId() {
    byte[] idBytes = new byte[6];
    workerRNG().nextBytes(idBytes);
    return Base64.getEncoder().encodeToString(idBytes);
  }

  private void createNewEncryptionKey() {
    long now = System.currentTimeMillis();
    ByteBuffer keyBytes = newKey();
    WrappedEncryptionKey key =
        new WrappedEncryptionKey(newKeyId(), keyBytes, wrapKey(keyBytes), now);
    keyData.encryptionKeys.put(key.id(), key);
    keyData.currentEncryptionKey = key;
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
                ByteBuffers.toByteArray(keyMetadata().aadPrefix()),
                keyMetadata().fileLength());
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
