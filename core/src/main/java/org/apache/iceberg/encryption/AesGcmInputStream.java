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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.iceberg.io.SeekableInputStream;

import static org.apache.iceberg.encryption.AesGcmOutputStream.HEADER_SIZE_LENGTH;

public class AesGcmInputStream extends SeekableInputStream {
  private SeekableInputStream sourceStream;
  private long netSourceFileSize;

  private Cipher gcmCipher;
  private SecretKey key;
  private byte[] nonce;

  private byte[] ciphertextBlockBuffer;
  private int cipherBlockSize;
  private int plainBlockSize;
  private long plainStreamPosition;
  private int currentBlockIndex;
  private int currentOffsetInPlainBlock;
  private int numberOfBlocks;
  private int lastBlockSize;
  private long plainStreamSize;
  private byte[] fileAadPrefix;

  AesGcmInputStream(SeekableInputStream sourceStream, long sourceLength,
                    byte[] aesKey, byte[] fileAadPrefix) throws IOException {
    this.netSourceFileSize = sourceLength - HEADER_SIZE_LENGTH;
    this.sourceStream = sourceStream;
    byte[] blockSizeBytes = new byte[HEADER_SIZE_LENGTH];
    int fetched = sourceStream.read(blockSizeBytes); // TODO check
    this.plainStreamPosition = 0;
    this.fileAadPrefix = fileAadPrefix;

    plainBlockSize = ByteBuffer.wrap(blockSizeBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    cipherBlockSize = plainBlockSize + AesGcmOutputStream.GCM_NONCE_LENGTH + AesGcmOutputStream.GCM_TAG_LENGTH;

    try {
      gcmCipher = Cipher.getInstance("AES/GCM/NoPadding");
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
    this.nonce = new byte[AesGcmOutputStream.GCM_NONCE_LENGTH];
    this.key = new SecretKeySpec(aesKey, "AES");
    this.ciphertextBlockBuffer = new byte[cipherBlockSize];
    this.currentBlockIndex = 0;
    this.currentOffsetInPlainBlock = 0;

    numberOfBlocks = (int) (netSourceFileSize / cipherBlockSize);
    lastBlockSize = (int) (netSourceFileSize % cipherBlockSize);
    if (lastBlockSize == 0) {
      lastBlockSize = cipherBlockSize;
    } else {
      numberOfBlocks += 1;
    }

    plainStreamSize = (numberOfBlocks - 1L) * plainBlockSize +
            (lastBlockSize - AesGcmOutputStream.GCM_NONCE_LENGTH - AesGcmOutputStream.GCM_TAG_LENGTH);
  }

  public long plaintextStreamSize() {
    return plainStreamSize;
  }

  @Override
  public int available() throws IOException {
    return (int) (plainStreamSize - plainStreamPosition);
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len <= 0) {
      throw new IOException("Negative read length " + len);
    }

    if (available() <= 0) {
      return -1;
    }

    boolean lastBlock = currentBlockIndex + 1 == numberOfBlocks;
    int resultBufferOffset = off;
    int remaining = len;

    while (remaining > 0) {
      sourceStream.seek(HEADER_SIZE_LENGTH + currentBlockIndex * cipherBlockSize);

      int toLoad = lastBlock ? lastBlockSize : cipherBlockSize;
      int loaded = sourceStream.read(ciphertextBlockBuffer, 0, toLoad);
      if (loaded != toLoad) {
        throw new IOException("Read " + loaded + " instead of " + toLoad);
      }

      // Copy nonce
      System.arraycopy(ciphertextBlockBuffer, 0, nonce, 0, AesGcmOutputStream.GCM_NONCE_LENGTH);

      byte[] plaintextBlock = null;
      try {
        GCMParameterSpec spec = new GCMParameterSpec(AesGcmOutputStream.GCM_TAG_LENGTH_BITS, nonce);
        gcmCipher.init(Cipher.DECRYPT_MODE, key, spec);

        // TODO byte[] aaD = calculateAAD(fileAadPrefix, currentBlockIndex);

        plaintextBlock = gcmCipher.doFinal(ciphertextBlockBuffer, AesGcmOutputStream.GCM_NONCE_LENGTH,
                toLoad - AesGcmOutputStream.GCM_NONCE_LENGTH);
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed to decrypt", e);
      }

      int remainingInBlock = plaintextBlock.length - currentOffsetInPlainBlock;
      boolean finishTheBlock = remaining >= remainingInBlock;
      int toCopy = finishTheBlock ? remainingInBlock : remaining;

      System.arraycopy(plaintextBlock, currentOffsetInPlainBlock, b, resultBufferOffset, toCopy);
      remaining -= toCopy;
      resultBufferOffset += toCopy;
      currentOffsetInPlainBlock += toCopy;
      boolean endOfStream = lastBlock && finishTheBlock;
      if (endOfStream) {
        break;
      }
      if (finishTheBlock) {
        currentBlockIndex++;
        currentOffsetInPlainBlock = 0;
        lastBlock = currentBlockIndex + 1 == numberOfBlocks;
      }
    }

    plainStreamPosition += len - remaining;
    return len - remaining;
  }

  @Override
  public void seek(long newPos) throws IOException {
    currentBlockIndex = (int) (newPos / plainBlockSize);
    currentOffsetInPlainBlock = (int) (newPos % plainBlockSize);
    plainStreamPosition = newPos;
  }

  @Override
  public long getPos() throws IOException {
    return plainStreamPosition;
  }

  @Override
  public int read() throws IOException {
    throw new IOException("should not be called");
  }

  @Override
  public void close() throws IOException {
    sourceStream.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    // TODO implement
  }

  @Override
  public synchronized void reset() throws IOException {
    // TODO implement
  }

  @Override
  public boolean markSupported() {
    // TODO implement
    return false;
  }
}
