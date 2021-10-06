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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.iceberg.io.PositionOutputStream;

public class AesGcmOutputStream extends PositionOutputStream {
  // AES-GCM parameters
  public static final int GCM_NONCE_LENGTH = 12; // in bytes
  public static final int GCM_TAG_LENGTH = 16; // in bytes
  public static final int GCM_TAG_LENGTH_BITS = 8 * GCM_TAG_LENGTH;

  static final int HEADER_SIZE_LENGTH = 4;

  private PositionOutputStream targetStream;

  private Cipher gcmCipher;
  private SecureRandom random;
  private SecretKey key;
  private byte[] nonce;

  private int blockSize = 1024 * 1024; // TODO Make configurable
  private byte[] plaintextBlockBuffer;
  private int positionInBuffer;
  private long streamPosition;
  private int currentBlockIndex;
  private byte[] fileAadPrefix;

  AesGcmOutputStream(PositionOutputStream targetStream, byte[] aesKey, byte[] fileAadPrefix) throws IOException {
    this.targetStream = targetStream;

    try {
      gcmCipher = Cipher.getInstance("AES/GCM/NoPadding");
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
    this.random = new SecureRandom();
    this.nonce = new byte[GCM_NONCE_LENGTH];
    this.key = new SecretKeySpec(aesKey, "AES");
    this.plaintextBlockBuffer = new byte[blockSize];
    this.positionInBuffer = 0;
    this.streamPosition = 0;
    this.currentBlockIndex = 0;
    this.fileAadPrefix = fileAadPrefix;

    byte[] blockSizeBytes = ByteBuffer.allocate(HEADER_SIZE_LENGTH).order(ByteOrder.LITTLE_ENDIAN).putInt(blockSize).array();
    targetStream.write(blockSizeBytes);
  }

  @Override
  public void write(int b) throws IOException {
    throw new IOException("should not be called");
  }

  @Override
  public void write(byte[] b)  throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    int remaining = len;
    int offset = off;

    while (remaining > 0) {
      int freeBlockBytes = blockSize - positionInBuffer;
      int toWrite = freeBlockBytes <= remaining ? freeBlockBytes : remaining;

      System.arraycopy(b, offset, plaintextBlockBuffer, positionInBuffer, toWrite);
      positionInBuffer += toWrite;
      if (positionInBuffer == blockSize) {
        encryptAndWriteBlock();
        positionInBuffer = 0;
      }
      offset += toWrite;
      remaining -= toWrite;
    }

    streamPosition += len;
  }

  @Override
  public long getPos() throws IOException {
    return streamPosition;
  }

  @Override
  public void flush() throws IOException {
    targetStream.flush();
  }

  @Override
  public void close() throws IOException {
    if (positionInBuffer > 0) {
      encryptAndWriteBlock();
    }
    targetStream.close();
  }

  private void encryptAndWriteBlock() throws IOException {
    random.nextBytes(nonce);
    GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
    // TODO byte[] aaD = calculateAAD(fileAadPrefix, currentBlockIndex);
    try {
      gcmCipher.init(Cipher.ENCRYPT_MODE, key, spec);
    } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
      throw new IOException("Failed to init GCM cipher", e);
    }

    byte[] cipherText;
    try {
      cipherText = gcmCipher.doFinal(plaintextBlockBuffer, 0, positionInBuffer);
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to encrypt", e);
    }

    currentBlockIndex++;

    targetStream.write(nonce);
    targetStream.write(cipherText);
  }
}
