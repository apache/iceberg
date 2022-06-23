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
import org.apache.iceberg.io.PositionOutputStream;

public class AesGcmOutputStream extends PositionOutputStream {
  public static final int plainBlockSize = 1024 * 1024;

  private final Ciphers.AesGcmEncryptor gcmEncryptor;
  private final PositionOutputStream targetStream;
  private final byte[] plaintextBlockBuffer;
  private final byte[] fileAadPrefix;

  private int positionInBuffer;
  private long streamPosition;
  private int currentBlockIndex;

  AesGcmOutputStream(PositionOutputStream targetStream, byte[] aesKey, byte[] fileAadPrefix) throws IOException {
    this.targetStream = targetStream;
    this.gcmEncryptor = new Ciphers.AesGcmEncryptor(aesKey);
    this.plaintextBlockBuffer = new byte[plainBlockSize];
    this.positionInBuffer = 0;
    this.streamPosition = 0;
    this.currentBlockIndex = 0;
    this.fileAadPrefix = fileAadPrefix;

    byte[] prefixBytes = ByteBuffer.allocate(Ciphers.GCM_STREAM_PREFIX_LENGTH).order(ByteOrder.LITTLE_ENDIAN)
        .put(Ciphers.GCM_STREAM_MAGIC_ARRAY)
        .putInt(plainBlockSize)
        .array();
    targetStream.write(prefixBytes);
  }

  @Override
  public void write(int b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b.length - off < len) {
      throw new IOException("Insufficient bytes in buffer: " + b.length + " - " + off + " < " + len);
    }
    int remaining = len;
    int offset = off;

    while (remaining > 0) {
      int freeBlockBytes = plainBlockSize - positionInBuffer;
      int toWrite = freeBlockBytes <= remaining ? freeBlockBytes : remaining;

      System.arraycopy(b, offset, plaintextBlockBuffer, positionInBuffer, toWrite);
      positionInBuffer += toWrite;
      if (positionInBuffer == plainBlockSize) {
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
    byte[] aad = Ciphers.streamBlockAAD(fileAadPrefix, currentBlockIndex);
    byte[] cipherText = gcmEncryptor.encrypt(plaintextBlockBuffer, 0, positionInBuffer, aad);
    currentBlockIndex++;
    targetStream.write(cipherText);
  }
}
