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

  private static final byte[] HEADER_BYTES =
      ByteBuffer.allocate(Ciphers.GCM_STREAM_HEADER_LENGTH)
          .order(ByteOrder.LITTLE_ENDIAN)
          .put(Ciphers.GCM_STREAM_MAGIC_ARRAY)
          .putInt(Ciphers.PLAIN_BLOCK_SIZE)
          .array();
  private final Ciphers.AesGcmEncryptor gcmEncryptor;
  private final PositionOutputStream targetStream;
  private final byte[] fileAadPrefix;
  private final byte[] singleByte;

  private byte[] plainBlock;
  private byte[] cipherBlock;
  private int positionInPlainBlock;
  private long streamPosition;
  private int currentBlockIndex;
  private boolean isHeaderWritten;

  AesGcmOutputStream(PositionOutputStream targetStream, byte[] aesKey, byte[] fileAadPrefix) {
    this.targetStream = targetStream;
    this.gcmEncryptor = new Ciphers.AesGcmEncryptor(aesKey);
    this.plainBlock = new byte[Ciphers.PLAIN_BLOCK_SIZE];
    this.cipherBlock = new byte[Ciphers.CIPHER_BLOCK_SIZE];
    this.positionInPlainBlock = 0;
    this.streamPosition = 0;
    this.currentBlockIndex = 0;
    this.fileAadPrefix = fileAadPrefix;
    this.isHeaderWritten = false;
    this.singleByte = new byte[1];
  }

  @Override
  public void write(int b) throws IOException {
    singleByte[0] = (byte) (b & 0x000000FF);
    write(singleByte);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (!isHeaderWritten) {
      writeHeader();
    }

    if (b.length - off < len) {
      throw new IOException(
          "Insufficient bytes in buffer: " + b.length + " - " + off + " < " + len);
    }

    int remaining = len;
    int offset = off;

    while (remaining > 0) {
      int freeBlockBytes = plainBlock.length - positionInPlainBlock;
      int toWrite = Math.min(freeBlockBytes, remaining);

      System.arraycopy(b, offset, plainBlock, positionInPlainBlock, toWrite);
      positionInPlainBlock += toWrite;
      offset += toWrite;
      remaining -= toWrite;

      if (positionInPlainBlock == Ciphers.PLAIN_BLOCK_SIZE) {
        encryptAndWriteBlock();
      }
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
    if (!isHeaderWritten) {
      writeHeader();
    }

    if (positionInPlainBlock > 0) {
      encryptAndWriteBlock();
    }

    targetStream.close();
    plainBlock = null;
    cipherBlock = null;
  }

  private void writeHeader() throws IOException {

    targetStream.write(HEADER_BYTES);
    isHeaderWritten = true;
  }

  private void encryptAndWriteBlock() throws IOException {
    if (currentBlockIndex == Integer.MAX_VALUE) {
      throw new IOException("Cannot write block: exceeded Integer.MAX_VALUE blocks");
    }

    if (positionInPlainBlock == 0) {
      throw new IOException("Empty plain block");
    }

    byte[] aad = Ciphers.streamBlockAAD(fileAadPrefix, currentBlockIndex);
    int ciphertextLength =
        gcmEncryptor.encrypt(plainBlock, 0, positionInPlainBlock, cipherBlock, 0, aad);
    targetStream.write(cipherBlock, 0, ciphertextLength);
    positionInPlainBlock = 0;
    currentBlockIndex++;
  }
}
