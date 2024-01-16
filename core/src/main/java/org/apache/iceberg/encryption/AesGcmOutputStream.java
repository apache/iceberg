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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

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
  private final byte[] plainBlock;
  private final byte[] cipherBlock;

  private int positionInPlainBlock;
  private int currentBlockIndex;
  private boolean isHeaderWritten;
  private boolean lastBlockWritten;
  private boolean isClosed;
  private long finalPosition;

  AesGcmOutputStream(PositionOutputStream targetStream, byte[] aesKey, byte[] fileAadPrefix) {
    this.targetStream = targetStream;
    this.gcmEncryptor = new Ciphers.AesGcmEncryptor(aesKey);
    this.fileAadPrefix = fileAadPrefix;
    this.singleByte = new byte[1];
    this.plainBlock = new byte[Ciphers.PLAIN_BLOCK_SIZE];
    this.cipherBlock = new byte[Ciphers.CIPHER_BLOCK_SIZE];
    this.positionInPlainBlock = 0;
    this.currentBlockIndex = 0;
    this.isHeaderWritten = false;
    this.lastBlockWritten = false;
    this.isClosed = false;
    this.finalPosition = 0;
  }

  @Override
  public void write(int b) throws IOException {
    singleByte[0] = (byte) (b & 0x000000FF);
    write(singleByte);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (isClosed) {
      throw new IOException("Writing to closed stream");
    }

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

      if (positionInPlainBlock == plainBlock.length) {
        encryptAndWriteBlock();
      }
    }
  }

  @Override
  public long getPos() throws IOException {
    if (isClosed) {
      return finalPosition;
    }

    return (long) currentBlockIndex * Ciphers.PLAIN_BLOCK_SIZE + positionInPlainBlock;
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

    finalPosition = getPos();
    isClosed = true;

    encryptAndWriteBlock();

    targetStream.close();
  }

  private void writeHeader() throws IOException {
    targetStream.write(HEADER_BYTES);
    isHeaderWritten = true;
  }

  private void encryptAndWriteBlock() throws IOException {
    Preconditions.checkState(
        !lastBlockWritten, "Cannot encrypt block: a partial block has already been written");

    if (currentBlockIndex == Integer.MAX_VALUE) {
      throw new IOException("Cannot write block: exceeded Integer.MAX_VALUE blocks");
    }

    if (positionInPlainBlock == 0 && currentBlockIndex != 0) {
      return;
    }

    if (positionInPlainBlock != plainBlock.length) {
      // signal that a partial block has been written and must be the last
      this.lastBlockWritten = true;
    }

    byte[] aad = Ciphers.streamBlockAAD(fileAadPrefix, currentBlockIndex);
    int ciphertextLength =
        gcmEncryptor.encrypt(plainBlock, 0, positionInPlainBlock, cipherBlock, 0, aad);
    targetStream.write(cipherBlock, 0, ciphertextLength);
    positionInPlainBlock = 0;
    currentBlockIndex++;
  }
}
