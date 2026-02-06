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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class AesGcmInputStream extends SeekableInputStream {
  private final SeekableInputStream sourceStream;
  private final byte[] fileAADPrefix;
  private final Ciphers.AesGcmDecryptor decryptor;
  private final byte[] cipherBlockBuffer;
  private final byte[] currentPlainBlock;
  private final long numBlocks;
  private final int lastCipherBlockSize;
  private final long plainStreamSize;
  private final byte[] singleByte;

  private long plainStreamPosition;
  private long currentPlainBlockIndex;
  private int currentPlainBlockSize;

  AesGcmInputStream(
      SeekableInputStream sourceStream, long sourceLength, byte[] aesKey, byte[] fileAADPrefix) {
    this.sourceStream = sourceStream;
    this.fileAADPrefix = fileAADPrefix;
    this.decryptor = new Ciphers.AesGcmDecryptor(aesKey);
    this.cipherBlockBuffer = new byte[Ciphers.CIPHER_BLOCK_SIZE];
    this.currentPlainBlock = new byte[Ciphers.PLAIN_BLOCK_SIZE];
    this.plainStreamPosition = 0;
    this.currentPlainBlockIndex = -1;
    this.currentPlainBlockSize = 0;

    long streamLength = sourceLength - Ciphers.GCM_STREAM_HEADER_LENGTH;
    long numFullBlocks = Math.toIntExact(streamLength / Ciphers.CIPHER_BLOCK_SIZE);
    long cipherFullBlockLength = numFullBlocks * Ciphers.CIPHER_BLOCK_SIZE;
    int cipherBytesInLastBlock = Math.toIntExact(streamLength - cipherFullBlockLength);
    boolean fullBlocksOnly = (0 == cipherBytesInLastBlock);
    this.numBlocks = fullBlocksOnly ? numFullBlocks : numFullBlocks + 1;
    this.lastCipherBlockSize =
        fullBlocksOnly ? Ciphers.CIPHER_BLOCK_SIZE : cipherBytesInLastBlock; // never 0

    long lastPlainBlockSize =
        (long) lastCipherBlockSize - Ciphers.NONCE_LENGTH - Ciphers.GCM_TAG_LENGTH;
    this.plainStreamSize =
        numFullBlocks * Ciphers.PLAIN_BLOCK_SIZE + (fullBlocksOnly ? 0 : lastPlainBlockSize);
    this.singleByte = new byte[1];
  }

  private void validateHeader() throws IOException {
    byte[] headerBytes = new byte[Ciphers.GCM_STREAM_HEADER_LENGTH];
    IOUtil.readFully(sourceStream, headerBytes, 0, headerBytes.length);

    Preconditions.checkState(
        Ciphers.GCM_STREAM_MAGIC.equals(ByteBuffer.wrap(headerBytes, 0, 4)),
        "Invalid GCM stream: magic does not match AGS1");

    int plainBlockSize = ByteBuffer.wrap(headerBytes, 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    Preconditions.checkState(
        plainBlockSize == Ciphers.PLAIN_BLOCK_SIZE,
        "Invalid GCM stream: block size %d != %d",
        plainBlockSize,
        Ciphers.PLAIN_BLOCK_SIZE);
  }

  @Override
  public int available() {
    long maxAvailable = plainStreamSize - plainStreamPosition;
    // See InputStream.available contract
    if (maxAvailable >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) maxAvailable;
    }
  }

  private int availableInCurrentBlock() {
    if (blockIndex(plainStreamPosition) != currentPlainBlockIndex) {
      return 0;
    }

    return currentPlainBlockSize - offsetInBlock(plainStreamPosition);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(len >= 0, "Invalid read length: " + len);

    if (currentPlainBlockIndex < 0) {
      decryptBlock(0);
    }

    if (available() <= 0 && len > 0) {
      return -1;
    }

    if (len == 0) {
      return 0;
    }

    int totalBytesRead = 0;
    int resultBufferOffset = off;
    int remainingBytesToRead = len;

    while (remainingBytesToRead > 0) {
      int availableInBlock = availableInCurrentBlock();
      if (availableInBlock > 0) {
        int bytesToCopy = Math.min(availableInBlock, remainingBytesToRead);
        int offsetInBlock = offsetInBlock(plainStreamPosition);
        System.arraycopy(currentPlainBlock, offsetInBlock, b, resultBufferOffset, bytesToCopy);
        totalBytesRead += bytesToCopy;
        remainingBytesToRead -= bytesToCopy;
        resultBufferOffset += bytesToCopy;
        this.plainStreamPosition += bytesToCopy;
      } else if (available() > 0) {
        decryptBlock(blockIndex(plainStreamPosition));

      } else {
        break;
      }
    }

    // return -1 for EOF
    return totalBytesRead > 0 ? totalBytesRead : -1;
  }

  @Override
  public void seek(long newPos) throws IOException {
    if (newPos < 0) {
      throw new IOException("Invalid position: " + newPos);
    } else if (newPos > plainStreamSize) {
      throw new EOFException(
          "Invalid position: " + newPos + " > stream length, " + plainStreamSize);
    }

    this.plainStreamPosition = newPos;
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      return 0;
    }

    long bytesLeftInStream = plainStreamSize - plainStreamPosition;
    if (n > bytesLeftInStream) {
      // skip the rest of the stream
      this.plainStreamPosition = plainStreamSize;
      return bytesLeftInStream;
    }

    this.plainStreamPosition += n;

    return n;
  }

  @Override
  public long getPos() throws IOException {
    return plainStreamPosition;
  }

  @Override
  public int read() throws IOException {
    int read = read(singleByte);
    if (read == -1) {
      return -1;
    }

    return singleByte[0] >= 0 ? singleByte[0] : 256 + singleByte[0];
  }

  @Override
  public void close() throws IOException {
    sourceStream.close();
  }

  private void decryptBlock(long blockIndex) throws IOException {
    if (blockIndex == currentPlainBlockIndex) {
      return;
    }

    long blockPositionInStream = blockOffset(blockIndex);
    if (sourceStream.getPos() != blockPositionInStream) {
      if (sourceStream.getPos() == 0) {
        validateHeader();
      }

      sourceStream.seek(blockPositionInStream);
    }

    boolean isLastBlock = blockIndex == numBlocks - 1;
    int cipherBlockSize = isLastBlock ? lastCipherBlockSize : Ciphers.CIPHER_BLOCK_SIZE;
    IOUtil.readFully(sourceStream, cipherBlockBuffer, 0, cipherBlockSize);

    byte[] blockAAD = Ciphers.streamBlockAAD(fileAADPrefix, Math.toIntExact(blockIndex));
    decryptor.decrypt(cipherBlockBuffer, 0, cipherBlockSize, currentPlainBlock, 0, blockAAD);
    this.currentPlainBlockSize = cipherBlockSize - Ciphers.NONCE_LENGTH - Ciphers.GCM_TAG_LENGTH;
    this.currentPlainBlockIndex = blockIndex;
  }

  private static long blockIndex(long plainPosition) {
    return plainPosition / Ciphers.PLAIN_BLOCK_SIZE;
  }

  private static int offsetInBlock(long plainPosition) {
    return Math.toIntExact(plainPosition % Ciphers.PLAIN_BLOCK_SIZE);
  }

  private static long blockOffset(long blockIndex) {
    return blockIndex * Ciphers.CIPHER_BLOCK_SIZE + Ciphers.GCM_STREAM_HEADER_LENGTH;
  }

  static long calculatePlaintextLength(long sourceLength) {
    long streamLength = sourceLength - Ciphers.GCM_STREAM_HEADER_LENGTH;

    if (streamLength == 0) {
      return 0;
    }

    long numberOfFullBlocks = streamLength / Ciphers.CIPHER_BLOCK_SIZE;
    long fullBlockSize = numberOfFullBlocks * Ciphers.CIPHER_BLOCK_SIZE;
    long cipherBytesInLastBlock = streamLength - fullBlockSize;
    boolean fullBlocksOnly = (0 == cipherBytesInLastBlock);
    long plainBytesInLastBlock =
        fullBlocksOnly
            ? 0
            : (cipherBytesInLastBlock - Ciphers.NONCE_LENGTH - Ciphers.GCM_TAG_LENGTH);

    return (numberOfFullBlocks * Ciphers.PLAIN_BLOCK_SIZE) + plainBytesInLastBlock;
  }
}
