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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class AesGcmInputStream extends SeekableInputStream {
  private final SeekableInputStream sourceStream;
  private final Ciphers.AesGcmDecryptor gcmDecryptor;
  private final byte[] cipherBlockBuffer;
  private final int cipherBlockSize;
  private final int plainBlockSize;
  private final int numberOfBlocks;
  private final int lastCipherBlockSize;
  private final long plainStreamSize;
  private final byte[] fileAADPrefix;

  private long plainStreamPosition;
  private int currentBlockIndex;
  private int currentOffsetInPlainBlock;
  private byte[] currentDecryptedBlock;
  private int currentDecryptedBlockIndex;

  AesGcmInputStream(
      SeekableInputStream sourceStream, long sourceLength, byte[] aesKey, byte[] fileAADPrefix)
      throws IOException {
    long netSourceLength = netSourceFileLength(sourceLength);
    boolean emptyCipherStream = (0 == netSourceLength);
    this.sourceStream = sourceStream;
    byte[] headerBytes = new byte[Ciphers.GCM_STREAM_HEADER_LENGTH];
    IOUtil.readFully(sourceStream, headerBytes, 0, headerBytes.length);
    byte[] magic = new byte[Ciphers.GCM_STREAM_MAGIC_ARRAY.length];
    System.arraycopy(headerBytes, 0, magic, 0, Ciphers.GCM_STREAM_MAGIC_ARRAY.length);
    Preconditions.checkState(
        Arrays.equals(Ciphers.GCM_STREAM_MAGIC_ARRAY, magic),
        "Cannot open encrypted file, it does not begin with magic string "
            + Ciphers.GCM_STREAM_MAGIC_STRING);
    this.currentDecryptedBlockIndex = -1;

    if (!emptyCipherStream) {
      this.plainStreamPosition = 0;
      this.fileAADPrefix = fileAADPrefix;
      gcmDecryptor = new Ciphers.AesGcmDecryptor(aesKey);
      plainBlockSize =
          ByteBuffer.wrap(headerBytes, Ciphers.GCM_STREAM_MAGIC_ARRAY.length, 4)
              .order(ByteOrder.LITTLE_ENDIAN)
              .getInt();
      Preconditions.checkState(plainBlockSize > 0, "Wrong plainBlockSize " + plainBlockSize);

      Preconditions.checkState(
          plainBlockSize == AesGcmOutputStream.plainBlockSize,
          "Wrong plainBlockSize "
              + plainBlockSize
              + ". Only size of "
              + AesGcmOutputStream.plainBlockSize
              + " is currently supported");

      cipherBlockSize = plainBlockSize + Ciphers.NONCE_LENGTH + Ciphers.GCM_TAG_LENGTH;
      this.cipherBlockBuffer = new byte[cipherBlockSize];
      this.currentBlockIndex = 0;
      this.currentOffsetInPlainBlock = 0;

      int numberOfFullBlocks = Math.toIntExact(netSourceLength / cipherBlockSize);
      int cipherBytesInLastBlock =
          Math.toIntExact(netSourceLength - numberOfFullBlocks * cipherBlockSize);
      boolean fullBlocksOnly = (0 == cipherBytesInLastBlock);
      numberOfBlocks = fullBlocksOnly ? numberOfFullBlocks : numberOfFullBlocks + 1;
      lastCipherBlockSize = fullBlocksOnly ? cipherBlockSize : cipherBytesInLastBlock; // never 0
      plainStreamSize = calculatePlaintextLength(sourceLength, plainBlockSize);
    } else {
      plainStreamSize = 0;

      gcmDecryptor = null;
      cipherBlockBuffer = null;
      cipherBlockSize = -1;
      plainBlockSize = -1;
      numberOfBlocks = -1;
      lastCipherBlockSize = -1;
      this.fileAADPrefix = null;
    }
  }

  @Override
  public int available() throws IOException {
    long maxAvailable = plainStreamSize - plainStreamPosition;
    // See InputStream.available contract
    if (maxAvailable >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) maxAvailable;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(len >= 0, "Negative read length " + len);

    if (available() <= 0 && len > 0) {
      throw new EOFException();
    }

    if (len == 0) {
      return 0;
    }

    boolean isLastBlockInStream = (currentBlockIndex + 1 == numberOfBlocks);
    int resultBufferOffset = off;
    int remainingBytesToRead = len;

    while (remainingBytesToRead > 0) {
      byte[] plainBlock = decryptNextBlock(isLastBlockInStream);

      int remainingBytesInBlock = plainBlock.length - currentOffsetInPlainBlock;
      boolean finishTheBlock = remainingBytesToRead >= remainingBytesInBlock;
      int bytesToCopy = finishTheBlock ? remainingBytesInBlock : remainingBytesToRead;
      System.arraycopy(plainBlock, currentOffsetInPlainBlock, b, resultBufferOffset, bytesToCopy);
      remainingBytesToRead -= bytesToCopy;
      resultBufferOffset += bytesToCopy;
      currentOffsetInPlainBlock += bytesToCopy;

      boolean endOfStream = isLastBlockInStream && finishTheBlock;

      if (endOfStream) {
        break;
      }

      if (finishTheBlock) {
        currentBlockIndex++;
        currentOffsetInPlainBlock = 0;
        isLastBlockInStream = (currentBlockIndex + 1 == numberOfBlocks);
      }
    }

    plainStreamPosition += len - remainingBytesToRead;
    return len - remainingBytesToRead;
  }

  @Override
  public void seek(long newPos) throws IOException {
    if (newPos < 0) {
      throw new IOException("Negative new position " + newPos);
    } else if (newPos > plainStreamSize) {
      throw new IOException(
          "New position " + newPos + " exceeds the max stream size " + plainStreamSize);
    }

    currentBlockIndex = Math.toIntExact(newPos / plainBlockSize);
    currentOffsetInPlainBlock = Math.toIntExact(newPos % plainBlockSize);
    plainStreamPosition = newPos;
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      return 0;
    }

    if (plainStreamPosition == plainStreamSize) {
      return 0;
    }

    long newPosition = plainStreamPosition + n;

    if (newPosition > plainStreamSize) {
      long skipped = plainStreamSize - plainStreamPosition;
      try {
        seek(plainStreamSize);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return skipped;
    }

    try {
      seek(newPosition);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return n;
  }

  @Override
  public long getPos() throws IOException {
    return plainStreamPosition;
  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    sourceStream.close();
    currentDecryptedBlock = null;
  }

  static long calculatePlaintextLength(long sourceLength, int plainBlockSize) {
    long netSourceFileLength = netSourceFileLength(sourceLength);

    if (netSourceFileLength == 0) {
      return 0;
    }

    int cipherBlockSize = plainBlockSize + Ciphers.NONCE_LENGTH + Ciphers.GCM_TAG_LENGTH;
    int numberOfFullBlocks = Math.toIntExact(netSourceFileLength / cipherBlockSize);
    int cipherBytesInLastBlock =
        Math.toIntExact(netSourceFileLength - numberOfFullBlocks * cipherBlockSize);
    boolean fullBlocksOnly = (0 == cipherBytesInLastBlock);
    int plainBytesInLastBlock =
        fullBlocksOnly
            ? 0
            : (cipherBytesInLastBlock - Ciphers.NONCE_LENGTH - Ciphers.GCM_TAG_LENGTH);

    return (long) numberOfFullBlocks * plainBlockSize + plainBytesInLastBlock;
  }

  private byte[] decryptNextBlock(boolean isLastBlockInStream) throws IOException {
    if (currentBlockIndex == currentDecryptedBlockIndex) {
      return currentDecryptedBlock;
    }

    long blockPositionInStream = blockOffset(currentBlockIndex);
    if (sourceStream.getPos() != blockPositionInStream) {
      sourceStream.seek(blockPositionInStream);
    }

    int currentCipherBlockSize = isLastBlockInStream ? lastCipherBlockSize : cipherBlockSize;
    IOUtil.readFully(sourceStream, cipherBlockBuffer, 0, currentCipherBlockSize);

    byte[] aad = Ciphers.streamBlockAAD(fileAADPrefix, currentBlockIndex);
    byte[] result = gcmDecryptor.decrypt(cipherBlockBuffer, 0, currentCipherBlockSize, aad);
    currentDecryptedBlockIndex = currentBlockIndex;
    currentDecryptedBlock = result;
    return result;
  }

  private long blockOffset(int blockIndex) {
    return (long) blockIndex * cipherBlockSize + Ciphers.GCM_STREAM_HEADER_LENGTH;
  }

  private static long netSourceFileLength(long sourceFileLength) {
    long netSourceLength = sourceFileLength - Ciphers.GCM_STREAM_HEADER_LENGTH;
    Preconditions.checkArgument(
        netSourceLength >= 0,
        "Source length " + sourceFileLength + " is shorter than GCM prefix. File is not encrypted");

    return netSourceLength;
  }
}
