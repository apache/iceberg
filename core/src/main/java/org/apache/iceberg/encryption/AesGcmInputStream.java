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
import java.util.Arrays;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class AesGcmInputStream extends SeekableInputStream {
  private final SeekableInputStream sourceStream;
  private final boolean emptyCipherStream;
  private final long netSourceFileSize;
  private final Ciphers.AesGcmDecryptor gcmDecryptor;
  private final byte[] ciphertextBlockBuffer;
  private final int cipherBlockSize;
  private final int plainBlockSize;
  private final int numberOfBlocks;
  private final int lastCipherBlockSize;
  private final long plainStreamSize;
  private final byte[] fileAadPrefix;

  private long plainStreamPosition;
  private int currentBlockIndex;
  private int currentOffsetInPlainBlock;

  AesGcmInputStream(SeekableInputStream sourceStream, long sourceLength,
                    byte[] aesKey, byte[] fileAadPrefix) throws IOException {
    this.netSourceFileSize = sourceLength - Ciphers.GCM_STREAM_PREFIX_LENGTH;
    Preconditions.checkArgument(netSourceFileSize >= 0,
        "Source length " + sourceLength + " is shorter than GCM prefix. File is not encrypted");

    this.emptyCipherStream = (0 == netSourceFileSize);
    this.sourceStream = sourceStream;
    byte[] prefixBytes = new byte[Ciphers.GCM_STREAM_PREFIX_LENGTH];
    int fetched = sourceStream.read(prefixBytes);
    Preconditions.checkState(fetched == Ciphers.GCM_STREAM_PREFIX_LENGTH,
        "Insufficient read " + fetched +
            ". The stream length should be at least " + Ciphers.GCM_STREAM_PREFIX_LENGTH);

    byte[] magic = new byte[Ciphers.GCM_STREAM_MAGIC_ARRAY.length];
    System.arraycopy(prefixBytes, 0, magic, 0, Ciphers.GCM_STREAM_MAGIC_ARRAY.length);
    Preconditions.checkState(Arrays.equals(Ciphers.GCM_STREAM_MAGIC_ARRAY, magic),
        "Cannot open encrypted file, it does not begin with magic string " + Ciphers.GCM_STREAM_MAGIC_STRING);

    if (!emptyCipherStream) {
      this.plainStreamPosition = 0;
      this.fileAadPrefix = fileAadPrefix;
      gcmDecryptor = new Ciphers.AesGcmDecryptor(aesKey);
      plainBlockSize = ByteBuffer.wrap(prefixBytes, Ciphers.GCM_STREAM_MAGIC_ARRAY.length, 4)
          .order(ByteOrder.LITTLE_ENDIAN).getInt();
      Preconditions.checkState(plainBlockSize > 0, "Wrong plainBlockSize " + plainBlockSize);

      cipherBlockSize = plainBlockSize + Ciphers.NONCE_LENGTH + Ciphers.GCM_TAG_LENGTH;
      this.ciphertextBlockBuffer = new byte[cipherBlockSize];
      this.currentBlockIndex = 0;
      this.currentOffsetInPlainBlock = 0;

      int numberOfFullBlocks = Math.toIntExact(netSourceFileSize / cipherBlockSize);
      int cipherBytesInLastBlock = Math.toIntExact(netSourceFileSize - numberOfFullBlocks * cipherBlockSize);
      boolean fullBlocksOnly = (0 == cipherBytesInLastBlock);
      numberOfBlocks = fullBlocksOnly ? numberOfFullBlocks : numberOfFullBlocks + 1;
      lastCipherBlockSize = fullBlocksOnly ? cipherBlockSize : cipherBytesInLastBlock; // never 0
      int plainBytesInLastBlock = fullBlocksOnly ? 0 :
          (cipherBytesInLastBlock - Ciphers.NONCE_LENGTH - Ciphers.GCM_TAG_LENGTH);
      plainStreamSize = numberOfFullBlocks * plainBlockSize + plainBytesInLastBlock;
    } else {
      plainStreamSize = 0;

      gcmDecryptor = null;
      ciphertextBlockBuffer = null;
      cipherBlockSize = -1;
      plainBlockSize = -1;
      numberOfBlocks = -1;
      lastCipherBlockSize = -1;
      this.fileAadPrefix = null;
    }
  }

  public long plaintextStreamSize() {
    return plainStreamSize;
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
    if (len < 0) {
      throw new IOException("Negative read length " + len);
    }

    if (available() <= 0) {
      return -1;
    }

    boolean lastBlock = (currentBlockIndex + 1 == numberOfBlocks);
    int resultBufferOffset = off;
    int remaining = len;

    sourceStream.seek(Ciphers.GCM_STREAM_PREFIX_LENGTH + currentBlockIndex * cipherBlockSize);

    while (remaining > 0) {
      int toLoad = lastBlock ? lastCipherBlockSize : cipherBlockSize;
      int loaded = sourceStream.read(ciphertextBlockBuffer, 0, toLoad);
      if (loaded != toLoad) {
        throw new IOException("Should read " + toLoad + " bytes, but got only " + loaded + " bytes");
      }

      byte[] aad = Ciphers.streamBlockAAD(fileAadPrefix, currentBlockIndex);
      byte[] plaintextBlock = gcmDecryptor.decrypt(ciphertextBlockBuffer, 0, toLoad, aad);

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
        lastBlock = (currentBlockIndex + 1 == numberOfBlocks);
      }
    }

    plainStreamPosition += len - remaining;
    return len - remaining;
  }

  @Override
  public void seek(long newPos) throws IOException {
    if (newPos < 0) {
      throw new IOException("Negative new position " + newPos);
    } else if (newPos > plainStreamSize) {
      throw new IOException("New position " + newPos + " exceeds the max stream size "  + plainStreamSize);
    }
    currentBlockIndex = Math.toIntExact(newPos / plainBlockSize);
    currentOffsetInPlainBlock = Math.toIntExact(newPos % plainBlockSize);
    plainStreamPosition = newPos;
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
  }
}
