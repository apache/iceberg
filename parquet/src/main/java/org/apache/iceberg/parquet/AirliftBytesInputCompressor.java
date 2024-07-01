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
package org.apache.iceberg.parquet;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class AirliftBytesInputCompressor
    implements CompressionCodecFactory.BytesInputCompressor,
        CompressionCodecFactory.BytesInputDecompressor {

  private final CompressionCodecName codecName;
  private final Compressor compressor;
  private final Decompressor decompressor;
  private final ByteBufferAllocator allocator;
  private final Deque<ByteBuffer> allocatedBuffers;

  public AirliftBytesInputCompressor(CompressionCodecName codecName) {
    this(codecName, new HeapByteBufferAllocator());
  }

  public AirliftBytesInputCompressor(
      CompressionCodecName codecName, ByteBufferAllocator allocator) {
    this.codecName = codecName;

    switch (codecName) {
      case LZ4:
        compressor = new Lz4Compressor();
        decompressor = new Lz4Decompressor();
        break;
      case LZO:
        compressor = new LzoCompressor();
        decompressor = new LzoDecompressor();
        break;
      case SNAPPY:
        compressor = new SnappyCompressor();
        decompressor = new SnappyDecompressor();
        break;
      case ZSTD:
        compressor = new ZstdCompressor();
        decompressor = new ZstdDecompressor();
        break;
      default:
        throw new UnsupportedOperationException(
            "Add Hadoop to the classpath, compression not supported by Airlift: " + codecName);
    }

    this.allocator = allocator;
    this.allocatedBuffers = new ArrayDeque<>();
  }

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    ByteBuffer inBuf = bytes.toByteBuffer();

    int maxOutLen = compressor.maxCompressedLength((int) bytes.size());
    ByteBuffer outBuf = allocator.allocate(maxOutLen);

    this.allocatedBuffers.push(outBuf);
    compressor.compress(inBuf, outBuf);

    return BytesInput.from((ByteBuffer) outBuf.flip());
  }

  @Override
  public CompressionCodecName getCodecName() {
    return codecName;
  }

  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    ByteBuffer inBuf = bytes.toByteBuffer();
    ByteBuffer outBuf = allocator.allocate(uncompressedSize);
    this.allocatedBuffers.push(outBuf);
    decompressor.decompress(inBuf, outBuf);
    return BytesInput.from((ByteBuffer) outBuf.flip());
  }

  @Override
  public void decompress(
      ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
      throws IOException {
    decompressor.decompress(input, output);
  }

  @Override
  public void release() {
    while (!allocatedBuffers.isEmpty()) {
      allocator.release(allocatedBuffers.pop());
    }
  }
}
