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

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.Snappy;

/** Compression codecs for Parquet pages that do not depend on Hadoop. */
@SuppressWarnings("deprecation")
class NativeCodecFactory implements CompressionCodecFactory {

  @Override
  public BytesInputCompressor getCompressor(CompressionCodecName codec) {
    checkSupported(codec);
    return new Compressor(codec);
  }

  @Override
  public BytesInputDecompressor getDecompressor(CompressionCodecName codec) {
    checkSupported(codec);
    return new Decompressor(codec);
  }

  @Override
  public void release() {}

  private static void checkSupported(CompressionCodecName codec) {
    switch (codec) {
      case UNCOMPRESSED, GZIP, SNAPPY, ZSTD -> {}
      default ->
          throw new UnsupportedOperationException(
              "Parquet codec not supported without Hadoop: " + codec);
    }
  }

  private static class Compressor implements BytesInputCompressor {
    private final CompressionCodecName codec;

    Compressor(CompressionCodecName codec) {
      this.codec = codec;
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      return switch (codec) {
        case UNCOMPRESSED -> bytes;
        case GZIP -> {
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            bytes.writeAllTo(gzip);
          }
          yield BytesInput.from(out.toByteArray());
        }
        case SNAPPY -> BytesInput.from(Snappy.compress(bytes.toByteArray()));
        case ZSTD -> BytesInput.from(Zstd.compress(bytes.toByteArray()));
        default -> throw new UnsupportedOperationException("Unsupported codec: " + codec);
      };
    }

    @Override
    public CompressionCodecName getCodecName() {
      return codec;
    }

    @Override
    public void release() {}
  }

  private static class Decompressor implements BytesInputDecompressor {
    private final CompressionCodecName codec;

    Decompressor(CompressionCodecName codec) {
      this.codec = codec;
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      return switch (codec) {
        case UNCOMPRESSED -> bytes;
        case GZIP -> {
          try (GZIPInputStream gzip =
              new GZIPInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            yield BytesInput.from(gzip.readNBytes(uncompressedSize));
          }
        }
        case SNAPPY -> BytesInput.from(Snappy.uncompress(bytes.toByteArray()));
        case ZSTD -> BytesInput.from(Zstd.decompress(bytes.toByteArray(), uncompressedSize));
        default -> throw new UnsupportedOperationException("Unsupported codec: " + codec);
      };
    }

    @Override
    public void decompress(
        ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
        throws IOException {
      byte[] compressed = new byte[compressedSize];
      input.get(compressed);
      output.put(decompress(BytesInput.from(compressed), uncompressedSize).toByteArray());
    }

    @Override
    public void release() {}
  }
}
