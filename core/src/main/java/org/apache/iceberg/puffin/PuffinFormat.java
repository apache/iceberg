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
package org.apache.iceberg.puffin;

import io.airlift.compress.Compressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;

final class PuffinFormat {
  private PuffinFormat() {}

  enum Flag {
    FOOTER_PAYLOAD_COMPRESSED(0, 0),
  /**/ ;

    private static final Map<Pair<Integer, Integer>, Flag> BY_BYTE_AND_BIT =
        Stream.of(values())
            .collect(
                ImmutableMap.toImmutableMap(
                    flag -> Pair.of(flag.byteNumber(), flag.bitNumber()), Function.identity()));

    private final int byteNumber;
    private final int bitNumber;

    Flag(int byteNumber, int bitNumber) {
      Preconditions.checkArgument(
          0 <= byteNumber && byteNumber < PuffinFormat.FOOTER_STRUCT_FLAGS_LENGTH,
          "Invalid byteNumber");
      Preconditions.checkArgument(0 <= bitNumber && bitNumber < Byte.SIZE, "Invalid bitNumber");
      this.byteNumber = byteNumber;
      this.bitNumber = bitNumber;
    }

    @Nullable
    static Flag fromBit(int byteNumber, int bitNumber) {
      return BY_BYTE_AND_BIT.get(Pair.of(byteNumber, bitNumber));
    }

    public int byteNumber() {
      return byteNumber;
    }

    public int bitNumber() {
      return bitNumber;
    }
  }

  static final int FOOTER_START_MAGIC_OFFSET = 0;
  static final int FOOTER_START_MAGIC_LENGTH = getMagic().length;

  // "Footer struct" denotes the fixed-length portion of the Footer
  static final int FOOTER_STRUCT_PAYLOAD_SIZE_OFFSET = 0;
  static final int FOOTER_STRUCT_FLAGS_OFFSET = FOOTER_STRUCT_PAYLOAD_SIZE_OFFSET + 4;
  static final int FOOTER_STRUCT_FLAGS_LENGTH = 4;
  static final int FOOTER_STRUCT_MAGIC_OFFSET =
      FOOTER_STRUCT_FLAGS_OFFSET + FOOTER_STRUCT_FLAGS_LENGTH;
  static final int FOOTER_STRUCT_LENGTH = FOOTER_STRUCT_MAGIC_OFFSET + getMagic().length;

  static final PuffinCompressionCodec FOOTER_COMPRESSION_CODEC = PuffinCompressionCodec.LZ4;

  static byte[] getMagic() {
    return new byte[] {0x50, 0x46, 0x41, 0x31};
  }

  static void writeIntegerLittleEndian(OutputStream outputStream, int value) throws IOException {
    outputStream.write(0xFF & value);
    outputStream.write(0xFF & (value >> 8));
    outputStream.write(0xFF & (value >> 16));
    outputStream.write(0xFF & (value >> 24));
  }

  static int readIntegerLittleEndian(byte[] data, int offset) {
    return Byte.toUnsignedInt(data[offset])
        | (Byte.toUnsignedInt(data[offset + 1]) << 8)
        | (Byte.toUnsignedInt(data[offset + 2]) << 16)
        | (Byte.toUnsignedInt(data[offset + 3]) << 24);
  }

  static ByteBuffer compress(PuffinCompressionCodec codec, ByteBuffer input) {
    switch (codec) {
      case NONE:
        return input.duplicate();
      case LZ4:
        // TODO requires LZ4 frame compressor, e.g.
        // https://github.com/airlift/aircompressor/pull/142
        break;
      case ZSTD:
        return compress(new ZstdCompressor(), input);
    }
    throw new UnsupportedOperationException("Unsupported codec: " + codec);
  }

  private static ByteBuffer compress(Compressor compressor, ByteBuffer input) {
    ByteBuffer output = ByteBuffer.allocate(compressor.maxCompressedLength(input.remaining()));
    compressor.compress(input.duplicate(), output);
    output.flip();
    return output;
  }

  static ByteBuffer decompress(PuffinCompressionCodec codec, ByteBuffer input) {
    switch (codec) {
      case NONE:
        return input.duplicate();

      case LZ4:
        // TODO requires LZ4 frame decompressor, e.g.
        // https://github.com/airlift/aircompressor/pull/142
        break;

      case ZSTD:
        return decompressZstd(input);
    }

    throw new UnsupportedOperationException("Unsupported codec: " + codec);
  }

  private static ByteBuffer decompressZstd(ByteBuffer input) {
    byte[] inputBytes;
    int inputOffset;
    int inputLength;
    if (input.hasArray()) {
      inputBytes = input.array();
      inputOffset = input.arrayOffset();
      inputLength = input.remaining();
    } else {
      // TODO implement ZstdDecompressor.getDecompressedSize for ByteBuffer to avoid copying
      inputBytes = ByteBuffers.toByteArray(input);
      inputOffset = 0;
      inputLength = inputBytes.length;
    }

    byte[] decompressed =
        new byte
            [Math.toIntExact(
                ZstdDecompressor.getDecompressedSize(inputBytes, inputOffset, inputLength))];
    int decompressedLength =
        new ZstdDecompressor()
            .decompress(inputBytes, inputOffset, inputLength, decompressed, 0, decompressed.length);
    Preconditions.checkState(
        decompressedLength == decompressed.length, "Invalid decompressed length");
    return ByteBuffer.wrap(decompressed);
  }
}
