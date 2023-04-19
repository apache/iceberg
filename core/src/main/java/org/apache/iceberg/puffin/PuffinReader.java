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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.puffin.PuffinFormat.Flag;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.Pair;

public class PuffinReader implements Closeable {
  // Must not be modified
  private static final byte[] MAGIC = PuffinFormat.getMagic();

  private final long fileSize;
  private final SeekableInputStream input;
  private Integer knownFooterSize;
  private FileMetadata knownFileMetadata;

  PuffinReader(InputFile inputFile, @Nullable Long fileSize, @Nullable Long footerSize) {
    Preconditions.checkNotNull(inputFile, "inputFile is null");
    this.fileSize = fileSize == null ? inputFile.getLength() : fileSize;
    this.input = inputFile.newStream();
    if (footerSize != null) {
      Preconditions.checkArgument(
          0 < footerSize && footerSize <= this.fileSize - MAGIC.length,
          "Invalid footer size: %s",
          footerSize);
      this.knownFooterSize = Math.toIntExact(footerSize);
    }
  }

  public FileMetadata fileMetadata() throws IOException {
    if (knownFileMetadata == null) {
      int footerSize = footerSize();
      byte[] footer = readInput(fileSize - footerSize, footerSize);

      checkMagic(footer, PuffinFormat.FOOTER_START_MAGIC_OFFSET);
      int footerStructOffset = footerSize - PuffinFormat.FOOTER_STRUCT_LENGTH;
      checkMagic(footer, footerStructOffset + PuffinFormat.FOOTER_STRUCT_MAGIC_OFFSET);

      PuffinCompressionCodec footerCompression = PuffinCompressionCodec.NONE;
      for (Flag flag : decodeFlags(footer, footerStructOffset)) {
        switch (flag) {
          case FOOTER_PAYLOAD_COMPRESSED:
            footerCompression = PuffinFormat.FOOTER_COMPRESSION_CODEC;
            break;
          default:
            throw new IllegalStateException("Unsupported flag: " + flag);
        }
      }

      int footerPayloadSize =
          PuffinFormat.readIntegerLittleEndian(
              footer, footerStructOffset + PuffinFormat.FOOTER_STRUCT_PAYLOAD_SIZE_OFFSET);
      Preconditions.checkState(
          footerSize
              == PuffinFormat.FOOTER_START_MAGIC_LENGTH
                  + footerPayloadSize
                  + PuffinFormat.FOOTER_STRUCT_LENGTH,
          "Unexpected footer payload size value %s for footer size %s",
          footerPayloadSize,
          footerSize);

      ByteBuffer footerPayload = ByteBuffer.wrap(footer, 4, footerPayloadSize);
      ByteBuffer footerJson = PuffinFormat.decompress(footerCompression, footerPayload);
      this.knownFileMetadata = parseFileMetadata(footerJson);
    }
    return knownFileMetadata;
  }

  private Set<Flag> decodeFlags(byte[] footer, int footerStructOffset) {
    EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
    for (int byteNumber = 0; byteNumber < PuffinFormat.FOOTER_STRUCT_FLAGS_LENGTH; byteNumber++) {
      int flagByte =
          Byte.toUnsignedInt(
              footer[footerStructOffset + PuffinFormat.FOOTER_STRUCT_FLAGS_OFFSET + byteNumber]);
      int bitNumber = 0;
      while (flagByte != 0) {
        if ((flagByte & 0x1) != 0) {
          Flag flag = Flag.fromBit(byteNumber, bitNumber);
          Preconditions.checkState(
              flag != null, "Unknown flag byte %s and bit %s set", byteNumber, bitNumber);
          flags.add(flag);
        }
        flagByte = flagByte >> 1;
        bitNumber++;
      }
    }
    return flags;
  }

  public Iterable<Pair<BlobMetadata, ByteBuffer>> readAll(List<BlobMetadata> blobs) {
    if (blobs.isEmpty()) {
      return ImmutableList.of();
    }

    // TODO inspect blob offsets and coalesce read regions close to each other

    return () ->
        blobs.stream()
            .sorted(Comparator.comparingLong(BlobMetadata::offset))
            .map(
                (BlobMetadata blobMetadata) -> {
                  try {
                    input.seek(blobMetadata.offset());
                    byte[] bytes = new byte[Math.toIntExact(blobMetadata.length())];
                    ByteStreams.readFully(input, bytes);
                    ByteBuffer rawData = ByteBuffer.wrap(bytes);
                    PuffinCompressionCodec codec =
                        PuffinCompressionCodec.forName(blobMetadata.compressionCodec());
                    ByteBuffer data = PuffinFormat.decompress(codec, rawData);
                    return Pair.of(blobMetadata, data);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
            .iterator();
  }

  private static void checkMagic(byte[] data, int offset) {
    byte[] read = Arrays.copyOfRange(data, offset, offset + MAGIC.length);
    if (!Arrays.equals(read, MAGIC)) {
      throw new IllegalStateException(
          String.format(
              "Invalid file: expected magic at offset %s: %s, but got %s",
              offset, Arrays.toString(MAGIC), Arrays.toString(read)));
    }
  }

  private int footerSize() throws IOException {
    if (knownFooterSize == null) {
      Preconditions.checkState(
          fileSize >= PuffinFormat.FOOTER_STRUCT_LENGTH,
          "Invalid file: file length %s is less tha minimal length of the footer tail %s",
          fileSize,
          PuffinFormat.FOOTER_STRUCT_LENGTH);
      byte[] footerStruct =
          readInput(
              fileSize - PuffinFormat.FOOTER_STRUCT_LENGTH, PuffinFormat.FOOTER_STRUCT_LENGTH);
      checkMagic(footerStruct, PuffinFormat.FOOTER_STRUCT_MAGIC_OFFSET);

      int footerPayloadSize =
          PuffinFormat.readIntegerLittleEndian(
              footerStruct, PuffinFormat.FOOTER_STRUCT_PAYLOAD_SIZE_OFFSET);
      knownFooterSize =
          PuffinFormat.FOOTER_START_MAGIC_LENGTH
              + footerPayloadSize
              + PuffinFormat.FOOTER_STRUCT_LENGTH;
    }
    return knownFooterSize;
  }

  private byte[] readInput(long offset, int length) throws IOException {
    byte[] data = new byte[length];
    if (input instanceof RangeReadable) {
      ((RangeReadable) input).readFully(offset, data);
    } else {
      input.seek(offset);
      ByteStreams.readFully(input, data);
    }
    return data;
  }

  private static FileMetadata parseFileMetadata(ByteBuffer data) {
    String footerJson = StandardCharsets.UTF_8.decode(data).toString();
    return FileMetadataParser.fromJson(footerJson);
  }

  @Override
  public void close() throws IOException {
    input.close();
    knownFooterSize = null;
    knownFileMetadata = null;
  }
}
