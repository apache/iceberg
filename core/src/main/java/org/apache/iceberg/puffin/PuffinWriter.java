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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.puffin.PuffinFormat.Flag;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class PuffinWriter implements FileAppender<Blob> {
  // Must not be modified
  private static final byte[] MAGIC = PuffinFormat.getMagic();

  private final PositionOutputStream outputStream;
  private final Map<String, String> properties;
  private final PuffinCompressionCodec footerCompression;
  private final PuffinCompressionCodec defaultBlobCompression;

  private final List<BlobMetadata> writtenBlobsMetadata = Lists.newArrayList();
  private boolean headerWritten;
  private boolean finished;
  private Optional<Integer> footerSize = Optional.empty();
  private Optional<Long> fileSize = Optional.empty();

  PuffinWriter(
      OutputFile outputFile,
      Map<String, String> properties,
      boolean compressFooter,
      PuffinCompressionCodec defaultBlobCompression) {
    Preconditions.checkNotNull(outputFile, "outputFile is null");
    Preconditions.checkNotNull(properties, "properties is null");
    Preconditions.checkNotNull(defaultBlobCompression, "defaultBlobCompression is null");
    this.outputStream = outputFile.create();
    this.properties = ImmutableMap.copyOf(properties);
    this.footerCompression =
        compressFooter ? PuffinFormat.FOOTER_COMPRESSION_CODEC : PuffinCompressionCodec.NONE;
    this.defaultBlobCompression = defaultBlobCompression;
  }

  @Override
  public void add(Blob blob) {
    Preconditions.checkNotNull(blob, "blob is null");
    checkNotFinished();
    try {
      writeHeaderIfNeeded();
      long fileOffset = outputStream.getPos();
      PuffinCompressionCodec codec =
          MoreObjects.firstNonNull(blob.requestedCompression(), defaultBlobCompression);
      ByteBuffer rawData = PuffinFormat.compress(codec, blob.blobData());
      int length = rawData.remaining();
      IOUtil.writeFully(outputStream, rawData);
      writtenBlobsMetadata.add(
          new BlobMetadata(
              blob.type(),
              blob.inputFields(),
              blob.snapshotId(),
              blob.sequenceNumber(),
              fileOffset,
              length,
              codec.codecName(),
              blob.properties()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Metrics metrics() {
    return new Metrics();
  }

  @Override
  public long length() {
    return fileSize();
  }

  @Override
  public void close() throws IOException {
    if (!finished) {
      finish();
    }

    outputStream.close();
  }

  private void writeHeaderIfNeeded() throws IOException {
    if (headerWritten) {
      return;
    }

    this.outputStream.write(MAGIC);
    this.headerWritten = true;
  }

  public void finish() throws IOException {
    checkNotFinished();
    writeHeaderIfNeeded();
    Preconditions.checkState(!footerSize.isPresent(), "footerSize already set");
    long footerOffset = outputStream.getPos();
    writeFooter();
    this.footerSize = Optional.of(Math.toIntExact(outputStream.getPos() - footerOffset));
    this.fileSize = Optional.of(outputStream.getPos());
    this.finished = true;
  }

  private void writeFooter() throws IOException {
    FileMetadata fileMetadata = new FileMetadata(writtenBlobsMetadata, properties);
    ByteBuffer footerJson =
        ByteBuffer.wrap(
            FileMetadataParser.toJson(fileMetadata, false).getBytes(StandardCharsets.UTF_8));
    ByteBuffer footerPayload = PuffinFormat.compress(footerCompression, footerJson);
    outputStream.write(MAGIC);
    int footerPayloadLength = footerPayload.remaining();
    IOUtil.writeFully(outputStream, footerPayload);
    PuffinFormat.writeIntegerLittleEndian(outputStream, footerPayloadLength);
    writeFlags();
    outputStream.write(MAGIC);
  }

  private void writeFlags() throws IOException {
    Map<Integer, List<Flag>> flagsByByteNumber =
        fileFlags().stream().collect(Collectors.groupingBy(Flag::byteNumber));
    for (int byteNumber = 0; byteNumber < PuffinFormat.FOOTER_STRUCT_FLAGS_LENGTH; byteNumber++) {
      int byteFlag = 0;
      for (Flag flag : flagsByByteNumber.getOrDefault(byteNumber, ImmutableList.of())) {
        byteFlag |= 0x1 << flag.bitNumber();
      }
      outputStream.write(byteFlag);
    }
  }

  public long footerSize() {
    return footerSize.orElseThrow(() -> new IllegalStateException("Footer not written yet"));
  }

  public long fileSize() {
    return fileSize.orElseThrow(() -> new IllegalStateException("File not written yet"));
  }

  public List<BlobMetadata> writtenBlobsMetadata() {
    return ImmutableList.copyOf(writtenBlobsMetadata);
  }

  private Set<Flag> fileFlags() {
    EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
    if (footerCompression != PuffinCompressionCodec.NONE) {
      flags.add(Flag.FOOTER_PAYLOAD_COMPRESSED);
    }

    return flags;
  }

  private void checkNotFinished() {
    Preconditions.checkState(!finished, "Writer already finished");
  }
}
