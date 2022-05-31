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

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.puffin.PuffinCompressionCodec.NONE;
import static org.apache.iceberg.puffin.PuffinCompressionCodec.ZSTD;
import static org.apache.iceberg.puffin.PuffinFormatTestUtil.EMPTY_PUFFIN_UNCOMPRESSED_FOOTER_SIZE;
import static org.apache.iceberg.puffin.PuffinFormatTestUtil.readTestResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPuffinWriter {
  @Test
  public void testEmptyFooterCompressed() {
    PuffinWriter writer =
        Puffin.write(new InMemoryFileIO().newOutputFile(UUID.randomUUID().toString()))
            .compressFooter()
            .build();
    assertThatThrownBy(writer::footerSize)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Footer not written yet");
    assertThatThrownBy(writer::finish)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported codec: LZ4");
    assertThatThrownBy(writer::close)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported codec: LZ4");
  }

  @Test
  public void testEmptyFooterUncompressed() throws Exception {
    OutputFile outputFile = new InMemoryFileIO().newOutputFile(UUID.randomUUID().toString());
    PuffinWriter writer = Puffin.write(outputFile)
        .build();
    assertThatThrownBy(writer::footerSize)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Footer not written yet");
    writer.finish();
    assertThat(writer.footerSize()).isEqualTo(EMPTY_PUFFIN_UNCOMPRESSED_FOOTER_SIZE);
    writer.close();

    byte[] expected = readTestResource("v1/empty-puffin-uncompressed.bin");
    byte[] bytes = new byte[expected.length];
    try (SeekableInputStream inputStream = outputFile.toInputFile().newStream()) {
      Assertions.assertThat(inputStream.read(bytes)).isEqualTo(bytes.length);
      Assertions.assertThat(bytes).isEqualTo(expected);
    }
    // getFooterSize is still accessible after close()
    assertThat(writer.footerSize()).isEqualTo(EMPTY_PUFFIN_UNCOMPRESSED_FOOTER_SIZE);
    assertThat(writer.writtenBlobsMetadata()).isEmpty();
  }

  @Test
  public void testImplicitFinish() throws Exception {
    OutputFile outputFile = new InMemoryFileIO().newOutputFile(UUID.randomUUID().toString());
    PuffinWriter writer = Puffin.write(outputFile)
        .build();
    writer.close();
    byte[] expected = readTestResource("v1/empty-puffin-uncompressed.bin");
    byte[] bytes = new byte[expected.length];
    try (SeekableInputStream inputStream = outputFile.toInputFile().newStream()) {
      Assertions.assertThat(inputStream.read(bytes)).isEqualTo(bytes.length);
      Assertions.assertThat(bytes).isEqualTo(expected);
    }
    assertThat(writer.footerSize()).isEqualTo(EMPTY_PUFFIN_UNCOMPRESSED_FOOTER_SIZE);
  }

  @Test
  public void testWriteMetricDataUncompressed() throws Exception {
    testWriteMetric(NONE, "v1/sample-metric-data-uncompressed.bin");
  }

  @Test
  public void testWriteMetricDataCompressedZstd() throws Exception {
    testWriteMetric(ZSTD, "v1/sample-metric-data-compressed-zstd.bin");
  }

  private void testWriteMetric(PuffinCompressionCodec compression, String expectedResource) throws Exception {
    OutputFile outputFile = new InMemoryFileIO().newOutputFile(UUID.randomUUID().toString());
    try (PuffinWriter writer = Puffin.write(outputFile)
        .createdBy("Test 1234")
        .build()) {
      writer.add(new Blob("some-blob", ImmutableList.of(1), ByteBuffer.wrap("abcdefghi".getBytes(UTF_8)),
          compression, ImmutableMap.of()));

      // "xxx"s are stripped away by data offsets
      byte[] bytes =
          "xxx some blob \u0000 binary data 🤯 that is not very very very very very very long, is it? xxx".getBytes(
              UTF_8);
      writer.add(new Blob("some-other-blob", ImmutableList.of(2), ByteBuffer.wrap(bytes, 4, bytes.length - 8),
          compression, ImmutableMap.of()));

      assertThat(writer.writtenBlobsMetadata()).hasSize(2);
      BlobMetadata firstMetadata = writer.writtenBlobsMetadata().get(0);
      assertThat(firstMetadata.type()).isEqualTo("some-blob");
      assertThat(firstMetadata.inputFields()).isEqualTo(ImmutableList.of(1));
      assertThat(firstMetadata.properties()).isEqualTo(ImmutableMap.of());
      BlobMetadata secondMetadata = writer.writtenBlobsMetadata().get(1);
      assertThat(secondMetadata.type()).isEqualTo("some-other-blob");
      assertThat(secondMetadata.inputFields()).isEqualTo(ImmutableList.of(2));
      assertThat(secondMetadata.properties()).isEqualTo(ImmutableMap.of());
    }

    byte[] expected = readTestResource(expectedResource);
    byte[] readBytes = new byte[expected.length];
    try (SeekableInputStream inputStream = outputFile.toInputFile().newStream()) {
      Assertions.assertThat(inputStream.read(readBytes)).isEqualTo(readBytes.length);
      Assertions.assertThat(readBytes).isEqualTo(expected);
    }
  }
}
