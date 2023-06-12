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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.puffin.PuffinCompressionCodec.NONE;
import static org.apache.iceberg.puffin.PuffinCompressionCodec.ZSTD;
import static org.apache.iceberg.puffin.PuffinFormatTestUtil.EMPTY_PUFFIN_UNCOMPRESSED_FOOTER_SIZE;
import static org.apache.iceberg.puffin.PuffinFormatTestUtil.readTestResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestPuffinWriter {
  @Test
  public void testEmptyFooterCompressed() {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();

    PuffinWriter writer = Puffin.write(outputFile).compressFooter().build();
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
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    PuffinWriter writer = Puffin.write(outputFile).build();
    assertThatThrownBy(writer::footerSize)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Footer not written yet");
    writer.finish();
    assertThat(writer.footerSize()).isEqualTo(EMPTY_PUFFIN_UNCOMPRESSED_FOOTER_SIZE);
    writer.close();
    assertThat(outputFile.toByteArray())
        .isEqualTo(readTestResource("v1/empty-puffin-uncompressed.bin"));
    // getFooterSize is still accessible after close()
    assertThat(writer.footerSize()).isEqualTo(EMPTY_PUFFIN_UNCOMPRESSED_FOOTER_SIZE);
    assertThat(writer.writtenBlobsMetadata()).isEmpty();
  }

  @Test
  public void testImplicitFinish() throws Exception {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    PuffinWriter writer = Puffin.write(outputFile).build();
    writer.close();
    assertThat(outputFile.toByteArray())
        .isEqualTo(readTestResource("v1/empty-puffin-uncompressed.bin"));
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

  private void testWriteMetric(PuffinCompressionCodec compression, String expectedResource)
      throws Exception {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (PuffinWriter writer = Puffin.write(outputFile).createdBy("Test 1234").build()) {
      writer.add(
          new Blob(
              "some-blob",
              ImmutableList.of(1),
              2,
              1,
              ByteBuffer.wrap("abcdefghi".getBytes(UTF_8)),
              compression,
              ImmutableMap.of()));

      // "xxx"s are stripped away by data offsets
      byte[] bytes =
          "xxx some blob \u0000 binary data 🤯 that is not very very very very very very long, is it? xxx"
              .getBytes(UTF_8);
      writer.add(
          new Blob(
              "some-other-blob",
              ImmutableList.of(2),
              2,
              1,
              ByteBuffer.wrap(bytes, 4, bytes.length - 8),
              compression,
              ImmutableMap.of()));

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
    assertThat(outputFile.toByteArray()).isEqualTo(expected);
  }
}
