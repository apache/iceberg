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
import static org.apache.iceberg.puffin.PuffinFormatTestUtil.SAMPLE_METRIC_DATA_COMPRESSED_ZSTD_FOOTER_SIZE;
import static org.apache.iceberg.puffin.PuffinFormatTestUtil.readTestResource;
import static org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.Test;

public class TestPuffinReader {
  @Test
  public void testEmptyFooterUncompressed() throws Exception {
    testEmpty("v1/empty-puffin-uncompressed.bin", EMPTY_PUFFIN_UNCOMPRESSED_FOOTER_SIZE);
  }

  @Test
  public void testEmptyWithUnknownFooterSize() throws Exception {
    testEmpty("v1/empty-puffin-uncompressed.bin", null);
  }

  private void testEmpty(String resourceName, @Nullable Long footerSize) throws Exception {
    InMemoryInputFile inputFile = new InMemoryInputFile(readTestResource(resourceName));
    Puffin.ReadBuilder readBuilder = Puffin.read(inputFile).withFileSize(inputFile.getLength());
    if (footerSize != null) {
      readBuilder = readBuilder.withFooterSize(footerSize);
    }
    try (PuffinReader reader = readBuilder.build()) {
      FileMetadata fileMetadata = reader.fileMetadata();
      assertThat(fileMetadata.properties()).as("file properties").isEqualTo(ImmutableMap.of());
      assertThat(fileMetadata.blobs()).as("blob list").isEmpty();
    }
  }

  @Test
  public void testWrongFooterSize() throws Exception {
    String resourceName = "v1/sample-metric-data-compressed-zstd.bin";
    long footerSize = SAMPLE_METRIC_DATA_COMPRESSED_ZSTD_FOOTER_SIZE;
    testWrongFooterSize(resourceName, footerSize - 1, "Invalid file: expected magic at offset");
    testWrongFooterSize(resourceName, footerSize + 1, "Invalid file: expected magic at offset");
    testWrongFooterSize(resourceName, footerSize - 10, "Invalid file: expected magic at offset");
    testWrongFooterSize(resourceName, footerSize + 10, "Invalid file: expected magic at offset");
    testWrongFooterSize(resourceName, footerSize - 10000, "Invalid footer size");
    testWrongFooterSize(resourceName, footerSize + 10000, "Invalid footer size");
  }

  private void testWrongFooterSize(
      String resourceName, long wrongFooterSize, String expectedMessagePrefix) throws Exception {
    InMemoryInputFile inputFile = new InMemoryInputFile(readTestResource(resourceName));
    Puffin.ReadBuilder builder =
        Puffin.read(inputFile).withFileSize(inputFile.getLength()).withFooterSize(wrongFooterSize);
    assertThatThrownBy(
            () -> {
              try (PuffinReader reader = builder.build()) {
                reader.fileMetadata();
              }
            })
        .hasMessageStartingWith(expectedMessagePrefix);
  }

  @Test
  public void testReadMetricDataUncompressed() throws Exception {
    testReadMetricData("v1/sample-metric-data-uncompressed.bin", NONE);
  }

  @Test
  public void testReadMetricDataCompressedZstd() throws Exception {
    testReadMetricData("v1/sample-metric-data-compressed-zstd.bin", ZSTD);
  }

  private void testReadMetricData(String resourceName, PuffinCompressionCodec expectedCodec)
      throws Exception {
    InMemoryInputFile inputFile = new InMemoryInputFile(readTestResource(resourceName));
    try (PuffinReader reader = Puffin.read(inputFile).build()) {
      FileMetadata fileMetadata = reader.fileMetadata();
      assertThat(fileMetadata.properties())
          .as("file properties")
          .isEqualTo(ImmutableMap.of("created-by", "Test 1234"));
      assertThat(fileMetadata.blobs()).as("blob list").hasSize(2);

      BlobMetadata firstBlob = fileMetadata.blobs().get(0);
      assertThat(firstBlob.type()).as("type").isEqualTo("some-blob");
      assertThat(firstBlob.inputFields()).as("columns").isEqualTo(ImmutableList.of(1));
      assertThat(firstBlob.offset()).as("offset").isEqualTo(4);
      assertThat(firstBlob.compressionCodec())
          .as("compression codec")
          .isEqualTo(expectedCodec.codecName());

      BlobMetadata secondBlob = fileMetadata.blobs().get(1);
      assertThat(secondBlob.type()).as("type").isEqualTo("some-other-blob");
      assertThat(secondBlob.inputFields()).as("columns").isEqualTo(ImmutableList.of(2));
      assertThat(secondBlob.offset())
          .as("offset")
          .isEqualTo(firstBlob.offset() + firstBlob.length());
      assertThat(secondBlob.compressionCodec())
          .as("compression codec")
          .isEqualTo(expectedCodec.codecName());

      Map<BlobMetadata, byte[]> read =
          Streams.stream(reader.readAll(ImmutableList.of(firstBlob, secondBlob)))
              .collect(toImmutableMap(Pair::first, pair -> ByteBuffers.toByteArray(pair.second())));

      assertThat(read)
          .as("read")
          .containsOnlyKeys(firstBlob, secondBlob)
          .containsEntry(firstBlob, "abcdefghi".getBytes(UTF_8))
          .containsEntry(
              secondBlob,
              "some blob \u0000 binary data 🤯 that is not very very very very very very long, is it?"
                  .getBytes(UTF_8));
    }
  }

  @Test
  public void testValidateFooterSizeValue() throws Exception {
    // Ensure the definition of SAMPLE_METRIC_DATA_COMPRESSED_ZSTD_FOOTER_SIZE remains accurate
    InMemoryInputFile inputFile =
        new InMemoryInputFile(readTestResource("v1/sample-metric-data-compressed-zstd.bin"));
    try (PuffinReader reader =
        Puffin.read(inputFile)
            .withFooterSize(SAMPLE_METRIC_DATA_COMPRESSED_ZSTD_FOOTER_SIZE)
            .build()) {
      assertThat(reader.fileMetadata().properties())
          .isEqualTo(ImmutableMap.of("created-by", "Test 1234"));
    }
  }

  @Test
  public void testCoalesceRangesContiguousBlobs() {
    // Two contiguous blobs (second starts right after the first ends) should be coalesced
    BlobMetadata blob1 = testBlobMetadata(100, 50);
    BlobMetadata blob2 = testBlobMetadata(150, 60);

    List<List<BlobMetadata>> ranges = PuffinReader.coalesceRanges(ImmutableList.of(blob1, blob2));

    assertThat(ranges).as("contiguous blobs should be in one range").hasSize(1);
    assertThat(ranges.get(0)).containsExactly(blob1, blob2);
  }

  @Test
  public void testCoalesceRangesNonContiguousBlobs() {
    // Two blobs with a gap should NOT be coalesced
    BlobMetadata blob1 = testBlobMetadata(100, 50);
    BlobMetadata blob2 = testBlobMetadata(200, 60);

    List<List<BlobMetadata>> ranges = PuffinReader.coalesceRanges(ImmutableList.of(blob1, blob2));

    assertThat(ranges).as("non-contiguous blobs should be in separate ranges").hasSize(2);
    assertThat(ranges.get(0)).containsExactly(blob1);
    assertThat(ranges.get(1)).containsExactly(blob2);
  }

  @Test
  public void testCoalesceRangesSingleBlob() {
    BlobMetadata blob1 = testBlobMetadata(100, 50);

    List<List<BlobMetadata>> ranges = PuffinReader.coalesceRanges(ImmutableList.of(blob1));

    assertThat(ranges).as("single blob should produce one range").hasSize(1);
    assertThat(ranges.get(0)).containsExactly(blob1);
  }

  @Test
  public void testCoalesceRangesMultipleContiguousGroups() {
    // Group 1: blobs at [100, 150) and [150, 210) — contiguous
    // Group 2: blobs at [300, 350) and [350, 400) — contiguous
    BlobMetadata blob1 = testBlobMetadata(100, 50);
    BlobMetadata blob2 = testBlobMetadata(150, 60);
    BlobMetadata blob3 = testBlobMetadata(300, 50);
    BlobMetadata blob4 = testBlobMetadata(350, 50);

    List<List<BlobMetadata>> ranges =
        PuffinReader.coalesceRanges(ImmutableList.of(blob1, blob2, blob3, blob4));

    assertThat(ranges).as("should have two separate groups").hasSize(2);
    assertThat(ranges.get(0)).containsExactly(blob1, blob2);
    assertThat(ranges.get(1)).containsExactly(blob3, blob4);
  }

  @Test
  public void testCoalesceRangesAllNonContiguous() {
    BlobMetadata blob1 = testBlobMetadata(100, 10);
    BlobMetadata blob2 = testBlobMetadata(200, 10);
    BlobMetadata blob3 = testBlobMetadata(300, 10);

    List<List<BlobMetadata>> ranges =
        PuffinReader.coalesceRanges(ImmutableList.of(blob1, blob2, blob3));

    assertThat(ranges).as("all non-contiguous blobs should produce separate ranges").hasSize(3);
  }

  @Test
  public void testCoalescedReadAllWithMultipleBlobs() throws Exception {
    // Write multiple blobs and verify readAll correctly reads them with coalescing
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (PuffinWriter writer = Puffin.write(outputFile).build()) {
      writer.add(
          new Blob(
              "blob-type-a",
              ImmutableList.of(1),
              1,
              1,
              ByteBuffer.wrap("first-blob-data".getBytes(UTF_8)),
              NONE,
              ImmutableMap.of()));
      writer.add(
          new Blob(
              "blob-type-b",
              ImmutableList.of(2),
              1,
              1,
              ByteBuffer.wrap("second-blob-data".getBytes(UTF_8)),
              NONE,
              ImmutableMap.of()));
      writer.add(
          new Blob(
              "blob-type-c",
              ImmutableList.of(3),
              1,
              1,
              ByteBuffer.wrap("third-blob-data".getBytes(UTF_8)),
              NONE,
              ImmutableMap.of()));
    }

    InMemoryInputFile inputFile = new InMemoryInputFile(outputFile.toByteArray());
    try (PuffinReader reader = Puffin.read(inputFile).build()) {
      FileMetadata fileMetadata = reader.fileMetadata();
      assertThat(fileMetadata.blobs()).hasSize(3);

      // Read all blobs — these should be contiguous and coalesced into a single read
      Map<BlobMetadata, byte[]> read =
          Streams.stream(reader.readAll(fileMetadata.blobs()))
              .collect(toImmutableMap(Pair::first, pair -> ByteBuffers.toByteArray(pair.second())));

      assertThat(read).hasSize(3);
      assertThat(read.get(fileMetadata.blobs().get(0)))
          .isEqualTo("first-blob-data".getBytes(UTF_8));
      assertThat(read.get(fileMetadata.blobs().get(1)))
          .isEqualTo("second-blob-data".getBytes(UTF_8));
      assertThat(read.get(fileMetadata.blobs().get(2)))
          .isEqualTo("third-blob-data".getBytes(UTF_8));
    }
  }

  @Test
  public void testCoalescedReadAllReversedOrder() throws Exception {
    // Verify readAll works when blobs are requested in reverse order
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (PuffinWriter writer = Puffin.write(outputFile).build()) {
      writer.add(
          new Blob(
              "blob-a",
              ImmutableList.of(1),
              1,
              1,
              ByteBuffer.wrap("aaa".getBytes(UTF_8)),
              NONE,
              ImmutableMap.of()));
      writer.add(
          new Blob(
              "blob-b",
              ImmutableList.of(2),
              1,
              1,
              ByteBuffer.wrap("bbb".getBytes(UTF_8)),
              NONE,
              ImmutableMap.of()));
    }

    InMemoryInputFile inputFile = new InMemoryInputFile(outputFile.toByteArray());
    try (PuffinReader reader = Puffin.read(inputFile).build()) {
      FileMetadata fileMetadata = reader.fileMetadata();
      BlobMetadata firstBlob = fileMetadata.blobs().get(0);
      BlobMetadata secondBlob = fileMetadata.blobs().get(1);

      // Request in reverse order
      Map<BlobMetadata, byte[]> read =
          Streams.stream(reader.readAll(ImmutableList.of(secondBlob, firstBlob)))
              .collect(toImmutableMap(Pair::first, pair -> ByteBuffers.toByteArray(pair.second())));

      assertThat(read.get(firstBlob)).isEqualTo("aaa".getBytes(UTF_8));
      assertThat(read.get(secondBlob)).isEqualTo("bbb".getBytes(UTF_8));
    }
  }

  private static BlobMetadata testBlobMetadata(long offset, long length) {
    return new BlobMetadata(
        "test-type", ImmutableList.of(1), 1, 1, offset, length, null, ImmutableMap.of());
  }
}
