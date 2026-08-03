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
package org.apache.iceberg.avro;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestFixedByteBufferWriter {

  @TempDir java.nio.file.Path temp;

  /**
   * Regression test for FIXED(N) Avro encoding. FixedByteBufferWriter must call
   * encoder.writeFixed() (exact N bytes) not encoder.writeBytes() (zigzag length prefix + data).
   * With writeBytes(), the length prefix spills into the next field in the record and corrupts it.
   */
  @Test
  public void testFixedWriterProducesExactBytes() throws IOException {
    byte[] input = new byte[] {(byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
    ByteBuffer value = ByteBuffer.wrap(input);

    ValueWriter<ByteBuffer> writer = ValueWriters.fixedBuffers(3);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(value, encoder);
    encoder.flush();

    byte[] written = out.toByteArray();

    // writeFixed emits exactly 3 bytes: AB CD EF
    // writeBytes would emit 4 bytes: 06 AB CD EF  (zigzag(3)=0x06 prefix + data)
    assertThat(written).hasSize(3);
    assertThat(written).isEqualTo(input);
  }

  /**
   * Regression test for manifest round-trip with a FIXED(N) identity-partition column. Verifies
   * that record_count and partition value survive a write/read cycle without corruption. Before the
   * fix, record_count read back as -568 instead of 4 because the zigzag length prefix written by
   * writeBytes() spilled into the record_count varint.
   */
  @Test
  public void testManifestRoundTripWithFixedPartition() throws IOException {
    Schema schema =
        new Schema(
            optional(1, "val1", Types.IntegerType.get()),
            optional(2, "val8", Types.FixedType.ofLength(3)));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("val8").build();

    byte[] partitionBytes = new byte[] {(byte) 0xAB, (byte) 0xCD, (byte) 0xEF};

    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(1024)
            .withPartition(partitionData(spec, partitionBytes))
            .withRecordCount(4)
            .build();

    OutputFile outputFile = Files.localOutput(temp.resolve("manifest.avro").toFile());
    ManifestWriter<DataFile> writer = ManifestFiles.write(2, spec, outputFile, 1L);
    try {
      writer.add(dataFile);
    } finally {
      writer.close();
    }

    ManifestFile manifest = writer.toManifestFile();
    FileIO fileIO =
        new FileIO() {
          @Override
          public InputFile newInputFile(String path) {
            return Files.localInput(path);
          }

          @Override
          public OutputFile newOutputFile(String path) {
            return Files.localOutput(path);
          }

          @Override
          public void deleteFile(String path) {}
        };

    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, fileIO)) {
      List<DataFile> files = Lists.newArrayList((CloseableIterable<DataFile>) reader);
      assertThat(files).hasSize(1);

      DataFile read = Iterables.getOnlyElement(files);

      assertThat(read.recordCount())
          .as("record_count must not be corrupted by a stray length-prefix byte")
          .isEqualTo(4L);

      ByteBuffer readPartition = (ByteBuffer) read.partition().get(0, ByteBuffer.class);
      byte[] readBytes = new byte[readPartition.remaining()];
      readPartition.duplicate().get(readBytes);
      assertThat(readBytes)
          .as("FIXED(3) partition value must survive the manifest round-trip unchanged")
          .isEqualTo(partitionBytes);
    }
  }

  private static org.apache.iceberg.StructLike partitionData(
      PartitionSpec spec, byte[] fixedBytes) {
    org.apache.iceberg.PartitionData data =
        new org.apache.iceberg.PartitionData(spec.partitionType());
    data.set(0, ByteBuffer.wrap(fixedBytes));
    return data;
  }
}
