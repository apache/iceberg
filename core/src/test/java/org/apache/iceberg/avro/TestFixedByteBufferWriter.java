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
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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

  /**
   * End-to-end regression test for the backward-compat read path. Simulates a manifest written by a
   * pre-fix writer: FIXED fields encoded with {@code writeBytes()} (length-prefixed) and no {@code
   * iceberg.avro.fixed-encoding} header. The reader must detect the absent header, select {@link
   * ValueReaders#byteBuffers()}, and decode the data without corruption.
   */
  @Test
  public void testLegacyManifestRoundTrip() throws IOException {
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

    // Write a normal v2 manifest to get the Avro schema, metadata, and encoded records.
    File manifestFile = temp.resolve("manifest.avro").toFile();
    ManifestWriter<DataFile> v2Writer =
        ManifestFiles.write(2, spec, Files.localOutput(manifestFile), 1L);
    try {
      v2Writer.add(dataFile);
    } finally {
      v2Writer.close();
    }
    ManifestFile manifest = v2Writer.toManifestFile();

    // Read the v2 manifest as generic Avro records, collecting schema and non-encoding metadata.
    org.apache.avro.Schema avroSchema;
    Map<String, byte[]> metaToCopy = Maps.newHashMap();
    List<GenericRecord> rows = Lists.newArrayList();
    try (DataFileReader<GenericRecord> dfr =
        new DataFileReader<>(manifestFile, new GenericDatumReader<>())) {
      avroSchema = dfr.getSchema();
      for (String key : dfr.getMetaKeys()) {
        // Skip Avro-reserved keys (written automatically by DataFileWriter) and the encoding
        // stamp so the rewritten file looks like a pre-fix legacy manifest.
        if (!key.startsWith("avro.")
            && !AvroFileMetadataAware.FIXED_ENCODING_META_KEY.equals(key)) {
          metaToCopy.put(key, dfr.getMeta(key));
        }
      }
      while (dfr.hasNext()) {
        rows.add(dfr.next());
      }
    }

    // Overwrite the manifest with legacy encoding: writeBytes() for all FIXED fields, no stamp.
    GenericDatumWriter<GenericRecord> legacyDatumWriter =
        new GenericDatumWriter<GenericRecord>(avroSchema) {
          @Override
          protected void writeFixed(org.apache.avro.Schema schema, Object datum, Encoder out)
              throws IOException {
            out.writeBytes(ByteBuffer.wrap(((GenericData.Fixed) datum).bytes()));
          }
        };
    try (DataFileWriter<GenericRecord> dfw = new DataFileWriter<>(legacyDatumWriter)) {
      metaToCopy.forEach(dfw::setMeta);
      dfw.create(avroSchema, manifestFile);
      for (GenericRecord row : rows) {
        dfw.append(row);
      }
    }

    // Read back through ManifestFiles — exercises the full InternalReader dispatch path.
    // The absent header causes InternalReader to use byteBuffers() instead of fixedBuffers().
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
          .as("record_count must not be corrupted when reading a legacy manifest")
          .isEqualTo(4L);

      ByteBuffer readPartition = (ByteBuffer) read.partition().get(0, ByteBuffer.class);
      byte[] readBytes = new byte[readPartition.remaining()];
      readPartition.duplicate().get(readBytes);
      assertThat(readBytes)
          .as("FIXED(3) partition value must be read correctly from a legacy manifest")
          .isEqualTo(partitionBytes);
    }
  }

  /**
   * Verifies that the legacy reader ({@code ValueReaders.byteBuffers()}) correctly decodes bytes
   * that were written with {@code encoder.writeBytes()} — the old buggy encoding used by pre-fix
   * writers. The new reader uses {@link AvroFileMetadataAware} to detect old files (no {@code
   * iceberg.avro.fixed-encoding} header) and falls back to this path automatically.
   */
  @Test
  public void testLegacyReaderDecodesOldEncoding() throws IOException {
    byte[] input = new byte[] {(byte) 0xAB, (byte) 0xCD, (byte) 0xEF};

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeBytes(ByteBuffer.wrap(input));
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    ByteBuffer result = ValueReaders.byteBuffers().read(decoder, null);

    byte[] readBytes = new byte[result.remaining()];
    result.duplicate().get(readBytes);
    assertThat(readBytes).isEqualTo(input);
  }

  private static org.apache.iceberg.StructLike partitionData(
      PartitionSpec spec, byte[] fixedBytes) {
    org.apache.iceberg.PartitionData data =
        new org.apache.iceberg.PartitionData(spec.partitionType());
    data.set(0, ByteBuffer.wrap(fixedBytes));
    return data;
  }
}
