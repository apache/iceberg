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

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionInputFile;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestParquetUtil {
  private static final Schema SCHEMA = new Schema(optional(1, "id", Types.IntegerType.get()));
  private static final ByteBuffer FILE_DEK =
      ByteBuffer.wrap("0123456789abcdef".getBytes(StandardCharsets.UTF_8));
  private static final ByteBuffer AAD_PREFIX =
      ByteBuffer.wrap("aad-prefix".getBytes(StandardCharsets.UTF_8));

  @TempDir private Path temp;

  @Test
  void fileMetricsOfNativelyEncryptedFile() throws IOException {
    File file = createTempFile(temp);
    writeEncryptedFile(file, Lists.newArrayList(3, null, 1, 2));

    assertThatThrownBy(() -> ParquetUtil.fileMetrics(localInput(file), MetricsConfig.getDefault()))
        .isInstanceOf(ParquetCryptoRuntimeException.class)
        .hasMessage("Trying to read file with encrypted footer. No keys available");

    NativeEncryptionInputFile input =
        new NativeInputFile(localInput(file), new KeyMetadata(FILE_DEK, AAD_PREFIX));
    Metrics metrics = ParquetUtil.fileMetrics(input, MetricsConfig.getDefault());

    assertThat(metrics.recordCount()).isEqualTo(4L);
    assertThat(metrics.valueCounts()).containsEntry(1, 4L);
    assertThat(metrics.nullValueCounts()).containsEntry(1, 1L);
    assertThat((Integer) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBound(metrics)))
        .isEqualTo(1);
    assertThat((Integer) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBound(metrics)))
        .isEqualTo(3);
  }

  private static ByteBuffer lowerBound(Metrics metrics) {
    return metrics.lowerBounds().get(1);
  }

  private static ByteBuffer upperBound(Metrics metrics) {
    return metrics.upperBounds().get(1);
  }

  private static void writeEncryptedFile(File file, List<Integer> values) throws IOException {
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(SCHEMA.asStruct());
    try (FileAppender<GenericData.Record> writer =
        Parquet.write(localOutput(file))
            .schema(SCHEMA)
            .withFileEncryptionKey(FILE_DEK)
            .withAADPrefix(AAD_PREFIX)
            .build()) {
      for (Integer value : values) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("id", value);
        writer.add(record);
      }
    }
  }

  private record KeyMetadata(ByteBuffer encryptionKey, ByteBuffer aadPrefix)
      implements NativeEncryptionKeyMetadata {
    @Override
    public ByteBuffer buffer() {
      return ByteBuffer.allocate(0);
    }

    @Override
    public EncryptionKeyMetadata copy() {
      return this;
    }
  }

  private record NativeInputFile(
      InputFile encryptedInputFile, NativeEncryptionKeyMetadata keyMetadata)
      implements NativeEncryptionInputFile {
    @Override
    public long getLength() {
      return encryptedInputFile.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return encryptedInputFile.newStream();
    }

    @Override
    public String location() {
      return encryptedInputFile.location();
    }

    @Override
    public boolean exists() {
      return encryptedInputFile.exists();
    }
  }
}
