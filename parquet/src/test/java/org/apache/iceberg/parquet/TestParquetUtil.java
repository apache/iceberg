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
import java.nio.file.Path;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.encryption.NativeEncryptionInputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestParquetUtil {
  private static final Schema SCHEMA = new Schema(optional(1, "id", Types.IntegerType.get()));
  private static final List<Integer> VALUES = Lists.newArrayList(3, null, 1, 2);

  @TempDir private Path temp;

  @Test
  void fileMetricsOfPlainFile() throws IOException {
    File file = createTempFile(temp);
    write(Parquet.write(localOutput(file)));

    assertMetrics(ParquetUtil.fileMetrics(localInput(file), MetricsConfig.getDefault()));
  }

  @Test
  void fileMetricsOfNativelyEncryptedFile() throws IOException {
    File file = createTempFile(temp);
    EncryptionManager encryption = EncryptionTestHelpers.createEncryptionManager();
    EncryptedOutputFile output = encryption.encrypt(localOutput(file));
    write(Parquet.write(output));

    assertThatThrownBy(() -> ParquetUtil.fileMetrics(localInput(file), MetricsConfig.getDefault()))
        .isInstanceOf(ParquetCryptoRuntimeException.class)
        .hasMessage("Trying to read file with encrypted footer. No keys available");

    InputFile input =
        encryption.decrypt(EncryptedFiles.encryptedInput(localInput(file), output.keyMetadata()));
    assertThat(input).isInstanceOf(NativeEncryptionInputFile.class);
    assertMetrics(ParquetUtil.fileMetrics(input, MetricsConfig.getDefault()));
  }

  private static void write(Parquet.WriteBuilder builder) throws IOException {
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(SCHEMA.asStruct());
    try (FileAppender<GenericData.Record> writer = builder.schema(SCHEMA).build()) {
      for (Integer value : VALUES) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("id", value);
        writer.add(record);
      }
    }
  }

  private static void assertMetrics(Metrics metrics) {
    assertThat(metrics.recordCount()).isEqualTo(4L);
    assertThat(metrics.valueCounts()).containsEntry(1, 4L);
    assertThat(metrics.nullValueCounts()).containsEntry(1, 1L);
    Integer lower =
        Conversions.fromByteBuffer(Types.IntegerType.get(), metrics.lowerBounds().get(1));
    Integer upper =
        Conversions.fromByteBuffer(Types.IntegerType.get(), metrics.upperBounds().get(1));
    assertThat(lower).isEqualTo(1);
    assertThat(upper).isEqualTo(3);
  }
}
