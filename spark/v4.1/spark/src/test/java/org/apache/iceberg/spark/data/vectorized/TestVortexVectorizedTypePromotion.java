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
package org.apache.iceberg.spark.data.vectorized;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Iceberg lets a column widen after it was written, so a file can hold ints and floats for columns
 * the projection now reads as longs and doubles. Spark's generated code asks the column vector for
 * the projected type, so the narrower vector has to answer the wider getter.
 */
public class TestVortexVectorizedTypePromotion {
  private static final int ROWS = 500;

  @TempDir private Path temp;

  @Test
  public void testIntColumnProjectedAsLong() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "measure", Types.FloatType.get()));
    Schema projection =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "measure", Types.DoubleType.get()));

    InputFile inputFile = writeRecords(writeSchema);

    long position = 0;
    for (ColumnarBatch batch : readBatches(inputFile, projection)) {
      for (int row = 0; row < batch.numRows(); row++, position++) {
        assertThat(batch.column(0).getLong(row)).as("int column read as long").isEqualTo(position);
        assertThat(batch.column(1).getDouble(row))
            .as("float column read as double")
            .isEqualTo((double) (float) (position / 2.0f));
      }

      // Spark's generated code picks getters from the type the vector reports, so the promoted
      // type has to be visible here too, not just answerable by the accessor.
      assertThat(batch.column(0).dataType()).isEqualTo(DataTypes.LongType);
      assertThat(batch.column(1).dataType()).isEqualTo(DataTypes.DoubleType);
    }

    assertThat(position).isEqualTo(ROWS);
  }

  private InputFile writeRecords(Schema writeSchema) throws IOException {
    EncryptedOutputFile outputFile =
        EncryptedFiles.encryptedOutput(
            Files.localOutput(temp.resolve("promotion-" + System.nanoTime() + ".vortex").toFile()),
            EncryptionKeyMetadata.EMPTY);

    List<Record> records = Lists.newArrayListWithCapacity(ROWS);
    for (int i = 0; i < ROWS; i++) {
      Record record = GenericRecord.create(writeSchema);
      record.setField("id", i);
      record.setField("measure", i / 2.0f);
      records.add(record);
    }

    DataWriter<Record> writer =
        FormatModelRegistry.dataWriteBuilder(FileFormat.VORTEX, Record.class, outputFile)
            .schema(writeSchema)
            .spec(PartitionSpec.unpartitioned())
            .build();
    try (DataWriter<Record> closing = writer) {
      records.forEach(closing::write);
    }

    return outputFile.encryptingOutputFile().toInputFile();
  }

  private CloseableIterable<ColumnarBatch> readBatches(InputFile inputFile, Schema projection) {
    return FormatModelRegistry.readBuilder(FileFormat.VORTEX, ColumnarBatch.class, inputFile)
        .project(projection)
        .build();
  }
}
