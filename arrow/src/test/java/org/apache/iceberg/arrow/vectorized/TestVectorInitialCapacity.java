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
package org.apache.iceberg.arrow.vectorized;

import static org.apache.iceberg.Files.localInput;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Asserts how {@link VectorizedArrowReader} sizes the vectors it allocates, by reading a table
 * through the vectorized path and inspecting the vectors it produced.
 *
 * <p>Every {@code setInitialCapacity} argument in the reader is a <em>value count</em>. Passing a
 * byte count instead over-reserves by the width factor, which is what these tests pin: each vector
 * must be able to hold a full batch of rows, and must not reserve room for a batch multiplied by a
 * width.
 */
public class TestVectorInitialCapacity {

  private static final int BATCH_SIZE = 5000;

  /** Mirrors {@code VectorizedArrowReader#AVERAGE_VARIABLE_WIDTH_RECORD_SIZE}. */
  private static final int AVERAGE_VARIABLE_WIDTH = 10;

  private static final int FIXED_WIDTH = 16;
  private static final int NUM_ROWS = 10;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "string", Types.StringType.get()),
          Types.NestedField.required(2, "bytes", Types.BinaryType.get()),
          Types.NestedField.required(3, "fixed", Types.FixedType.ofLength(FIXED_WIDTH)));

  @TempDir private File tempDir;

  private Table table;

  @BeforeEach
  public void before() throws Exception {
    this.table = new HadoopTables().create(SCHEMA, tempDir.toURI().toString());
    table.newAppend().appendFile(writeRows()).commit();
  }

  @Test
  public void variableWidthVectorsHoldOneBatchOfValues() throws Exception {
    try (VectorizedTableScanIterable scan =
        new VectorizedTableScanIterable(table.newScan(), BATCH_SIZE, false)) {
      for (ColumnarBatch batch : scan) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();

        // VARCHAR, allocated by allocateVectorForEnumJsonBsonString
        VarCharVector strings = (VarCharVector) root.getVector("string");
        assertThat(strings.getValueCapacity())
            .as("a varchar vector must reserve offsets for a batch of rows")
            .isGreaterThanOrEqualTo(BATCH_SIZE);
        assertThat(strings.getValueCapacity())
            .as("a varchar vector must not reserve offsets for batchSize * averageWidth rows")
            .isLessThan(BATCH_SIZE * AVERAGE_VARIABLE_WIDTH);

        // VARBINARY, allocated by the BINARY case
        VarBinaryVector bytes = (VarBinaryVector) root.getVector("bytes");
        assertThat(bytes.getValueCapacity())
            .as("a varbinary vector must reserve offsets for a batch of rows")
            .isGreaterThanOrEqualTo(BATCH_SIZE);
        assertThat(bytes.getValueCapacity())
            .as("a varbinary vector must not reserve offsets for batchSize * averageWidth rows")
            .isLessThan(BATCH_SIZE * AVERAGE_VARIABLE_WIDTH);
      }
    }
  }

  @Test
  public void fixedWidthVectorsHoldOneBatchOfValues() throws Exception {
    try (VectorizedTableScanIterable scan =
        new VectorizedTableScanIterable(table.newScan(), BATCH_SIZE, false)) {
      for (ColumnarBatch batch : scan) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();

        // FIXED_WIDTH_BINARY, allocated by the FIXED_LEN_BYTE_ARRAY case
        FixedSizeBinaryVector fixed = (FixedSizeBinaryVector) root.getVector("fixed");
        assertThat(fixed.getValueCapacity())
            .as("a fixed width vector must hold a batch of values")
            .isGreaterThanOrEqualTo(BATCH_SIZE);
        assertThat(fixed.getValueCapacity())
            .as("a fixed width vector must not reserve batchSize * typeWidth values")
            .isLessThan(BATCH_SIZE * FIXED_WIDTH);
      }
    }
  }

  /**
   * Writes {@link #NUM_ROWS} rows with dictionary encoding turned off, so the reader takes the
   * plain-encoded allocation path under test rather than {@code allocateDictEncodedVector}.
   */
  private DataFile writeRows() throws Exception {
    File parquetFile = new File(tempDir, "initial-capacity.parquet");
    parquetFile.delete();

    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord record = GenericRecord.create(SCHEMA);
      record.setField("string", "string-" + i);
      record.setField("bytes", ByteBuffer.wrap(("bytes-" + i).getBytes(StandardCharsets.UTF_8)));
      record.setField("fixed", fixedValue(i));
      records.add(record);
    }

    FileAppender<GenericRecord> appender =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .withDictionaryEncoding("string", false)
            .withDictionaryEncoding("bytes", false)
            .withDictionaryEncoding("fixed", false)
            .build();
    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withInputFile(localInput(parquetFile))
        .withMetrics(appender.metrics())
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  private static byte[] fixedValue(int row) {
    byte[] value = new byte[FIXED_WIDTH];
    for (int i = 0; i < FIXED_WIDTH; i++) {
      value[i] = (byte) (row + i);
    }
    return value;
  }
}
