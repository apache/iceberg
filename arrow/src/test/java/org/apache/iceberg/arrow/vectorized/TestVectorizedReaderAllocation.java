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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that {@code VectorizedArrowReader#allocateVectorBasedOnTypeName} allocates vectors sized to
 * the batch size (value count), not to the batch size multiplied by the type width or by an
 * average-record-size hint. See https://github.com/apache/iceberg/issues/11672.
 */
public class TestVectorizedReaderAllocation {

  // Chosen so that a regression re-multiplying the batch size by the type width or by the
  // AVERAGE_VARIABLE_WIDTH_RECORD_SIZE hint would push the observed capacity far past the bound
  // asserted below.
  private static final int BATCH_SIZE = 16;
  private static final int FIXED_LENGTH = 1000;

  @TempDir private File tempDir;

  @Test
  public void fixedLenByteArrayVectorCapacityMatchesBatchSize() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "fixed_col", Types.FixedType.ofLength(FIXED_LENGTH)));
    Table table = writeSingleRow(schema, "fixed_col", (Object) new byte[FIXED_LENGTH]);

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), BATCH_SIZE, false)) {
      for (ColumnarBatch batch : reader) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();
        FixedSizeBinaryVector vec = (FixedSizeBinaryVector) root.getVector("fixed_col");
        assertThat(vec.getValueCapacity())
            .as(
                "fixed(%d) vector capacity must scale with batch size, not batch size * width",
                FIXED_LENGTH)
            .isLessThanOrEqualTo(BATCH_SIZE * 4);
        root.close();
      }
    }
  }

  @Test
  public void binaryVectorCapacityMatchesBatchSize() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "binary_col", Types.BinaryType.get()));
    Table table =
        writeSingleRow(
            schema,
            "binary_col",
            (Object) ByteBuffer.wrap("value".getBytes(StandardCharsets.UTF_8)));

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), BATCH_SIZE, false)) {
      for (ColumnarBatch batch : reader) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();
        VarBinaryVector vec = (VarBinaryVector) root.getVector("binary_col");
        assertThat(vec.getValueCapacity())
            .as(
                "binary vector offset buffer must scale with batch size, not with an "
                    + "average-record-size hint")
            .isLessThanOrEqualTo(BATCH_SIZE * 4);
        root.close();
      }
    }
  }

  private Table writeSingleRow(Schema schema, String fieldName, Object value) throws Exception {
    HadoopTables tables = new HadoopTables();
    Table table = tables.create(schema, tempDir.toURI().toString());

    GenericRecord record = GenericRecord.create(schema);
    record.setField(fieldName, value);
    List<GenericRecord> records = Lists.newArrayList(record);

    File parquetFile = File.createTempFile("junit", ".parquet", tempDir);
    assertThat(parquetFile.delete()).isTrue();
    FileAppender<GenericRecord> appender =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::create)
            .build();
    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(localInput(parquetFile))
            .withMetrics(appender.metrics())
            .withFormat(FileFormat.PARQUET)
            .build();
    table.newAppend().appendFile(dataFile).commit();
    return table;
  }
}
