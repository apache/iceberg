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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.math.BigDecimal;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Vectorized-read tests focused on Iceberg field defaults. */
public class TestVectorizedDefaultValues {

  @TempDir private File tempDir;

  @Test
  public void testDecimalWithDefaultValueNotDictionaryEncoded() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required("id").withId(1).ofType(Types.LongType.get()).build(),
            Types.NestedField.optional("int_backed")
                .withId(2)
                .ofType(Types.DecimalType.of(5, 2))
                .withInitialDefault(Literal.of(new BigDecimal("0.00")))
                .withWriteDefault(Literal.of(new BigDecimal("0.00")))
                .build(),
            Types.NestedField.optional("long_backed")
                .withId(3)
                .ofType(Types.DecimalType.of(15, 2))
                .withInitialDefault(Literal.of(new BigDecimal("0.00")))
                .withWriteDefault(Literal.of(new BigDecimal("0.00")))
                .build(),
            Types.NestedField.optional("fixed_backed")
                .withId(4)
                .ofType(Types.DecimalType.of(25, 2))
                .withInitialDefault(Literal.of(new BigDecimal("0.00")))
                .withWriteDefault(Literal.of(new BigDecimal("0.00")))
                .build());

    HadoopTables tables = new HadoopTables();
    Table table =
        tables.create(
            schema,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"),
            tempDir.toURI().toString());

    List<GenericRecord> records = Lists.newArrayList();
    GenericRecord template = GenericRecord.create(schema);
    for (long i = 0; i < 5; i++) {
      GenericRecord rec = template.copy();
      rec.setField("id", i);
      rec.setField("int_backed", new BigDecimal("12.34"));
      rec.setField("long_backed", new BigDecimal("1234567890.12"));
      rec.setField("fixed_backed", new BigDecimal("9876543210.99"));
      records.add(rec);
    }

    File dataFile = new File(tempDir, "decimal-no-dict.parquet");
    try (FileAppender<GenericRecord> writer =
        Parquet.write(Files.localOutput(dataFile))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::create)
            .set(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
            .build()) {
      writer.addAll(records);
    }

    DataFile parquetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(dataFile.getAbsolutePath())
            .withFileSizeInBytes(dataFile.length())
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(records.size())
            .build();
    table.newAppend().appendFile(parquetFile).commit();

    int rowsRead = 0;
    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), 1024, false)) {
      for (ColumnarBatch batch : reader) {
        ColumnVector idColumn = batch.column(0);
        ColumnVector intBackedColumn = batch.column(1);
        ColumnVector longBackedColumn = batch.column(2);
        ColumnVector fixedBackedColumn = batch.column(3);

        for (int i = 0; i < batch.numRows(); i++) {
          GenericRecord expected = records.get(rowsRead + i);
          assertThat(idColumn.getLong(i)).isEqualTo(expected.getField("id"));
          assertThat(intBackedColumn.getDecimal(i, 5, 2))
              .isEqualTo(expected.getField("int_backed"));
          assertThat(longBackedColumn.getDecimal(i, 15, 2))
              .isEqualTo(expected.getField("long_backed"));
          assertThat(fixedBackedColumn.getDecimal(i, 25, 2))
              .isEqualTo(expected.getField("fixed_backed"));
        }

        rowsRead += batch.numRows();
      }
    }

    assertThat(rowsRead).isEqualTo(records.size());
  }
}
