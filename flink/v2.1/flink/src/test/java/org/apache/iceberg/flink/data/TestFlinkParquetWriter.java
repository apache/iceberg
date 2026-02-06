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
package org.apache.iceberg.flink.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTestBase;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.io.TempDir;

public class TestFlinkParquetWriter extends DataTestBase {
  private static final int NUM_RECORDS = 100;

  @TempDir private Path temp;

  @Override
  protected boolean supportsUnknown() {
    return true;
  }

  @Override
  protected boolean supportsTimestampNanos() {
    return true;
  }

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema, List<Record> data)
      throws IOException {
    OutputFile outputFile = new InMemoryOutputFile();

    LogicalType logicalType = FlinkSchemaUtil.convert(writeSchema);
    try (FileAppender<RowData> writer =
        Parquet.write(outputFile)
            .schema(writeSchema)
            .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(logicalType, msgType))
            .build()) {
      writer.addAll(RandomRowData.convert(writeSchema, data));
    }

    try (CloseableIterable<Record> reader =
        Parquet.read(outputFile.toInputFile())
            .project(expectedSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(expectedSchema, fileSchema))
            .build()) {
      Iterator<Record> actual = reader.iterator();
      RowType rowType = FlinkSchemaUtil.convert(expectedSchema);
      for (Record expected : data) {
        assertThat(actual).hasNext();
        RowData actualRowData = RowDataConverter.convert(expectedSchema, actual.next());
        TestHelpers.assertRowData(expectedSchema.asStruct(), rowType, expected, actualRowData);
      }

      assertThat(actual).isExhausted();
    }
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, RandomGenericData.generate(schema, NUM_RECORDS, 19981));

    writeAndValidate(
        schema,
        Lists.newArrayList(
            RandomGenericData.generateDictionaryEncodableRecords(schema, NUM_RECORDS, 21124)));

    writeAndValidate(
        schema,
        Lists.newArrayList(
            RandomGenericData.generateFallbackRecords(
                schema, NUM_RECORDS, 21124, NUM_RECORDS / 20)));
  }

  @Override
  protected void writeAndValidate(Schema schema, List<Record> data) throws IOException {
    writeAndValidate(schema, schema, data);
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    List<Record> data = RandomGenericData.generate(writeSchema, NUM_RECORDS, 1991L);
    writeAndValidate(writeSchema, expectedSchema, data);
  }
}
