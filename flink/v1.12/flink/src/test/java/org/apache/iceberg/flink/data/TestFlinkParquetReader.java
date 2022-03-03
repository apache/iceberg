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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.junit.Assert;

public class TestFlinkParquetReader extends DataTest {
  private static final int NUM_RECORDS = 100;

  private void writeAndValidate(Iterable<Record> iterable, Schema schema) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build()) {
      writer.addAll(iterable);
    }

    try (CloseableIterable<RowData> reader = Parquet.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(type -> FlinkParquetReaders.buildReader(schema, type))
        .build()) {
      Iterator<Record> expected = iterable.iterator();
      Iterator<RowData> rows = reader.iterator();
      LogicalType rowType = FlinkSchemaUtil.convert(schema);
      for (int i = 0; i < NUM_RECORDS; i += 1) {
        Assert.assertTrue("Should have expected number of rows", rows.hasNext());
        TestHelpers.assertRowData(schema.asStruct(), rowType, expected.next(), rows.next());
      }
      Assert.assertFalse("Should not have extra rows", rows.hasNext());
    }
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(RandomGenericData.generate(schema, NUM_RECORDS, 19981), schema);
    writeAndValidate(RandomGenericData.generateDictionaryEncodableRecords(schema, NUM_RECORDS, 21124), schema);
    writeAndValidate(RandomGenericData.generateFallbackRecords(schema, NUM_RECORDS, 21124, NUM_RECORDS / 20), schema);
  }
}
