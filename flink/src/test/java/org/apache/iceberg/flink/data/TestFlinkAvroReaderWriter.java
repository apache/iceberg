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
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;

public class TestFlinkAvroReaderWriter extends DataTest {

  private static final int NUM_RECORDS = 100;

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    RowType flinkSchema = FlinkSchemaUtil.convert(schema);
    List<Record> expectedRecords = RandomGenericData.generate(schema, NUM_RECORDS, 1991L);
    List<RowData> expectedRows = Lists.newArrayList(RandomRowData.convert(schema, expectedRecords));

    File recordsFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", recordsFile.delete());

    // Write the expected records into AVRO file, then read them into RowData and assert with the expected Record list.
    try (FileAppender<Record> writer = Avro.write(Files.localOutput(recordsFile))
        .schema(schema)
        .createWriterFunc(DataWriter::create)
        .build()) {
      writer.addAll(expectedRecords);
    }

    try (CloseableIterable<RowData> reader = Avro.read(Files.localInput(recordsFile))
        .project(schema)
        .createReaderFunc(FlinkAvroReader::new)
        .build()) {
      Iterator<Record> expected = expectedRecords.iterator();
      Iterator<RowData> rows = reader.iterator();
      for (int i = 0; i < NUM_RECORDS; i++) {
        Assert.assertTrue("Should have expected number of records", rows.hasNext());
        TestHelpers.assertRowData(schema.asStruct(), flinkSchema, expected.next(), rows.next());
      }
      Assert.assertFalse("Should not have extra records", rows.hasNext());
    }

    File rowDataFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", rowDataFile.delete());

    // Write the expected RowData into AVRO file, then read them into Record and assert with the expected RowData list.
    try (FileAppender<RowData> writer = Avro.write(Files.localOutput(rowDataFile))
        .schema(schema)
        .createWriterFunc(ignore -> new FlinkAvroWriter(flinkSchema))
        .build()) {
      writer.addAll(expectedRows);
    }

    try (CloseableIterable<Record> reader = Avro.read(Files.localInput(rowDataFile))
        .project(schema)
        .createReaderFunc(DataReader::create)
        .build()) {
      Iterator<RowData> expected = expectedRows.iterator();
      Iterator<Record> records = reader.iterator();
      for (int i = 0; i < NUM_RECORDS; i += 1) {
        Assert.assertTrue("Should have expected number of records", records.hasNext());
        TestHelpers.assertRowData(schema.asStruct(), flinkSchema, records.next(), expected.next());
      }
      Assert.assertFalse("Should not have extra records", records.hasNext());
    }
  }
}
