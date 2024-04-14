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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class TestFlinkOrcReaderWriter extends DataTest {
  private static final int NUM_RECORDS = 100;

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    RowType flinkSchema = FlinkSchemaUtil.convert(schema);
    List<Record> expectedRecords = RandomGenericData.generate(schema, NUM_RECORDS, 1990L);
    List<RowData> expectedRows = Lists.newArrayList(RandomRowData.convert(schema, expectedRecords));

    File recordsFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(recordsFile.delete()).isTrue();

    // Write the expected records into ORC file, then read them into RowData and assert with the
    // expected Record list.
    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(recordsFile))
            .schema(schema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      writer.addAll(expectedRecords);
    }

    try (CloseableIterable<RowData> reader =
        ORC.read(Files.localInput(recordsFile))
            .project(schema)
            .createReaderFunc(type -> new FlinkOrcReader(schema, type))
            .build()) {
      Iterator<Record> expected = expectedRecords.iterator();
      Iterator<RowData> rows = reader.iterator();
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertThat(rows).hasNext();
        TestHelpers.assertRowData(schema.asStruct(), flinkSchema, expected.next(), rows.next());
      }
      assertThat(rows).isExhausted();
    }

    File rowDataFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(rowDataFile.delete()).isTrue();

    // Write the expected RowData into ORC file, then read them into Record and assert with the
    // expected RowData list.
    RowType rowType = FlinkSchemaUtil.convert(schema);
    try (FileAppender<RowData> writer =
        ORC.write(Files.localOutput(rowDataFile))
            .schema(schema)
            .createWriterFunc((iSchema, typeDesc) -> FlinkOrcWriter.buildWriter(rowType, iSchema))
            .build()) {
      writer.addAll(expectedRows);
    }

    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(rowDataFile))
            .project(schema)
            .createReaderFunc(type -> GenericOrcReader.buildReader(schema, type))
            .build()) {
      Iterator<RowData> expected = expectedRows.iterator();
      Iterator<Record> records = reader.iterator();
      for (int i = 0; i < NUM_RECORDS; i += 1) {
        assertThat(records.hasNext()).isTrue();
        TestHelpers.assertRowData(schema.asStruct(), flinkSchema, records.next(), expected.next());
      }
      assertThat(records).isExhausted();
    }
  }
}
