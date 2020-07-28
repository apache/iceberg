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
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class TestFlinkOrcReaderWriter extends DataTest {
  private static final int NUM_RECORDS = 200;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    Iterable<RowData> iterable = RandomData.generateRowData(schema, NUM_RECORDS, 1990L);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    RowType rowType = FlinkSchemaUtil.convert(schema);
    try (FileAppender<RowData> writer = ORC.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc((iSchema, typeDesc) -> FlinkOrcWriter.buildWriter(rowType, iSchema))
        .build()) {
      writer.addAll(iterable);
    }

    try (CloseableIterable<RowData> reader = ORC.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(type -> FlinkOrcReader.buildReader(schema, type))
        .build()) {
      Iterator<RowData> expected = iterable.iterator();
      Iterator<RowData> rows = reader.iterator();
      for (int i = 0; i < NUM_RECORDS; i += 1) {
        Assert.assertTrue("Should have expected number of rows", rows.hasNext());
        Assert.assertEquals(expected.next(), rows.next());
      }
      Assert.assertFalse("Should not have extra rows", rows.hasNext());
    }
  }
}
