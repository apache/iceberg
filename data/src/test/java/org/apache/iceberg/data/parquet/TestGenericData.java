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

package org.apache.iceberg.data.parquet;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;

public class TestGenericData extends DataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    List<Record> expected = RandomGenericData.generate(schema, 100, 0L);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> appender = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build()) {
      appender.addAll(expected);
    }

    List<Record> rows;
    try (CloseableIterable<Record> reader = Parquet.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
        .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      DataTestHelpers.assertEquals(schema.asStruct(), expected.get(i), rows.get(i));
    }
  }
}
