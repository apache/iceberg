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
package org.apache.iceberg.data.orc;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class OrcRowIteratorBase {

  protected static final Schema DATA_SCHEMA = new Schema(required(100, "id", Types.LongType.get()));

  protected static final int NUM_ROWS = 8000;
  protected static final List<Record> DATA_ROWS;

  static {
    DATA_ROWS = Lists.newArrayListWithCapacity(NUM_ROWS);
    for (long i = 0; i < NUM_ROWS; i++) {
      Record row = GenericRecord.create(DATA_SCHEMA);
      row.set(0, i);
      DATA_ROWS.add(row);
    }
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private File testFile;

  @Before
  public void writeFile() throws IOException {
    testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(testFile))
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .schema(DATA_SCHEMA)
            // write in such a way that the file contains 2 stripes each with 4 row groups of 1000
            // rows
            .set("iceberg.orc.vectorbatch.size", "1000")
            .set(OrcConf.ROW_INDEX_STRIDE.getAttribute(), "1000")
            .set(OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(), "4000")
            .set(OrcConf.STRIPE_SIZE.getAttribute(), "1")
            .build()) {
      writer.addAll(DATA_ROWS);
    }
  }

  protected void readAndValidate(Expression filter, List<Record> expected) throws IOException {
    List<Record> rows;
    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(testFile))
            .project(DATA_SCHEMA)
            .filter(filter)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(DATA_SCHEMA, fileSchema))
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      DataTestHelpers.assertEquals(DATA_SCHEMA.asStruct(), expected.get(i), rows.get(i));
    }
  }
}
