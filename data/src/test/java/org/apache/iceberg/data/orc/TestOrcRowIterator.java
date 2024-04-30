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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestOrcRowIterator {

  private static final Schema DATA_SCHEMA = new Schema(required(100, "id", Types.LongType.get()));

  private static final int NUM_ROWS = 8000;
  private static final List<Record> DATA_ROWS;

  static {
    DATA_ROWS = Lists.newArrayListWithCapacity(NUM_ROWS);
    for (long i = 0; i < NUM_ROWS; i++) {
      Record row = GenericRecord.create(DATA_SCHEMA);
      row.set(0, i);
      DATA_ROWS.add(row);
    }
  }

  @TempDir private File temp;

  @BeforeEach
  public void writeFile() throws IOException {
    Assertions.assertTrue(temp.delete(), "Delete should succeed");

    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(temp))
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

  @Test
  public void testReadAllStripes() throws IOException {
    // With default batch size of 1024, will read the following batches
    // Stripe 1: 1024, 1024, 1024, 928
    // Stripe 2: 1024, 1024, 1024, 928
    readAndValidate(Expressions.alwaysTrue(), DATA_ROWS);
  }

  @Test
  public void testReadFilteredRowGroupInMiddle() throws IOException {
    // We skip the 2nd row group [1000, 2000] in Stripe 1
    // With default batch size of 1024, will read the following batches
    // Stripe 1: 1000, 1024, 976
    readAndValidate(
        Expressions.in("id", 500, 2500, 3500),
        Lists.newArrayList(
            Iterables.concat(DATA_ROWS.subList(0, 1000), DATA_ROWS.subList(2000, 4000))));
  }

  private void readAndValidate(Expression filter, List<Record> expected) throws IOException {
    List<Record> rows;
    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(temp))
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
