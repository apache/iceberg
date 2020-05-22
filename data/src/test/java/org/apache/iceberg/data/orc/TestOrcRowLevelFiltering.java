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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.orc.OrcRowFilter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestOrcRowLevelFiltering {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final Schema SCHEMA = new Schema(
      required(100, "id", Types.LongType.get()),
      required(101, "data1", Types.StringType.get()),
      required(102, "data2", Types.StringType.get())
  );

  private static final List<Record> RECORDS = LongStream.range(0, 100).mapToObj(i -> {
    Record record = GenericRecord.create(SCHEMA);
    record.set(0, i);
    record.set(1, "data1:" + i);
    record.set(2, "data2:" + i);
    return record;
  }).collect(Collectors.toList());

  @Test
  public void testReadOrcWithRowFilterNoProjection() throws IOException {
    testReadOrcWithRowFilter(SCHEMA, rowFilterId(), RECORDS.subList(75, 100));
  }

  @Test
  public void testReadOrcWithRowFilterProjection() throws IOException {
    Schema projectedSchema = new Schema(
        required(101, "data1", Types.StringType.get())
    );

    List<Record> expected = RECORDS.subList(75, 100).stream().map(r -> {
      Record record = GenericRecord.create(projectedSchema);
      record.set(0, r.get(1));
      return record;
    }).collect(Collectors.toList());

    testReadOrcWithRowFilter(projectedSchema, rowFilterId(), expected);
  }

  @Test
  public void testReadOrcWithRowFilterPartialFilterColumns() throws IOException {
    Schema projectedSchema = new Schema(
        required(101, "data1", Types.StringType.get()),
        required(102, "data2", Types.StringType.get())
    );

    List<Record> expected = RECORDS.subList(25, 75).stream().map(r -> {
      Record record = GenericRecord.create(projectedSchema);
      record.set(0, r.get(1));
      record.set(1, r.get(2));
      return record;
    }).collect(Collectors.toList());

    testReadOrcWithRowFilter(projectedSchema, rowFilterIdAndData1(), expected);
  }

  @Test
  public void testReadOrcWithRowFilterNonExistentColumn() throws IOException {
    testReadOrcWithRowFilter(SCHEMA, rowFilterData3(), ImmutableList.of());
  }

  private void testReadOrcWithRowFilter(Schema schema, OrcRowFilter rowFilter, List<Record> expected)
      throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());
    try (FileAppender<Record> writer = ORC.write(Files.localOutput(testFile))
        .schema(SCHEMA)
        .createWriterFunc(GenericOrcWriter::buildWriter)
        .build()) {
      for (Record rec : RECORDS) {
        writer.add(rec);
      }
    }

    List<Record> rows;
    try (CloseableIterable<Record> reader = ORC.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
        .rowFilter(rowFilter)
        .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      DataTestHelpers.assertEquals(schema.asStruct(), expected.get(i), rows.get(i));
    }
  }

  private OrcRowFilter rowFilterId() {
    return new OrcRowFilter() {
      @Override
      public Schema requiredSchema() {
        return new Schema(
            required(100, "id", Types.LongType.get())
        );
      }

      @Override
      public boolean shouldKeep(Object[] values) {
        return (Long) values[0] >= 75;
      }
    };
  }

  private OrcRowFilter rowFilterIdAndData1() {
    return new OrcRowFilter() {
      @Override
      public Schema requiredSchema() {
        return new Schema(
            SCHEMA.findField("id"),
            SCHEMA.findField("data1")
        );
      }

      @Override
      public boolean shouldKeep(Object[] values) {
        return (Long) values[0] >= 25 && ((String) values[1]).compareTo("data1:75") < 0;
      }
    };
  }

  private OrcRowFilter rowFilterData3() {
    return new OrcRowFilter() {
      @Override
      public Schema requiredSchema() {
        return new Schema(
            optional(104, "data3", Types.LongType.get())
        );
      }

      @Override
      public boolean shouldKeep(Object[] values) {
        return values[0] != null && (Long) values[0] >= 25;
      }
    };
  }
}
