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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A filter is written against the table schema, which can differ from the schema a file was written
 * with. Each case reads a file through a later table schema and checks that the pushed filter
 * applies to the right file column and does not fail the scan.
 */
class TestVortexFilterSchemaEvolution {
  private static final Schema WRITE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "a", Types.LongType.get()),
          optional(3, "n", Types.IntegerType.get()),
          optional(4, "s", Types.StructType.of(optional(5, "inner", Types.IntegerType.get()))));

  @TempDir private Path temp;
  private InputFile file;

  @BeforeEach
  void writeFile() throws IOException {
    OutputFile outputFile = Files.localOutput(temp.resolve("evolution.vortex").toFile());
    try (FileAppender<Record> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(WRITE_SCHEMA)
            .content(FileContent.DATA)
            .build()) {
      for (long id = 0; id < 4; id++) {
        Record record = GenericRecord.create(WRITE_SCHEMA);
        record.setField("id", id);
        record.setField("a", id * 10);
        record.setField("n", (int) id);

        Record inner = GenericRecord.create(WRITE_SCHEMA.findType("s").asStructType());
        inner.setField("inner", (int) id * 100);
        record.setField("s", inner);
        appender.add(record);
      }
    }

    this.file = outputFile.toInputFile();
  }

  @Test
  void boundFilterUsesFieldIds() throws IOException {
    // Bound by id, field 1 is id. Matching it by position in the file would filter a instead.
    Expression filter = Binder.bind(WRITE_SCHEMA.asStruct(), Expressions.equal("id", 1L), true);
    assertThat(scan(WRITE_SCHEMA, filter)).containsExactly(1L);
  }

  @Test
  void renamedColumn() throws IOException {
    Schema renamed =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "b", Types.LongType.get()),
            optional(3, "n", Types.IntegerType.get()),
            optional(4, "t", Types.StructType.of(optional(5, "renamed", Types.IntegerType.get()))));

    assertThat(scan(renamed, Expressions.equal("b", 10L))).containsExactly(1L);
    assertThat(scan(renamed, Expressions.equal("t.renamed", 200))).containsExactly(2L);
  }

  @Test
  void addedColumnIsNotPushedDown() throws IOException {
    Schema added =
        new Schema(required(1, "id", Types.LongType.get()), optional(6, "c", Types.LongType.get()));

    // Every row reads c as null, so isNull matches all of them and equality matches none. The
    // engine applies the filter, so the scan returns every row in both cases.
    assertThat(scan(added, Expressions.isNull("c"))).containsExactly(0L, 1L, 2L, 3L);
    assertThat(scan(added, Expressions.equal("c", 1L))).containsExactly(0L, 1L, 2L, 3L);
  }

  @Test
  void reusedNameFiltersNewColumn() throws IOException {
    // a was renamed to b, and a new column took the name a. A filter on a refers to the new
    // column, which this file does not have, and must not filter the old column instead.
    Schema reused =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "b", Types.LongType.get()),
            optional(6, "a", Types.LongType.get()));

    assertThat(scan(reused, Expressions.isNull("a"))).containsExactly(0L, 1L, 2L, 3L);
  }

  @Test
  void promotedColumn() throws IOException {
    Schema promoted =
        new Schema(required(1, "id", Types.LongType.get()), optional(3, "n", Types.LongType.get()));

    assertThat(scan(promoted, Expressions.greaterThan("n", 1L))).containsExactly(2L, 3L);
    assertThat(scan(promoted, Expressions.in("n", 0L, 3L))).containsExactly(0L, 3L);

    // Above the int range: no row can match, and the scan must not fail on the literal.
    assertThat(scan(promoted, Expressions.equal("n", Long.MAX_VALUE))).isEmpty();
    assertThat(scan(promoted, Expressions.lessThan("n", Long.MAX_VALUE)))
        .containsExactly(0L, 1L, 2L, 3L);
  }

  private List<Long> scan(Schema schema, Expression filter) throws IOException {
    try (CloseableIterable<Record> records =
        formatModel().readBuilder(file).project(schema).filter(filter).build()) {
      List<Long> ids = Lists.newArrayList();
      for (Record record : records) {
        ids.add((Long) record.getField("id"));
      }

      return ids;
    }
  }

  private static VortexFormatModel<Record, StructType, VortexRowReader<?>> formatModel() {
    return VortexFormatModel.create(
        Record.class,
        StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> GenericVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
  }
}
