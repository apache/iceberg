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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestPartialFieldIds {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(
              3,
              "location",
              Types.StructType.of(
                  Types.NestedField.required(7, "lat", Types.DoubleType.get()),
                  Types.NestedField.required(8, "lon", Types.DoubleType.get()))),
          Types.NestedField.optional(
              4, "tags", Types.ListType.ofOptional(9, Types.StringType.get())),
          Types.NestedField.optional(
              5,
              "props",
              Types.MapType.ofOptional(10, 11, Types.StringType.get(), Types.IntegerType.get())),
          Types.NestedField.optional(6, "added", Types.StringType.get()));

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void identifiedFieldsRemainReadable(boolean mapping) throws IOException {
    List<Record> rows = read(SCHEMA.select("id", "name", "location", "added"), mapping);
    assertThat(rows).hasSize(3);
    assertThat(rows).extracting(r -> r.getField("id")).containsExactly(1L, 2L, 3L);
    assertThat(rows).extracting(r -> r.getField("name")).containsExactly("alpha", null, "charlie");
    assertThat(rows.get(0).getField("location")).isNotNull();
    assertThat(((Record) rows.get(0).getField("location")).getField("lat")).isEqualTo(40.7128);
    assertThat(rows.get(1).getField("location")).isNull();
    assertThat(rows).extracting(r -> r.getField("added")).containsOnlyNulls();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void missingContainerChildIdsFollowMissingFieldRules(boolean mapping) throws IOException {
    List<Record> full = read(SCHEMA, mapping);
    assertThat(full).hasSize(3);
    for (String field : List.of("tags", "props")) {
      assertThat(full).extracting(r -> r.getField(field)).containsOnlyNulls();
      List<Record> projected = read(SCHEMA.select(field), mapping);
      assertThat(projected).hasSize(3);
      assertThat(projected).extracting(r -> r.getField(field)).containsOnlyNulls();
    }
    assertThat(full).extracting(r -> r.getField("id")).containsExactly(1L, 2L, 3L);
  }

  @TempDir private Path temp;

  @Test
  void missingRequiredContainerFailsWithMissingFieldError() {
    Schema required = new Schema(Types.NestedField.required(4, "tags", SCHEMA.findType("tags")));
    assertThatThrownBy(() -> read(required, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing required field: tags");
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,false,false", "false,false,false,true",
    "true,false,false,false", "true,false,false,true",
    "false,true,false,false", "false,true,false,true",
    "false,false,true,false", "false,false,true,true",
    "true,true,true,false", "true,true,true,true"
  })
  void resolutionDoesNotDependOnMappingOrColumnOrder(
      boolean missingElement, boolean missingKey, boolean missingValue, boolean reverse)
      throws IOException {
    File file = write(missingElement, missingKey, missingValue, reverse, false);
    Schema schema = SCHEMA.select("id", "tags", "props");
    for (boolean mapping : List.of(false, true)) {
      List<Record> full = read(file, schema, mapping);
      assertThat(full).hasSize(1);
      Record row = full.get(0);
      assertThat(row.getField("id")).isEqualTo(42L);
      assertThat(row.getField("tags")).isEqualTo(missingElement ? null : List.of("tag"));
      assertThat(row.getField("props"))
          .isEqualTo(missingKey || missingValue ? null : Map.of("key", 7));
      for (String name : List.of("id", "tags", "props")) {
        assertThat(read(file, schema.select(name), mapping).get(0).getField(name))
            .isEqualTo(row.getField(name));
      }
    }
  }

  @Test
  void idlessFileStillUsesNameMapping() throws IOException {
    File file = write(true, true, true, true, true);
    Record row = read(file, SCHEMA.select("id", "tags", "props"), true).get(0);
    assertThat(row.getField("id")).isEqualTo(42L);
    assertThat(row.getField("tags")).isEqualTo(List.of("tag"));
    assertThat(row.getField("props")).isEqualTo(Map.of("key", 7));
  }

  @Test
  void fileWithOnlyMissingContainerStillReturnsRows() throws IOException {
    File file =
        writeShape(
            "optional group tags (LIST) = 4 { repeated group list { optional binary element (STRING); } }");
    List<Record> rows = read(file, SCHEMA.select("tags"), false);
    assertThat(rows).hasSize(2);
    assertThat(rows).extracting(r -> r.getField("tags")).containsOnlyNulls();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unsupportedStructPresenceFailsExplicitly(boolean idlessSibling) throws IOException {
    File file =
        writeShape(
            "optional group nested = 20 { "
                + (idlessSibling ? "optional binary marker (STRING); " : "")
                + "optional group tags (LIST) = 4 { repeated group list { optional binary element (STRING); } } }");
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                20, "nested", Types.StructType.of(SCHEMA.findField("tags"))));
    assertThatThrownBy(() -> read(file, schema, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("POC cannot preserve struct presence");
  }

  @Test
  void survivingSiblingPreservesStructPresence() throws IOException {
    File file =
        writeShape(
            "optional group nested = 20 { optional binary marker (STRING) = 21; optional group tags (LIST) = 4 { repeated group list { optional binary element (STRING); } } }");
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                20, "nested", Types.StructType.of(SCHEMA.findField("tags"))));
    List<Record> rows = read(file, schema, false);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getField("nested")).isNotNull();
    assertThat(((Record) rows.get(0).getField("nested")).getField("tags")).isNull();
    assertThat(rows.get(1).getField("nested")).isNull();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void pushedNullFiltersPreserveResidualResults(boolean isNull) throws IOException {
    File file =
        new File(TestPartialFieldIds.class.getResource("/partial-field-ids/a.parquet").getFile());
    try (CloseableIterable<Record> records =
        Parquet.read(Files.localInput(file))
            .project(SCHEMA)
            .filter(isNull ? Expressions.isNull("tags") : Expressions.notNull("tags"))
            .createReaderFunc(schema -> GenericParquetReaders.buildReader(SCHEMA, schema))
            .build()) {
      List<Record> rows = Lists.newArrayList(records);
      Evaluator residual =
          new Evaluator(
              SCHEMA.asStruct(),
              isNull ? Expressions.isNull("tags") : Expressions.notNull("tags"),
              true);
      assertThat(rows.stream().filter(residual::eval).count()).isEqualTo(isNull ? 3L : 0L);
      assertThat(rows).allSatisfy(row -> assertThat(row.getField("tags")).isNull());
    }
  }

  private File writeShape(String fields) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message test { " + fields + " }");
    File file = temp.resolve("shape.parquet").toFile();
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(ParquetIO.file(Files.localOutput(file)))
            .withType(schema)
            .build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(schema);
      Group first = factory.newGroup();
      Group parent = schema.containsField("nested") ? first.addGroup("nested") : first;
      parent.addGroup("tags").addGroup("list").add("element", "tag");
      writer.write(first);
      writer.write(factory.newGroup());
    }
    return file;
  }

  private File write(
      boolean missingElement,
      boolean missingKey,
      boolean missingValue,
      boolean reverse,
      boolean noIds)
      throws IOException {
    String id = "required int64 id" + (noIds ? "" : " = 1") + ";";
    String tags =
        "optional group tags (LIST)"
            + (noIds ? "" : " = 4")
            + " { repeated group list { optional binary element (STRING)"
            + (missingElement ? "" : " = 9")
            + "; } }";
    String props =
        "optional group props (MAP)"
            + (noIds ? "" : " = 5")
            + " { repeated group key_value { required binary key (STRING)"
            + (missingKey ? "" : " = 10")
            + "; optional int32 value"
            + (missingValue ? "" : " = 11")
            + "; } }";
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message test { " + (reverse ? props + tags + id : id + tags + props) + " }");
    File file = temp.resolve("input.parquet").toFile();
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(ParquetIO.file(Files.localOutput(file)))
            .withType(schema)
            .build()) {
      Group row = new SimpleGroupFactory(schema).newGroup();
      row.add("id", 42L);
      row.addGroup("tags").addGroup("list").add("element", "tag");
      Group entry = row.addGroup("props").addGroup("key_value");
      entry.add("key", "key");
      entry.add("value", 7);
      writer.write(row);
    }
    return file;
  }

  private List<Record> read(Schema projection, boolean mapping) throws IOException {
    File file =
        new File(TestPartialFieldIds.class.getResource("/partial-field-ids/a.parquet").getFile());
    return read(file, projection, mapping);
  }

  private List<Record> read(File file, Schema projection, boolean mapping) throws IOException {
    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(file))
            .project(projection)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema));
    if (mapping) {
      builder.withNameMapping(MappingUtil.create(SCHEMA));
    }
    List<Record> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records = builder.build()) {
      records.forEach(rows::add);
    }
    return rows;
  }
}
