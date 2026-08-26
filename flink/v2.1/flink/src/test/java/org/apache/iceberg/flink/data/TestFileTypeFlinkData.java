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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestFileTypeFlinkData {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));

  @Test
  void visitsAFileColumnAsARecord() {
    List<String> expected = Lists.newArrayList("id", "photo");
    for (Types.NestedField field : Types.FileType.of(2).fields()) {
      expected.add(field.name());
    }

    List<String> visited =
        FlinkSchemaVisitor.visit(FlinkSchemaUtil.convert(SCHEMA), SCHEMA, new FieldNameCollector());

    assertThat(visited).containsExactlyElementsOf(expected);
  }

  @Test
  void readsAFileColumnFromAvro() throws IOException {
    OutputFile file = writeAvro();

    List<RowData> rows;
    try (CloseableIterable<RowData> reader =
        Avro.read(file.toInputFile())
            .project(SCHEMA)
            .createResolvingReader(FlinkPlannedAvroReader::create)
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getLong(0)).isEqualTo(1L);
    assertPhoto(rows.get(0).getRow(1, Types.FileType.NUM_NESTED_FIELDS));
  }

  @Test
  void readsAProjectionOfAFileColumnFromAvro() throws IOException {
    OutputFile file = writeAvro();
    Schema projection = SCHEMA.select("photo");

    List<RowData> rows;
    try (CloseableIterable<RowData> reader =
        Avro.read(file.toInputFile())
            .project(projection)
            .createResolvingReader(FlinkPlannedAvroReader::create)
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getArity()).isEqualTo(1);
    assertPhoto(rows.get(0).getRow(0, Types.FileType.NUM_NESTED_FIELDS));
  }

  @Test
  void projectsAWholeFileColumn() {
    Schema projected = SCHEMA.select("photo");
    RowDataProjection projection =
        RowDataProjection.create(
            FlinkSchemaUtil.convert(SCHEMA), SCHEMA.asStruct(), projected.asStruct());

    RowData row = projection.wrap(GenericRowData.of(1L, photoRowData()));

    assertThat(row.getArity()).isEqualTo(1);
    assertPhoto(row.getRow(0, Types.FileType.NUM_NESTED_FIELDS));
  }

  @Test
  void projectsASubsetOfAFileColumn() {
    Schema projected = SCHEMA.select("photo.uri");
    assertThat(projected.findField("photo").type().isFileType()).isFalse();

    RowDataProjection projection =
        RowDataProjection.create(
            FlinkSchemaUtil.convert(SCHEMA), SCHEMA.asStruct(), projected.asStruct());

    RowData row = projection.wrap(GenericRowData.of(1L, photoRowData()));

    assertThat(row.getArity()).isEqualTo(1);
    assertThat(row.getRow(0, 1).getString(0)).hasToString("s3://bucket/photo");
  }

  private static void assertPhoto(RowData photo) {
    Types.StructType fields = Types.FileType.of(2).asStruct();

    assertThat(photo.getString(position(fields, "uri"))).hasToString("s3://bucket/photo");
    assertThat(photo.getLong(position(fields, "offset"))).isEqualTo(128L);
    assertThat(photo.getLong(position(fields, "size"))).isEqualTo(1024L);
    assertThat(photo.getString(position(fields, "content_type"))).hasToString("image/png");
    assertThat(photo.getString(position(fields, "checksum"))).hasToString("abc123");
    assertThat(photo.isNullAt(position(fields, "inline"))).isTrue();
  }

  private static int position(Types.StructType struct, String name) {
    return struct.fields().indexOf(struct.field(name));
  }

  private static GenericRowData photoRowData() {
    return GenericRowData.of(
        StringData.fromString("s3://bucket/photo"),
        128L,
        1024L,
        StringData.fromString("image/png"),
        StringData.fromString("abc123"),
        null);
  }

  private static OutputFile writeAvro() throws IOException {
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(SCHEMA, "table");
    org.apache.avro.Schema photoSchema = avroSchema.getField("photo").schema().getTypes().get(1);

    GenericData.Record photo = new GenericData.Record(photoSchema);
    photo.put("uri", "s3://bucket/photo");
    photo.put("offset", 128L);
    photo.put("size", 1024L);
    photo.put("content_type", "image/png");
    photo.put("checksum", "abc123");
    photo.put("inline", null);

    GenericData.Record row = new GenericData.Record(avroSchema);
    row.put("id", 1L);
    row.put("photo", photo);

    OutputFile file = new InMemoryOutputFile();
    try (FileAppender<GenericData.Record> writer =
        Avro.write(file).schema(SCHEMA).named("table").build()) {
      writer.add(row);
    }

    return file;
  }

  private static class FieldNameCollector extends FlinkSchemaVisitor<List<String>> {
    private final List<String> names = Lists.newArrayList();

    @Override
    public void beforeField(Types.NestedField field) {
      names.add(field.name());
    }

    @Override
    public List<String> record(
        Types.StructType iStruct, List<List<String>> results, List<LogicalType> fieldTypes) {
      return names;
    }
  }
}
