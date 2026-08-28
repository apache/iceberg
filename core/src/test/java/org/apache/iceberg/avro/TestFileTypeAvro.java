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
package org.apache.iceberg.avro;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFileTypeAvro {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));

  @TempDir private Path temp;

  @Test
  void visitsAFileColumnWithATypedAvroVisitor() {
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(SCHEMA, "table");

    assertThat(AvroSchemaWithTypeVisitor.visit(SCHEMA, avroSchema, new FieldNameCollector()))
        .contains("uri", "offset", "size", "content_type", "checksum", "inline");
  }

  @Test
  void roundTripsAFileColumnThroughAvro() throws IOException {
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

    OutputFile out = Files.localOutput(temp.resolve("file-type.avro").toFile());
    try (FileAppender<GenericData.Record> writer =
        Avro.write(out).schema(SCHEMA).named("table").build()) {
      writer.add(row);
    }

    List<GenericData.Record> rows;
    try (AvroIterable<GenericData.Record> reader =
        Avro.read(out.toInputFile()).project(SCHEMA).build()) {
      rows = Lists.newArrayList(reader);
    }

    assertThat(rows).hasSize(1);
    GenericData.Record readPhoto = (GenericData.Record) rows.get(0).get("photo");
    assertThat(readPhoto.get("uri")).hasToString("s3://bucket/photo");
    assertThat(readPhoto.get("offset")).isEqualTo(128L);
    assertThat(readPhoto.get("size")).isEqualTo(1024L);
  }

  private static class FieldNameCollector extends AvroSchemaWithTypeVisitor<List<String>> {
    @Override
    public List<String> record(
        Types.StructType iStruct,
        org.apache.avro.Schema record,
        List<String> names,
        List<List<String>> fields) {
      List<String> all = Lists.newArrayList(names);
      fields.stream().filter(java.util.Objects::nonNull).forEach(all::addAll);
      return all;
    }

    @Override
    public List<String> union(
        org.apache.iceberg.types.Type iType,
        org.apache.avro.Schema union,
        List<List<String>> options) {
      List<String> all = Lists.newArrayList();
      options.stream().filter(java.util.Objects::nonNull).forEach(all::addAll);
      return all;
    }

    @Override
    public List<String> array(
        Types.ListType iList, org.apache.avro.Schema array, List<String> element) {
      return element;
    }

    @Override
    public List<String> map(Types.MapType iMap, org.apache.avro.Schema map, List<String> value) {
      return value;
    }

    @Override
    public List<String> primitive(
        org.apache.iceberg.types.Type.PrimitiveType iPrimitive, org.apache.avro.Schema primitive) {
      return Lists.newArrayList();
    }
  }
}
