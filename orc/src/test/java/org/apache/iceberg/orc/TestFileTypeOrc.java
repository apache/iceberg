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
package org.apache.iceberg.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFileTypeOrc {
  private static final Types.FileType PHOTO = Types.FileType.of(2);
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "photo", PHOTO),
          optional(9, "data", Types.StringType.get()));

  @TempDir private Path temp;

  @Test
  void convertsToAnOrcStructTaggedAsAFile() {
    TypeDescription photo = ORCSchemaUtil.convert(SCHEMA).getChildren().get(1);

    assertThat(photo.getCategory()).isEqualTo(TypeDescription.Category.STRUCT);
    assertThat(photo.getAttributeValue(ORCSchemaUtil.ICEBERG_STRUCT_TYPE_ATTRIBUTE))
        .isEqualTo(ORCSchemaUtil.FILE);
    assertThat(photo.getFieldNames()).isEqualTo(fieldNames());
    assertThat(photo.getChildren().stream().map(ORCSchemaUtil::fieldId))
        .containsExactly(fieldIds());
  }

  @Test
  void buildsAProjectionOverTheNestedFields() {
    TypeDescription photo =
        ORCSchemaUtil.buildOrcProjection(SCHEMA, ORCSchemaUtil.convert(SCHEMA))
            .getChildren()
            .get(1);

    assertThat(photo.getAttributeValue(ORCSchemaUtil.ICEBERG_STRUCT_TYPE_ATTRIBUTE))
        .isEqualTo(ORCSchemaUtil.FILE);
    assertThat(photo.getFieldNames()).isEqualTo(fieldNames());
  }

  @Test
  void buildsAProjectionForAFileColumnMissingFromTheOrcSchema() {
    Schema withoutPhoto =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(9, "data", Types.StringType.get()));

    TypeDescription photo =
        ORCSchemaUtil.buildOrcProjection(SCHEMA, ORCSchemaUtil.convert(withoutPhoto))
            .getChildren()
            .get(1);

    assertThat(photo.getAttributeValue(ORCSchemaUtil.ICEBERG_STRUCT_TYPE_ATTRIBUTE))
        .isEqualTo(ORCSchemaUtil.FILE);
    assertThat(photo.getFieldNames())
        .isEqualTo(
            PHOTO.fields().stream()
                .map(field -> field.name() + "_r" + field.fieldId())
                .collect(Collectors.toList()));
  }

  @Test
  void convertsBackToTheFileType() {
    Schema converted = ORCSchemaUtil.convert(ORCSchemaUtil.convert(SCHEMA));

    assertThat(converted.findField("photo").type()).isEqualTo(PHOTO);
    assertThat(converted.asStruct()).isEqualTo(SCHEMA.asStruct());
  }

  @Test
  void convertsBackToTheFileTypeAfterANameMappingIsApplied() {
    TypeDescription withoutIds = ORCSchemaUtil.removeIds(ORCSchemaUtil.convert(SCHEMA));
    TypeDescription withIds =
        ORCSchemaUtil.applyNameMapping(withoutIds, MappingUtil.create(SCHEMA));

    assertThat(ORCSchemaUtil.convert(withIds).findField("photo").type()).isEqualTo(PHOTO);
  }

  @Test
  void roundTripsAllNestedFields() throws IOException {
    List<Record> expected = records();
    OutputFile file = writeFile(expected);

    List<Record> actual;
    try (CloseableIterable<Record> reader =
        ORC.read(file.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .build()) {
      actual = Lists.newArrayList(reader);
    }

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void collectsMetricsForTheNestedFields() throws IOException {
    DataFile dataFile = writeDataFile(records());

    assertThat(dataFile.nullValueCounts()).containsKeys(fieldIds());
    assertThat(dataFile.nullValueCounts().get(PHOTO.field("checksum").fieldId())).isEqualTo(2L);
    assertThat(dataFile.lowerBounds()).containsKeys(PHOTO.field("uri").fieldId());
  }

  @Test
  void doesNotPushDownPredicatesOnAFileColumn() {
    Expression bound = Binder.bind(SCHEMA.asStruct(), Expressions.isNull("photo"), true);

    SearchArgument actual =
        ExpressionToSearchArgument.convert(bound, ORCSchemaUtil.convert(SCHEMA));

    assertThat(actual.toString())
        .isEqualTo(
            SearchArgumentFactory.newBuilder()
                .literal(SearchArgument.TruthValue.YES_NO_NULL)
                .build()
                .toString());
  }

  private static List<String> fieldNames() {
    return PHOTO.fields().stream().map(Types.NestedField::name).collect(Collectors.toList());
  }

  private static Integer[] fieldIds() {
    return PHOTO.fields().stream().map(Types.NestedField::fieldId).toArray(Integer[]::new);
  }

  private static List<Record> records() {
    GenericRecord row = GenericRecord.create(SCHEMA);
    GenericRecord photo = GenericRecord.create(PHOTO.asStruct());

    return ImmutableList.of(
        row.copy(
            ImmutableMap.of(
                "id",
                1L,
                "photo",
                photo.copy(
                    ImmutableMap.of(
                        "uri",
                        "s3://bucket/full",
                        "offset",
                        128L,
                        "size",
                        1024L,
                        "content_type",
                        "image/png",
                        "checksum",
                        "deadbeef",
                        "inline",
                        ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8)))),
                "data",
                "a")),
        row.copy(
            ImmutableMap.of(
                "id",
                2L,
                "photo",
                photo.copy(ImmutableMap.of("uri", "s3://bucket/partial", "size", 8L)),
                "data",
                "b")),
        // the whole file column is null
        row.copy(ImmutableMap.of("id", 3L, "data", "c")));
  }

  private OutputFile outputFile() throws IOException {
    File file = File.createTempFile("test", ".orc", temp.toFile());
    assertThat(file.delete()).isTrue();
    return Files.localOutput(file);
  }

  private OutputFile writeFile(List<Record> rows) throws IOException {
    OutputFile file = outputFile();
    writer(file, rows);
    return file;
  }

  private DataFile writeDataFile(List<Record> rows) throws IOException {
    return writer(outputFile(), rows).toDataFile();
  }

  private DataWriter<Record> writer(OutputFile file, List<Record> rows) throws IOException {
    DataWriter<Record> writer =
        ORC.writeData(file)
            .schema(SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try (DataWriter<Record> toClose = writer) {
      for (Record row : rows) {
        toClose.write(row);
      }
    }

    return writer;
  }
}
