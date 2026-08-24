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

import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFileTypeParquet {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "photo", Types.FileType.of(2)),
          optional(9, "data", Types.StringType.get()));

  @TempDir private Path temp;

  @Test
  void convertsToTheParquetFileGroup() {
    MessageType expected =
        MessageTypeParser.parseMessageType(
            "message table {"
                + "  required int64 id = 1;"
                + "  optional group photo = 2 {"
                + "    optional binary uri (STRING) = 3;"
                + "    optional int64 offset = 4;"
                + "    optional int64 size = 5;"
                + "    optional binary content_type (STRING) = 6;"
                + "    optional binary checksum (STRING) = 7;"
                + "    optional binary inline = 8;"
                + "  }"
                + "  optional binary data (STRING) = 9;"
                + "}");

    assertThat(ParquetSchemaUtil.convert(SCHEMA, "table")).isEqualTo(expected);
  }

  @Test
  void convertsARequiredFileColumn() {
    Schema schema = new Schema(required(2, "photo", Types.FileType.of(2)));

    MessageType expected =
        MessageTypeParser.parseMessageType(
            "message table {"
                + "  required group photo = 2 {"
                + "    optional binary uri (STRING) = 3;"
                + "    optional int64 offset = 4;"
                + "    optional int64 size = 5;"
                + "    optional binary content_type (STRING) = 6;"
                + "    optional binary checksum (STRING) = 7;"
                + "    optional binary inline = 8;"
                + "  }"
                + "}");

    assertThat(ParquetSchemaUtil.convert(schema, "table")).isEqualTo(expected);
  }

  @Test
  void convertsAFileListElement() {
    Schema schema =
        new Schema(optional(1, "photos", Types.ListType.ofOptional(2, Types.FileType.of(2))));

    MessageType expected =
        MessageTypeParser.parseMessageType(
            "message table {"
                + "  optional group photos (LIST) = 1 {"
                + "    repeated group list {"
                + "      optional group element = 2 {"
                + "        optional binary uri (STRING) = 3;"
                + "        optional int64 offset = 4;"
                + "        optional int64 size = 5;"
                + "        optional binary content_type (STRING) = 6;"
                + "        optional binary checksum (STRING) = 7;"
                + "        optional binary inline = 8;"
                + "      }"
                + "    }"
                + "  }"
                + "}");

    assertThat(ParquetSchemaUtil.convert(schema, "table")).isEqualTo(expected);
  }

  @Test
  void convertsAFileMapValue() {
    Schema schema =
        new Schema(
            optional(
                1,
                "byName",
                Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.FileType.of(3))));

    MessageType expected =
        MessageTypeParser.parseMessageType(
            "message table {"
                + "  optional group byName (MAP) = 1 {"
                + "    repeated group key_value {"
                + "      required binary key (STRING) = 2;"
                + "      optional group value = 3 {"
                + "        optional binary uri (STRING) = 4;"
                + "        optional int64 offset = 5;"
                + "        optional int64 size = 6;"
                + "        optional binary content_type (STRING) = 7;"
                + "        optional binary checksum (STRING) = 8;"
                + "        optional binary inline = 9;"
                + "      }"
                + "    }"
                + "  }"
                + "}");

    assertThat(ParquetSchemaUtil.convert(schema, "table")).isEqualTo(expected);
  }

  @Test
  void convertsBackToAPlainStructWithoutTheFileAnnotation() {
    Schema converted = ParquetSchemaUtil.convert(ParquetSchemaUtil.convert(SCHEMA, "table"));

    // parquet 1.17.1 has no FILE annotation, so the group is indistinguishable from a struct here.
    // Readers recover the file type from the expected Iceberg schema instead.
    assertThat(converted.findField("photo").type().isFileType()).isFalse();
    assertThat(converted.findField("photo").type())
        .isEqualTo(Types.StructType.of(Types.FileType.of(2).fields()));
  }

  @Test
  void prunesToASingleNestedField() {
    MessageType pruned =
        ParquetSchemaUtil.pruneColumns(ParquetSchemaUtil.convert(SCHEMA, "table"), uriProjection());

    assertThat(pruned.getColumns()).hasSize(1);
    assertThat(pruned.getColumns().get(0).getPath()).containsExactly("photo", "uri");
  }

  @Test
  void roundTripsAllNestedFields() throws IOException {
    List<Record> expected = records();
    OutputFile file = write(expected);

    List<Record> actual;
    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
            .build()) {
      actual = Lists.newArrayList(reader);
    }

    assertThat(actual).hasSameSizeAs(expected);
    assertThat(record(actual, 0)).isEqualTo(expected.get(0).getField("photo"));
    assertThat(record(actual, 1).getField("uri")).isEqualTo("s3://bucket/partial");
    assertThat(record(actual, 1).getField("checksum")).isNull();
    assertThat(actual.get(2).getField("photo")).isNull();
  }

  @Test
  void roundTripsAFileListElement() throws IOException {
    Schema schema =
        new Schema(optional(1, "photos", Types.ListType.ofOptional(2, Types.FileType.of(2))));
    GenericRecord photo = GenericRecord.create(Types.FileType.of(2));
    Record expected =
        GenericRecord.create(schema)
            .copy(
                ImmutableMap.of(
                    "photos",
                    ImmutableList.of(
                        photo.copy(ImmutableMap.of("uri", "s3://bucket/a", "size", 1L)),
                        photo.copy(ImmutableMap.of("uri", "s3://bucket/b")))));

    OutputFile file = Files.localOutput(createTempFile(temp));
    try (DataWriter<Record> writer =
        Parquet.writeData(file)
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      writer.write(expected);
    }

    List<Record> actual;
    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      actual = Lists.newArrayList(reader);
    }

    assertThat(actual).hasSize(1);
    assertThat(actual.get(0).getField("photos")).isEqualTo(expected.getField("photos"));
  }

  @Test
  void readsAProjectionOfASingleNestedField() throws IOException {
    OutputFile file = write(records());
    Schema projection = uriProjection();

    List<Record> actual;
    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(projection)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema))
            .build()) {
      actual = Lists.newArrayList(reader);
    }

    assertThat(actual).hasSize(3);
    Record photo = record(actual, 0);
    assertThat(photo.struct().fields()).hasSize(1);
    assertThat(photo.getField("uri")).isEqualTo("s3://bucket/full");
  }

  @Test
  void collectsMetricsForNestedFieldsButNotTheContainer() throws IOException {
    DataFile dataFile = writeDataFile(records());

    assertThat(dataFile.nullValueCounts()).containsKeys(3, 4, 5, 6, 7, 8).doesNotContainKey(2);
    assertThat(dataFile.lowerBounds()).containsKeys(3, 4, 5).doesNotContainKey(2);
    assertThat(dataFile.nullValueCounts().get(3)).isEqualTo(1L);
    assertThat(dataFile.nullValueCounts().get(7)).isEqualTo(2L);
  }

  private static Schema uriProjection() {
    return new Schema(
        optional(2, "photo", Types.StructType.of(optional(3, "uri", Types.StringType.get()))));
  }

  private static List<Record> records() {
    GenericRecord row = GenericRecord.create(SCHEMA);
    GenericRecord photo = GenericRecord.create(Types.FileType.of(2));

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

  private static Record record(List<Record> rows, int position) {
    return (Record) rows.get(position).getField("photo");
  }

  private OutputFile write(List<Record> rows) throws IOException {
    OutputFile file = Files.localOutput(createTempFile(temp));
    DataWriter<Record> writer =
        Parquet.writeData(file)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try (DataWriter<Record> toClose = writer) {
      for (Record row : rows) {
        toClose.write(row);
      }
    }

    return file;
  }

  private DataFile writeDataFile(List<Record> rows) throws IOException {
    OutputFile file = Files.localOutput(createTempFile(temp));
    DataWriter<Record> writer =
        Parquet.writeData(file)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try (DataWriter<Record> toClose = writer) {
      for (Record row : rows) {
        toClose.write(row);
      }
    }

    return writer.toDataFile();
  }
}
