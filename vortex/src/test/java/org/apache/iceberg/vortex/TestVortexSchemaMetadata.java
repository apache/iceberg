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

import dev.vortex.api.Session;
import dev.vortex.api.VortexWriter;
import dev.vortex.io.NativeReadable;
import dev.vortex.io.NativeWritable;
import dev.vortex.jni.NativeFiles;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the Iceberg schema Vortex files carry in their file metadata, and the id-based column
 * binding it enables: that the schema is written, that a renamed column is bound through it, that
 * an id the file does not have is not bound by name instead, and that a file carrying no schema
 * falls back to binding by name.
 */
public class TestVortexSchemaMetadata {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(
              3,
              "location",
              Types.StructType.of(
                  required(4, "lat", Types.DoubleType.get()),
                  required(5, "long", Types.DoubleType.get()))),
          optional(6, "tags", Types.ListType.ofOptional(7, Types.StringType.get())),
          optional(
              8,
              "props",
              Types.MapType.ofOptional(9, 10, Types.StringType.get(), Types.IntegerType.get())));

  @TempDir private Path temp;

  @Test
  public void testWrittenFileCarriesTheIcebergSchema() throws IOException {
    InputFile file = write(SCHEMA, record(SCHEMA));

    Map<String, byte[]> metadata =
        NativeFiles.readMetadata(VortexSessions.shared(), readable(file));
    assertThat(metadata).containsKey(VortexSchemas.ICEBERG_SCHEMA_KEY);

    Schema stored =
        SchemaParser.fromJson(
            new String(metadata.get(VortexSchemas.ICEBERG_SCHEMA_KEY), StandardCharsets.UTF_8));
    assertThat(stored.asStruct()).isEqualTo(SCHEMA.asStruct());
  }

  @Test
  public void testRenamedColumnsBindByFieldId() throws IOException {
    InputFile file = write(SCHEMA, record(SCHEMA));

    // Every name differs from the file's, so nothing here can resolve by name.
    Schema renamed =
        new Schema(
            required(1, "row_id", Types.LongType.get()),
            optional(2, "payload", Types.StringType.get()),
            optional(
                3,
                "place",
                Types.StructType.of(
                    required(4, "latitude", Types.DoubleType.get()),
                    required(5, "longitude", Types.DoubleType.get()))),
            optional(6, "labels", Types.ListType.ofOptional(7, Types.StringType.get())),
            optional(
                8,
                "attributes",
                Types.MapType.ofOptional(9, 10, Types.StringType.get(), Types.IntegerType.get())));

    Record read = readOne(file, renamed);

    assertThat(read.getField("row_id")).isEqualTo(1L);
    assertThat(read.getField("payload")).isEqualTo("a");
    Record place = (Record) read.getField("place");
    assertThat(place.getField("latitude")).isEqualTo(1.0d);
    assertThat(place.getField("longitude")).isEqualTo(2.0d);
    assertThat(read.getField("labels")).isEqualTo(List.of("x", "y"));
    assertThat(read.getField("attributes")).isEqualTo(Map.of("k", 7));
  }

  @Test
  public void testAddedColumnReusingAnOldNameIsNotBoundToIt() throws IOException {
    InputFile file = write(SCHEMA, record(SCHEMA));

    // Field 2 was renamed data -> payload, and a new field 11 took the name "data". Binding by
    // name would hand field 11 the old column's values; binding by id must leave it null.
    Schema evolved =
        new Schema(
            required(1, "row_id", Types.LongType.get()),
            optional(2, "payload", Types.StringType.get()),
            optional(11, "data", Types.StringType.get()));

    Record read = readOne(file, evolved);

    assertThat(read.getField("payload")).isEqualTo("a");
    assertThat(read.getField("data")).isNull();
  }

  @Test
  public void testFileWithoutAnIcebergSchemaStillBindsByName() throws IOException {
    // Written straight through the Vortex writer with no metadata, standing in for a file whose
    // producer stores no Iceberg schema.
    Schema flat =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
    InputFile file = writeWithoutMetadata(flat);

    Map<String, byte[]> metadata =
        NativeFiles.readMetadata(VortexSessions.shared(), readable(file));
    assertThat(metadata).doesNotContainKey(VortexSchemas.ICEBERG_SCHEMA_KEY);

    // Matching names still resolve...
    Record read = readOne(file, flat);
    assertThat(read.getField("id")).isEqualTo(1L);
    assertThat(read.getField("data")).isEqualTo("a");

    // ...and a rename cannot be recovered, because the file carries no ids to bind to.
    Schema renamed =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "payload", Types.StringType.get()));
    assertThat(readOne(file, renamed).getField("payload")).isNull();
  }

  @Test
  public void testNameMappingSuppliesFieldIdsForAFileWithoutASchema() throws IOException {
    Schema flat =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
    InputFile file = writeWithoutMetadata(flat);

    // The file carries no ids, so a rename can only be resolved through a name mapping built from
    // the schema the file was written with.
    NameMapping mapping = MappingUtil.create(flat);
    Schema renamed =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "payload", Types.StringType.get()));

    assertThat(readOne(file, renamed, null).getField("payload")).isNull();
    assertThat(readOne(file, renamed, mapping).getField("payload")).isEqualTo("a");
  }

  @Test
  public void testStoredSchemaWinsOverNameMapping() throws IOException {
    InputFile file = write(SCHEMA, record(SCHEMA));

    // A mapping that points the file's own names at unrelated ids must not displace the schema the
    // file carries.
    NameMapping mapping = NameMapping.of(MappedField.of(999, "id"), MappedField.of(998, "data"));
    Record read = readOne(file, SCHEMA, mapping);

    assertThat(read.getField("id")).isEqualTo(1L);
    assertThat(read.getField("data")).isEqualTo("a");
  }

  private static Record record(Schema schema) {
    Record location = GenericRecord.create(schema.findType("location").asStructType());
    location.setField("lat", 1.0d);
    location.setField("long", 2.0d);

    Record record = GenericRecord.create(schema);
    record.setField("id", 1L);
    record.setField("data", "a");
    record.setField("location", location);
    record.setField("tags", List.of("x", "y"));
    record.setField("props", Map.of("k", 7));
    return record;
  }

  private InputFile write(Schema schema, Record record) throws IOException {
    OutputFile outputFile =
        Files.localOutput(temp.resolve("meta-" + System.nanoTime() + ".vortex").toFile());
    try (FileAppender<Record> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(schema)
            .content(FileContent.DATA)
            .build()) {
      appender.add(record);
    }

    return outputFile.toInputFile();
  }

  /** Writes a single row with the raw Vortex writer, so the file carries no Iceberg schema. */
  private InputFile writeWithoutMetadata(Schema schema) throws IOException {
    OutputFile outputFile =
        Files.localOutput(temp.resolve("bare-" + System.nanoTime() + ".vortex").toFile());
    dev.vortex.relocated.org.apache.arrow.memory.BufferAllocator allocator =
        dev.vortex.arrow.ArrowAllocation.rootAllocator();
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema vortexSchema =
        VortexSchemas.toVortexArrowSchema(schema);
    Session session = VortexSessions.shared();

    try (NativeWritable out = VortexIO.writable(outputFile);
        dev.vortex.relocated.org.apache.arrow.vector.VectorSchemaRoot root =
            dev.vortex.relocated.org.apache.arrow.vector.VectorSchemaRoot.create(
                vortexSchema, allocator);
        VortexWriter writer = VortexWriter.builder(session, out, vortexSchema, allocator).build()) {
      root.allocateNew();
      ((dev.vortex.relocated.org.apache.arrow.vector.BigIntVector) root.getVector("id"))
          .setSafe(0, 1L);
      ((dev.vortex.relocated.org.apache.arrow.vector.VarCharVector) root.getVector("data"))
          .setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
      root.setRowCount(1);

      try (dev.vortex.relocated.org.apache.arrow.c.ArrowArray array =
              dev.vortex.relocated.org.apache.arrow.c.ArrowArray.allocateNew(allocator);
          dev.vortex.relocated.org.apache.arrow.c.ArrowSchema cSchema =
              dev.vortex.relocated.org.apache.arrow.c.ArrowSchema.allocateNew(allocator)) {
        dev.vortex.relocated.org.apache.arrow.c.Data.exportVectorSchemaRoot(
            allocator, root, null, array, cSchema);
        writer.writeBatch(array.memoryAddress(), cSchema.memoryAddress());
      }

      writer.finish();
    }

    return outputFile.toInputFile();
  }

  private static NativeReadable readable(InputFile file) {
    return VortexIO.readable(file);
  }

  private static Record readOne(InputFile file, Schema projection) throws IOException {
    return readOne(file, projection, null);
  }

  private static Record readOne(InputFile file, Schema projection, NameMapping mapping)
      throws IOException {
    try (CloseableIterable<Record> records =
        formatModel().readBuilder(file).project(projection).withNameMapping(mapping).build()) {
      List<Record> rows = Lists.newArrayList(records);
      assertThat(rows).hasSize(1);
      return rows.get(0);
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
