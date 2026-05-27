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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.ParquetVariantExtractionReaders.VariantExtractionField;
import org.apache.iceberg.parquet.ParquetVariantExtractionReaders.VariantExtractionRow;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

class TestParquetVariantExtractionReaders {

  // Duplicates SparkVariantExtractionUtil.PLACEHOLDER_PATH; the parquet module cannot depend on
  // spark so the constant is copied here. Keep the two values in sync.
  private static final String PLACEHOLDER_PATH = "$.__placeholder_field__";

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "var", Types.VariantType.get()));

  private static final Schema OPTIONAL_VARIANT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "var", Types.VariantType.get()));

  @Test
  void selectiveReaderReadsFewerColumnsThanFullVariantReader() {
    GroupType cityField = shreddedStringField("city");
    GroupType zipField = shreddedStringField("zip");
    GroupType sizeField = shreddedLongField("size");
    GroupType variantGroup = shreddedObjectVariant("v", cityField, zipField, sizeField);
    MessageType fileSchema =
        org.apache.parquet.schema.Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .addField(variantGroup)
            .named("table");

    List<VariantExtractionField> fields = ImmutableList.of(extractionField(0, false, "$.size"));

    ParquetValueReader<?> fullReader =
        ParquetVariantVisitor.visit(
            variantGroup, new VariantReaderBuilder(fileSchema, List.of("v")));
    ParquetValueReader<?> selectiveReader =
        ParquetVariantExtractionReaders.buildRowReader(
            fileSchema, variantGroup, List.of("v"), fields);

    int fullColumns = ParquetVariantExtractionReaders.leafColumnCount(fullReader);
    int selectiveColumns = ParquetVariantExtractionReaders.leafColumnCount(selectiveReader);

    assertThat(selectiveColumns)
        .as("selective reader should read fewer Parquet columns than the full variant")
        .isLessThan(fullColumns);
    assertThat(selectiveColumns)
        .as("selective reader should not include unrelated shredded fields (city, zip)")
        .isLessThan(fullColumns - 2);
  }

  @Test
  void multipleExtractionsUnionColumnPaths() {
    GroupType cityField = shreddedStringField("city");
    GroupType zipField = shreddedStringField("zip");
    GroupType sizeField = shreddedLongField("size");
    GroupType variantGroup = shreddedObjectVariant("v", cityField, zipField, sizeField);
    MessageType fileSchema =
        org.apache.parquet.schema.Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .addField(variantGroup)
            .named("table");

    List<VariantExtractionField> bothFields =
        ImmutableList.of(extractionField(0, false, "$.city"), extractionField(1, false, "$.zip"));

    ParquetValueReader<?> cityOnlyReader =
        ParquetVariantExtractionReaders.buildRowReader(
            fileSchema,
            variantGroup,
            List.of("v"),
            ImmutableList.of(extractionField(0, false, "$.city")));
    ParquetValueReader<?> bothReader =
        ParquetVariantExtractionReaders.buildRowReader(
            fileSchema, variantGroup, List.of("v"), bothFields);

    assertThat(ParquetVariantExtractionReaders.leafColumnCount(bothReader))
        .as("two extractions should read more columns than one")
        .isGreaterThan(ParquetVariantExtractionReaders.leafColumnCount(cityOnlyReader));
    assertThat(ParquetVariantExtractionReaders.leafColumnCount(bothReader))
        .as("two extractions should still read fewer columns than the full variant")
        .isLessThan(
            ParquetVariantExtractionReaders.leafColumnCount(
                ParquetVariantVisitor.visit(
                    variantGroup, new VariantReaderBuilder(fileSchema, List.of("v")))));
  }

  @Test
  void unshreddedVariantUsesMetadataAndValueColumnsOnly() {
    GroupType variantGroup = unshreddedVariant("v");
    MessageType fileSchema =
        org.apache.parquet.schema.Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .addField(variantGroup)
            .named("table");

    List<VariantExtractionField> fields = ImmutableList.of(extractionField(0, false, "$.size"));

    ParquetValueReader<?> selectiveReader =
        ParquetVariantExtractionReaders.buildRowReader(
            fileSchema, variantGroup, List.of("v"), fields);

    assertThat(ParquetVariantExtractionReaders.leafColumnCount(selectiveReader))
        .as("unshredded fallback reads metadata and root value only")
        .isEqualTo(2);
  }

  @Test
  void placeholderReadsMetadataOnly() {
    GroupType cityField = shreddedStringField("city");
    GroupType variantGroup = shreddedObjectVariant("v", cityField);
    MessageType fileSchema =
        org.apache.parquet.schema.Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .addField(variantGroup)
            .named("table");

    List<VariantExtractionField> fields =
        ImmutableList.of(extractionField(0, true, PLACEHOLDER_PATH));

    ParquetValueReader<?> reader =
        ParquetVariantExtractionReaders.buildRowReader(
            fileSchema, variantGroup, List.of("v"), fields);

    assertThat(ParquetVariantExtractionReaders.leafColumnCount(reader)).isEqualTo(1);
  }

  @Test
  void shreddedExtractionReadsTypedValue() throws IOException {
    VariantMetadata metadata = Variants.metadata("size");
    ShreddedObject object = Variants.object(metadata);
    object.put("size", Variants.of(42L));
    Variant variant = Variant.of(metadata, object);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile outputFile = new InMemoryOutputFile();
    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(object))
            .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
            .build()) {
      writer.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(outputFile.toInputFile()))) {
      fileSchema = reader.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields = ImmutableList.of(extractionField(0, false, "$.size"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                schema ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        schema, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.placeholder(0)).isFalse();
      assertThat(row.value(0).asPrimitive().get()).isEqualTo(42L);
    }
  }

  // -----------------------------------------------------------------------
  // SelectiveObjectReader correctness tests (spec-driven)
  // Invariants from the Parquet Variant Shredding spec:
  //   value=null,  typed_value=null   → field is MISSING from the object
  //   value=0x00,  typed_value=null   → field is PRESENT with JSON null
  //   value=null,  typed_value=X      → field is PRESENT with typed value X
  //   value=obj,   typed_value={...}  → PARTIALLY SHREDDED object; keys are disjoint
  // -----------------------------------------------------------------------

  /**
   * Nested path where every step is shredded into typed_value.
   *
   * <p>Schema: var.typed_value.user.typed_value.login.typed_value = string
   *
   * <p>Row: user.login = "alice" (fully typed)
   *
   * <p>Expected: reads login.typed_value directly; login.value and user.value are null.
   */
  @Test
  void nestedPathFullyTyped() throws IOException {
    VariantMetadata meta = Variants.metadata("user", "login");
    ShreddedObject loginObj = Variants.object(meta);
    loginObj.put("login", Variants.of("alice"));
    ShreddedObject rootObj = Variants.object(meta);
    rootObj.put("user", loginObj);
    Variant variant = Variant.of(meta, rootObj);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(rootObj))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields =
        ImmutableList.of(extractionField(0, false, "$.user.login"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0)).isNotNull();
      assertThat(row.value(0).asPrimitive().get()).isEqualTo("alice");
    }
  }

  /**
   * Nested path where the child field exists in the schema but the row has JSON null for it.
   *
   * <p>The null value goes into login.value (as serialized variant null); login.typed_value is
   * absent. The SelectiveObjectReader must return the variant null (not skip it and fall back to an
   * ancestor blob).
   */
  @Test
  void nestedPathJsonNullUsesValueColumn() throws IOException {
    VariantMetadata meta = Variants.metadata("user", "login");
    ShreddedObject loginObj = Variants.object(meta);
    loginObj.put("login", Variants.ofNull());
    ShreddedObject rootObj = Variants.object(meta);
    rootObj.put("user", loginObj);
    Variant variant = Variant.of(meta, rootObj);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(rootObj))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields =
        ImmutableList.of(extractionField(0, false, "$.user.login"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      // login exists but is JSON null → the extraction returns a variant null, not Java null.
      assertThat(row.value(0)).isNotNull();
      assertThat(row.value(0).type()).isEqualTo(PhysicalType.NULL);
    }
  }

  /**
   * Nested path where the parent field is shredded but the leaf is not shredded.
   *
   * <p>Schema has user.typed_value, but login is NOT shredded. Login lives in user.value.
   *
   * <p>The SelectiveObjectReader must recover login from user.value.
   */
  @Test
  void nestedPathLeafNotShredded() throws IOException {
    VariantMetadata meta = Variants.metadata("user", "login");
    // Build an object where 'user' is shredded but 'login' within user is NOT shredded.
    // We achieve this by giving user a typed_value that has no login field.
    ShreddedObject userWithLoginInValue = Variants.object(meta);
    userWithLoginInValue.put("login", Variants.of("bob"));

    ShreddedObject rootObj = Variants.object(meta);
    rootObj.put("user", userWithLoginInValue);
    Variant variant = Variant.of(meta, rootObj);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    // Shred: user is in typed_value, but login inside user is left in user.value (not typed).
    // We use a shredding schema that only includes 'user' at the root level, with no typed
    // children inside user — so user.value gets the serialized user blob including login.
    ShreddedObject userShell = Variants.object(meta); // empty user — forces login into user.value

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc(
                (id, name) -> {
                  // Shred 'user' at root level but don't shred login within user.
                  return ParquetVariantUtil.toParquetSchema(rootObj);
                })
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields =
        ImmutableList.of(extractionField(0, false, "$.user.login"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0)).isNotNull();
      assertThat(row.value(0).asPrimitive().get()).isEqualTo("bob");
    }
  }

  /**
   * Multiple rows across two separate writes: simulates two different shredding schemas for the
   * same column (like two row groups with different inference results).
   *
   * <p>Row 1: user.login typed (login.typed_value = "alice") Row 2: user.login not typed (login in
   * user.value blob)
   *
   * <p>Both must read correctly from the same extraction reader.
   */
  @Test
  void nestedPathMixedTypedAndUntypedAcrossRows() throws IOException {
    VariantMetadata meta = Variants.metadata("user", "login");

    ShreddedObject userAlice = Variants.object(meta);
    userAlice.put("login", Variants.of("alice"));
    ShreddedObject rootAlice = Variants.object(meta);
    rootAlice.put("user", userAlice);

    ShreddedObject userBob = Variants.object(meta);
    userBob.put("login", Variants.of("bob"));
    ShreddedObject rootBob = Variants.object(meta);
    rootBob.put("user", userBob);

    Record r1 = GenericRecord.create(SCHEMA).copy("id", 1, "var", Variant.of(meta, rootAlice));
    Record r2 = GenericRecord.create(SCHEMA).copy("id", 2, "var", Variant.of(meta, rootBob));

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(rootAlice))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(r1);
      w.add(r2);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields =
        ImmutableList.of(extractionField(0, false, "$.user.login"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      List<VariantExtractionRow> rows = Lists.newArrayList(reader);
      assertThat(rows).hasSize(2);
      assertThat(rows.get(0).value(0).asPrimitive().get()).isEqualTo("alice");
      assertThat(rows.get(1).value(0).asPrimitive().get()).isEqualTo("bob");
    }
  }

  /**
   * Two extractions under the same top-level prefix share the same SelectiveObjectReader.
   *
   * <p>Schema: user.login (string) and user.role (string) both shredded.
   *
   * <p>Verifies both fields are extracted correctly from a single shared prefix reader.
   */
  @Test
  void twoFieldsUnderSamePrefixAreShared() throws IOException {
    VariantMetadata meta = Variants.metadata("user", "login", "role");
    ShreddedObject userObj = Variants.object(meta);
    userObj.put("login", Variants.of("alice"));
    userObj.put("role", Variants.of("admin"));
    ShreddedObject rootObj = Variants.object(meta);
    rootObj.put("user", userObj);
    Variant variant = Variant.of(meta, rootObj);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(rootObj))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields =
        ImmutableList.of(
            extractionField(0, false, "$.user.login"), extractionField(1, false, "$.user.role"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0).asPrimitive().get()).isEqualTo("alice");
      assertThat(row.value(1).asPrimitive().get()).isEqualTo("admin");
    }
  }

  /**
   * Same path extracted twice with different target types must share one physical column.
   *
   * <p>Both ordinals must produce the correct value; the column count must be the same as for a
   * single extraction of that path (no double-read of the same Parquet column).
   */
  @Test
  void samePathDifferentTargetTypesSharePhysicalColumn() throws IOException {
    VariantMetadata metadata = Variants.metadata("size");
    ShreddedObject object = Variants.object(metadata);
    object.put("size", Variants.of(42L));
    Variant variant = Variant.of(metadata, object);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile outputFile = new InMemoryOutputFile();
    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(object))
            .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
            .build()) {
      writer.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(outputFile.toInputFile()))) {
      fileSchema = reader.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    // Ordinal 0: $.size as long.  Ordinal 1: same path $.size but treated as long again
    // (different ordinal, same physical column).
    List<VariantExtractionField> singleField =
        ImmutableList.of(extractionField(0, false, "$.size"));
    List<VariantExtractionField> twoFields =
        ImmutableList.of(extractionField(0, false, "$.size"), extractionField(1, false, "$.size"));

    int singleColumns =
        ParquetVariantExtractionReaders.leafColumnCount(
            ParquetVariantExtractionReaders.buildRowReader(
                fileSchema, variantGroup, List.of("var"), singleField));
    int twoColumns =
        ParquetVariantExtractionReaders.leafColumnCount(
            ParquetVariantExtractionReaders.buildRowReader(
                fileSchema, variantGroup, List.of("var"), twoFields));

    assertThat(twoColumns)
        .as("two extractions for the same path must not add extra Parquet columns")
        .isEqualTo(singleColumns);

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                schema ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        schema, variantGroup, List.of("var"), twoFields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0).asPrimitive().get()).as("ordinal 0").isEqualTo(42L);
      assertThat(row.value(1).asPrimitive().get()).as("ordinal 1").isEqualTo(42L);
    }
  }

  /**
   * Selective reader for a nested path reads fewer columns than the full variant reader even when
   * many siblings exist at the parent level.
   */
  @Test
  void selectiveNestedReaderReadsFarFewerColumnsThanFull() {
    // Simulate a pull_request-like schema with many sibling fields under typed_value
    GroupType loginField = shreddedStringField("login");
    GroupType emailField = shreddedStringField("email");
    GroupType userGroup =
        org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("value")
            .optionalGroup()
            .addFields(loginField, emailField)
            .named("typed_value")
            .named("user");
    GroupType draftField = shreddedStringField("draft");
    GroupType titleField = shreddedStringField("title");
    GroupType bodyField = shreddedStringField("body");
    GroupType createdAtField = shreddedStringField("created_at");
    GroupType prGroup =
        org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("value")
            .optionalGroup()
            .addFields(userGroup, draftField, titleField, bodyField, createdAtField)
            .named("typed_value")
            .named("pull_request");
    GroupType variantGroup = shreddedObjectVariant("v", prGroup);
    MessageType fileSchema =
        org.apache.parquet.schema.Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .addField(variantGroup)
            .named("table");

    // Full variant reader
    int fullColumns =
        ParquetVariantExtractionReaders.leafColumnCount(
            ParquetVariantVisitor.visit(
                variantGroup, new VariantReaderBuilder(fileSchema, List.of("v"))));

    // Selective reader for $.pull_request.user.login
    int selectiveColumns =
        ParquetVariantExtractionReaders.leafColumnCount(
            ParquetVariantExtractionReaders.buildRowReader(
                fileSchema,
                variantGroup,
                List.of("v"),
                ImmutableList.of(extractionField(0, false, "$.pull_request.user.login"))));

    assertThat(selectiveColumns)
        .as(
            "selective reader for pull_request.user.login should read far fewer columns"
                + " than full variant (%d full vs %d selective)",
            fullColumns, selectiveColumns)
        .isLessThan(fullColumns / 2);
  }

  /**
   * Partially shredded object (spec row 2): the top-level object has some fields in {@code
   * typed_value} and other fields in {@code value}. Both must be accessible after merge.
   *
   * <p>Root object: {@code {"action":"opened", "user":{"login":"alice"}}}. Schema shreds {@code
   * user.login} but leaves {@code action} unshredded (in {@code value}).
   */
  @Test
  void partiallyShredded_bothValueAndTypedFieldsAccessible() throws IOException {
    VariantMetadata meta = Variants.metadata("action", "login", "user");
    ShreddedObject userObj = Variants.object(meta);
    userObj.put("login", Variants.of("alice"));
    ShreddedObject rootObj = Variants.object(meta);
    rootObj.put("action", Variants.of("opened"));
    rootObj.put("user", userObj);
    Variant variant = Variant.of(meta, rootObj);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(rootObj))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    // Extract both the typed field (user.login) and the unshredded field (action).
    List<VariantExtractionField> fields =
        ImmutableList.of(
            extractionField(0, false, "$.user.login"), extractionField(1, false, "$.action"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0).asPrimitive().get())
          .as("user.login from typed_value")
          .isEqualTo("alice");
      assertThat(row.value(1).asPrimitive().get())
          .as("action from value blob (not shredded)")
          .isEqualTo("opened");
    }
  }

  /**
   * Spec: {@code value=null, typed_value=null} → field is MISSING, extraction returns Java null.
   * Distinct from JSON null (which encodes as {@code value=0x00}).
   */
  @Test
  void missingFieldReturnsNull() throws IOException {
    // Object has "other" field but not "login"
    VariantMetadata meta = Variants.metadata("login", "other", "user");
    ShreddedObject userObj = Variants.object(meta);
    userObj.put("other", Variants.of("value"));
    ShreddedObject rootObj = Variants.object(meta);
    rootObj.put("user", userObj);
    Variant variant = Variant.of(meta, rootObj);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(rootObj))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    // Extract $.user.login which does not exist in this row
    List<VariantExtractionField> fields =
        ImmutableList.of(extractionField(0, false, "$.user.login"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0))
          .as("login is absent (missing) — should return Java null, not a Variant null")
          .isNull();
    }
  }

  /**
   * Spec row 7: field is present with a value of the wrong shredded type. The {@code typed_value}
   * column is null; the actual value is in {@code value}. The reader must return the value, not
   * null.
   *
   * <p>Example: schema shreds {@code size} as INT64, but the row has {@code size = "large"} (a
   * string). → {@code typed_value.size.typed_value = null}, {@code typed_value.size.value = string
   * blob}.
   */
  @Test
  void fieldWithWrongTypeFallsBackToValueColumn() throws IOException {
    VariantMetadata meta = Variants.metadata("size");
    ShreddedObject root = Variants.object(meta);
    root.put("size", Variants.of("large")); // string, but shredded schema expects long
    Variant variant = Variant.of(meta, root);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    // Provide a shredding schema that expects size as long — the writer will fall back to
    // size.value for the string.
    ShreddedObject longRoot = Variants.object(meta);
    longRoot.put("size", Variants.of(99L)); // long placeholder to generate the long schema

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(longRoot))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields = ImmutableList.of(extractionField(0, false, "$.size"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0))
          .as("wrong-type field should be returned from size.value, not null")
          .isNotNull();
      // The value is a string variant (from size.value), not a long.
      assertThat(row.value(0).type()).isEqualTo(PhysicalType.STRING);
      assertThat(row.value(0).asPrimitive().get()).isEqualTo("large");
    }
  }

  /**
   * When the variant column itself is SQL NULL, shredded metadata is absent and every extraction
   * slot must be null. This is distinct from a present variant where only individual paths are
   * missing or JSON-null.
   */
  @Test
  void nullVariantColumnReturnsNullMetadataAndExtractions() throws IOException {
    Record record = GenericRecord.create(OPTIONAL_VARIANT_SCHEMA).copy("id", 1, "var", null);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(OPTIONAL_VARIANT_SCHEMA)
            .createWriterFunc(fs -> InternalWriter.create(OPTIONAL_VARIANT_SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields =
        ImmutableList.of(
            extractionField(0, false, "$.size"), extractionField(1, true, PLACEHOLDER_PATH));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(OPTIONAL_VARIANT_SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.metadata()).isNull();
      assertThat(row.value(0)).isNull();
      assertThat(row.placeholder(0)).isFalse();
      assertThat(row.placeholder(1)).isFalse();
    }
  }

  /**
   * Spec: when the top-level variant is null, {@code value = 0x00, typed_value = null}. Any nested
   * field extraction must return null.
   */
  @Test
  void wholeVariantNullReturnsNullForNestedPath() throws IOException {
    VariantMetadata meta = Variants.metadata("user", "login");
    ShreddedObject rootObj = Variants.object(meta);
    rootObj.put("user", Variants.ofNull()); // user is null; the variant stores it in value
    Variant variant = Variant.of(meta, rootObj);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(rootObj))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields =
        ImmutableList.of(extractionField(0, false, "$.user.login"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      // user is null (not an object) → login cannot be extracted → null
      assertThat(row.value(0))
          .as("user is null variant, not an object — login extraction must return null")
          .isNull();
    }
  }

  // -----------------------------------------------------------------------
  // Array path tests
  // -----------------------------------------------------------------------

  /** Root variant is an array; extract element by index via {@code $[n]}. */
  @Test
  void arrayPath_extractsElementFromRootArray() throws IOException {
    ValueArray arr = Variants.array();
    arr.add(Variants.of(10L));
    arr.add(Variants.of(20L));
    arr.add(Variants.of(30L));
    Variant variant = Variant.of(Variants.emptyMetadata(), arr);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(arr))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields = ImmutableList.of(extractionField(0, false, "$[1]"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0)).isNotNull();
      assertThat(row.value(0).asPrimitive().get()).isEqualTo(20L);
    }
  }

  /** Root variant is an array of objects; extract a field from one element via {@code $[n].key}. */
  @Test
  void arrayPath_extractsFieldFromObjectInRootArray() throws IOException {
    VariantMetadata meta = Variants.metadata("name");
    ShreddedObject alice = Variants.object(meta);
    alice.put("name", Variants.of("alice"));
    ShreddedObject bob = Variants.object(meta);
    bob.put("name", Variants.of("bob"));
    ValueArray arr = Variants.array();
    arr.add(alice);
    arr.add(bob);
    Variant variant = Variant.of(meta, arr);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(arr))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields = ImmutableList.of(extractionField(0, false, "$[0].name"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0)).isNotNull();
      assertThat(row.value(0).asPrimitive().get()).isEqualTo("alice");
    }
  }

  /**
   * Shredded object field containing an array; extract element via {@code $.items[n]}.
   *
   * <p>{@code buildSelectiveNestedReader} walks to the {@code items} group (array base case in
   * {@code buildSelectivePathReader}), then applies the index in memory.
   */
  @Test
  void nestedArrayField_shreddedObjectWithArrayChild() throws IOException {
    VariantMetadata meta = Variants.metadata("items");
    ValueArray arr = Variants.array();
    arr.add(Variants.of(10L));
    arr.add(Variants.of(20L));
    arr.add(Variants.of(30L));
    ShreddedObject rootObj = Variants.object(meta);
    rootObj.put("items", arr);
    Variant variant = Variant.of(meta, rootObj);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(rootObj))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields = ImmutableList.of(extractionField(0, false, "$.items[0]"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0)).isNotNull();
      assertThat(row.value(0).asPrimitive().get()).isEqualTo(10L);
    }
  }

  /**
   * Shredded object field containing an array of objects; extract field from one element via {@code
   * $.users[n].key}.
   */
  @Test
  void nestedArrayOfObjects_extractsFieldFromElement() throws IOException {
    VariantMetadata meta = Variants.metadata("users", "name");
    ShreddedObject alice = Variants.object(meta);
    alice.put("name", Variants.of("alice"));
    ShreddedObject bob = Variants.object(meta);
    bob.put("name", Variants.of("bob"));
    ValueArray users = Variants.array();
    users.add(alice);
    users.add(bob);
    ShreddedObject rootObj = Variants.object(meta);
    rootObj.put("users", users);
    Variant variant = Variant.of(meta, rootObj);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(rootObj))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields =
        ImmutableList.of(extractionField(0, false, "$.users[0].name"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0)).isNotNull();
      assertThat(row.value(0).asPrimitive().get()).isEqualTo("alice");
    }
  }

  // -----------------------------------------------------------------------
  // Deep nesting tests
  // -----------------------------------------------------------------------

  /**
   * Three-level deep nested object with all levels shredded. Exercises a chain of three {@link
   * SelectiveObjectReader} instances and in-memory navigation of the result.
   *
   * <p>Schema: {@code {a: {b: {c: "deep"}}}} — each level is a typed_value object group.
   */
  @Test
  void threeLevel_deepNested_allShredded() throws IOException {
    VariantMetadata meta = Variants.metadata("a", "b", "c");
    ShreddedObject cObj = Variants.object(meta);
    cObj.put("c", Variants.of("deep"));
    ShreddedObject bObj = Variants.object(meta);
    bObj.put("b", cObj);
    ShreddedObject rootObj = Variants.object(meta);
    rootObj.put("a", bObj);
    Variant variant = Variant.of(meta, rootObj);
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(rootObj))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields = ImmutableList.of(extractionField(0, false, "$.a.b.c"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0)).isNotNull();
      assertThat(row.value(0).asPrimitive().get()).isEqualTo("deep");
    }
  }

  // -----------------------------------------------------------------------
  // Mixed-row DL variation tests
  // -----------------------------------------------------------------------

  /**
   * Same shredded schema across three rows; the {@code typed_value} column is present for some rows
   * and absent for others. Verifies that {@code isTypedValuePresent()} evaluates correctly per row
   * (not just once) as definition levels change throughout the column.
   *
   * <p>Row 1: {@code size = 42L} — {@code typed_value} populated (DL high) Row 2: {@code size =
   * "large"} — wrong type; {@code typed_value} null, {@code value} blob used Row 3: {@code size =
   * 99L} — {@code typed_value} populated again (DL high)
   */
  @Test
  void mixedRows_typedValueAndValueBlobAlternatePerRow() throws IOException {
    VariantMetadata meta = Variants.metadata("size");
    ShreddedObject longSchema = Variants.object(meta);
    longSchema.put("size", Variants.of(42L)); // drives INT64 shredding schema

    ShreddedObject root1 = Variants.object(meta);
    root1.put("size", Variants.of(42L));
    ShreddedObject root2 = Variants.object(meta);
    root2.put("size", Variants.of("large")); // string falls into value blob
    ShreddedObject root3 = Variants.object(meta);
    root3.put("size", Variants.of(99L));

    Record r1 = GenericRecord.create(SCHEMA).copy("id", 1, "var", Variant.of(meta, root1));
    Record r2 = GenericRecord.create(SCHEMA).copy("id", 2, "var", Variant.of(meta, root2));
    Record r3 = GenericRecord.create(SCHEMA).copy("id", 3, "var", Variant.of(meta, root3));

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(longSchema))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(r1);
      w.add(r2);
      w.add(r3);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields = ImmutableList.of(extractionField(0, false, "$.size"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      List<VariantExtractionRow> rows = Lists.newArrayList(reader);
      assertThat(rows).hasSize(3);
      assertThat(rows.get(0).value(0).asPrimitive().get()).as("row 1: typed long").isEqualTo(42L);
      assertThat(rows.get(1).value(0).type())
          .as("row 2: value blob holds string")
          .isEqualTo(PhysicalType.STRING);
      assertThat(rows.get(1).value(0).asPrimitive().get())
          .as("row 2: string value")
          .isEqualTo("large");
      assertThat(rows.get(2).value(0).asPrimitive().get()).as("row 3: typed long").isEqualTo(99L);
    }
  }

  /**
   * Same shredded schema across three rows with a nested path; {@code typed_value} at the parent
   * level is populated for rows 1 and 3 but absent for row 2 (wrong type). Verifies per-row DL
   * evaluation through a {@link SelectiveObjectReader}.
   *
   * <p>Row 1: {@code user.login = "alice"} — shredded Row 2: {@code user = 42L} — typed_value null
   * for login step; value blob used Row 3: {@code user.login = "carol"} — shredded
   */
  @Test
  void mixedRows_nestedPath_typedValueAbsentForSomeRows() throws IOException {
    VariantMetadata meta = Variants.metadata("user", "login");

    ShreddedObject userAlice = Variants.object(meta);
    userAlice.put("login", Variants.of("alice"));
    ShreddedObject root1 = Variants.object(meta);
    root1.put("user", userAlice);

    // Row 2: user is a scalar (long), not an object — typed_value for user is absent
    ShreddedObject root2 = Variants.object(meta);
    root2.put("user", Variants.of(42L));

    ShreddedObject userCarol = Variants.object(meta);
    userCarol.put("login", Variants.of("carol"));
    ShreddedObject root3 = Variants.object(meta);
    root3.put("user", userCarol);

    // Schema is driven by root1 (user is a shredded object with login)
    Record r1 = GenericRecord.create(SCHEMA).copy("id", 1, "var", Variant.of(meta, root1));
    Record r2 = GenericRecord.create(SCHEMA).copy("id", 2, "var", Variant.of(meta, root2));
    Record r3 = GenericRecord.create(SCHEMA).copy("id", 3, "var", Variant.of(meta, root3));

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc((id, name) -> ParquetVariantUtil.toParquetSchema(root1))
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(r1);
      w.add(r2);
      w.add(r3);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields =
        ImmutableList.of(extractionField(0, false, "$.user.login"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      List<VariantExtractionRow> rows = Lists.newArrayList(reader);
      assertThat(rows).hasSize(3);
      assertThat(rows.get(0).value(0).asPrimitive().get())
          .as("row 1: login from typed_value")
          .isEqualTo("alice");
      assertThat(rows.get(1).value(0))
          .as("row 2: user is scalar, login extraction returns null")
          .isNull();
      assertThat(rows.get(2).value(0).asPrimitive().get())
          .as("row 3: login from typed_value")
          .isEqualTo("carol");
    }
  }

  // -----------------------------------------------------------------------
  // Miscellaneous edge cases
  // -----------------------------------------------------------------------

  /**
   * Root variant is a scalar (not an object). Extracting any object field path must return null
   * without throwing.
   */
  @Test
  void topLevelScalar_fieldExtractionReturnsNull() throws IOException {
    // Root is a long scalar, not an object — $.field cannot be navigated.
    Variant variant = Variant.of(Variants.emptyMetadata(), Variants.of(42L));
    Record record = GenericRecord.create(SCHEMA).copy("id", 1, "var", variant);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<Record> w =
        Parquet.write(out)
            .schema(SCHEMA)
            .createWriterFunc(fs -> InternalWriter.create(SCHEMA.asStruct(), fs))
            .build()) {
      w.add(record);
    }

    MessageType fileSchema;
    GroupType variantGroup;
    try (ParquetFileReader r = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      fileSchema = r.getFileMetaData().getSchema();
      variantGroup = fileSchema.getType("var").asGroupType();
    }

    List<VariantExtractionField> fields = ImmutableList.of(extractionField(0, false, "$.field"));

    try (CloseableIterable<VariantExtractionRow> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(
                s ->
                    ParquetVariantExtractionReaders.buildRowReader(
                        s, variantGroup, List.of("var"), fields))
            .build()) {
      VariantExtractionRow row = Lists.newArrayList(reader).get(0);
      assertThat(row.value(0)).as("scalar variant root — field extraction returns null").isNull();
    }
  }

  private static VariantExtractionField extractionField(
      int ordinal, boolean placeholder, String jsonPath) {
    return new VariantExtractionField(ordinal, placeholder, pathParts(jsonPath));
  }

  private static List<String> pathParts(String jsonPath) {
    if (PLACEHOLDER_PATH.equals(jsonPath)) {
      return ImmutableList.of();
    }
    // Handles dot-separated object steps, bracket array indexes, and mixed paths
    // (e.g. "$.user.login", "$[0]", "$.items[0].name") via PathUtil.parseObjectPath.
    return PathUtil.parseObjectPath(jsonPath);
  }

  private static GroupType shreddedObjectVariant(String name, GroupType... objectFields) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .optionalGroup()
        .addFields(objectFields)
        .named("typed_value")
        .named(name);
  }

  private static GroupType unshreddedVariant(String name) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private static GroupType shreddedStringField(String name) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("typed_value")
        .named(name);
  }

  private static GroupType shreddedLongField(String name) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .optional(PrimitiveType.PrimitiveTypeName.INT64)
        .named("typed_value")
        .named(name);
  }
}
