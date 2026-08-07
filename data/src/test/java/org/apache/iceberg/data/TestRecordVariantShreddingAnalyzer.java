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
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.InternalTestHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.formats.FileWriterBuilder;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetFileTestUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRecordVariantShreddingAnalyzer {

  private static final Schema VARIANT_AFTER_ID_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "v", Types.VariantType.get()));

  private static final Schema VARIANT_BEFORE_ID_SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "v", Types.VariantType.get()),
          Types.NestedField.required(2, "id", Types.LongType.get()));

  private static final Schema MULTI_VARIANT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "v1", Types.VariantType.get()),
          Types.NestedField.optional(3, "other", Types.StringType.get()),
          Types.NestedField.optional(4, "v2", Types.VariantType.get()));

  // Engine schema whose variant column is named "w" instead of "v", so the "v" column resolved
  // from the Iceberg schema is not found and shredding does not activate.
  private static final Schema MISMATCHED_ENGINE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "w", Types.VariantType.get()));

  private static final int VARIANT_FIELD_ID = 2;
  private static final int VARIANT_FIRST_FIELD_ID = 1;
  private static final int FIRST_MULTI_VARIANT_FIELD_ID = 2;
  private static final int SECOND_MULTI_VARIANT_FIELD_ID = 4;
  private static final String BUFFER_SIZE = "2";

  private Variant variant;
  private List<Record> records;

  @TempDir private java.nio.file.Path temp;

  @BeforeEach
  public void before() {
    variant =
        VariantTestUtil.variant(
            ImmutableMap.of(
                "a", Variants.of(42),
                "b", Variants.of("hello")));

    GenericRecord record = GenericRecord.create(VARIANT_AFTER_ID_SCHEMA);
    records =
        ImmutableList.of(
            record.copy(ImmutableMap.of("id", 1L, "v", variant)),
            record.copy(ImmutableMap.of("id", 2L, "v", variant)),
            record.copy(ImmutableMap.of("id", 3L, "v", variant)));
  }

  @Test
  public void testAnalyzeVariantColumnsUsesIcebergColumnOrder() {
    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();

    Map<Integer, Type> shreddedTypes =
        analyzer.analyzeVariantColumns(records, VARIANT_AFTER_ID_SCHEMA, VARIANT_AFTER_ID_SCHEMA);

    assertThat(shreddedTypes).containsOnlyKeys(VARIANT_FIELD_ID);
    GroupType typedValue = shreddedTypes.get(VARIANT_FIELD_ID).asGroupType();
    assertThat(typedValue.getName()).isEqualTo("typed_value");
    assertThat(typedValue.containsField("a")).isTrue();
    assertThat(typedValue.containsField("b")).isTrue();
  }

  @Test
  public void testAnalyzeVariantColumnsWhenVariantIsFirstColumn() {
    GenericRecord record = GenericRecord.create(VARIANT_BEFORE_ID_SCHEMA);
    List<Record> variantFirstRecords =
        ImmutableList.of(
            record.copy(ImmutableMap.of("v", variant, "id", 1L)),
            record.copy(ImmutableMap.of("v", variant, "id", 2L)));

    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();
    Map<Integer, Type> shreddedTypes =
        analyzer.analyzeVariantColumns(
            variantFirstRecords, VARIANT_BEFORE_ID_SCHEMA, VARIANT_BEFORE_ID_SCHEMA);

    assertThat(shreddedTypes).containsOnlyKeys(VARIANT_FIRST_FIELD_ID);
    assertThat(shreddedTypes.get(VARIANT_FIRST_FIELD_ID).asGroupType().containsField("a")).isTrue();
  }

  @Test
  public void testAnalyzeVariantColumnsWithMultipleNonAdjacentVariants() {
    GenericRecord record = GenericRecord.create(MULTI_VARIANT_SCHEMA);
    List<Record> multiVariantRecords =
        ImmutableList.of(
            record.copy(ImmutableMap.of("id", 1L, "v1", variant, "other", "x", "v2", variant)),
            record.copy(ImmutableMap.of("id", 2L, "v1", variant, "other", "y", "v2", variant)));

    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();
    Map<Integer, Type> shreddedTypes =
        analyzer.analyzeVariantColumns(
            multiVariantRecords, MULTI_VARIANT_SCHEMA, MULTI_VARIANT_SCHEMA);

    assertThat(shreddedTypes)
        .containsOnlyKeys(FIRST_MULTI_VARIANT_FIELD_ID, SECOND_MULTI_VARIANT_FIELD_ID);
    assertThat(shreddedTypes.get(FIRST_MULTI_VARIANT_FIELD_ID).asGroupType().containsField("a"))
        .isTrue();
    assertThat(shreddedTypes.get(SECOND_MULTI_VARIANT_FIELD_ID).asGroupType().containsField("b"))
        .isTrue();
  }

  @Test
  public void testAnalyzeVariantColumnsSkipsNullVariantValues() {
    GenericRecord withVariant = GenericRecord.create(VARIANT_AFTER_ID_SCHEMA);
    withVariant.setField("id", 1L);
    withVariant.setField("v", variant);

    GenericRecord withNullVariant = GenericRecord.create(VARIANT_AFTER_ID_SCHEMA);
    withNullVariant.setField("id", 2L);
    withNullVariant.setField("v", null);

    GenericRecord withVariant2 = GenericRecord.create(VARIANT_AFTER_ID_SCHEMA);
    withVariant2.setField("id", 3L);
    withVariant2.setField("v", variant);

    List<Record> recordsWithNulls = ImmutableList.of(withVariant, withNullVariant, withVariant2);

    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();
    Map<Integer, Type> shreddedTypes =
        analyzer.analyzeVariantColumns(
            recordsWithNulls, VARIANT_AFTER_ID_SCHEMA, VARIANT_AFTER_ID_SCHEMA);

    assertThat(shreddedTypes).containsOnlyKeys(VARIANT_FIELD_ID);
    assertThat(shreddedTypes.get(VARIANT_FIELD_ID).asGroupType().containsField("a")).isTrue();
    assertThat(shreddedTypes.get(VARIANT_FIELD_ID).asGroupType().containsField("b")).isTrue();
  }

  @Test
  public void testAnalyzeVariantColumnsWithAllNullVariantValues() {
    GenericRecord nullVariant1 = GenericRecord.create(VARIANT_AFTER_ID_SCHEMA);
    nullVariant1.setField("id", 1L);
    nullVariant1.setField("v", null);

    GenericRecord nullVariant2 = GenericRecord.create(VARIANT_AFTER_ID_SCHEMA);
    nullVariant2.setField("id", 2L);
    nullVariant2.setField("v", null);

    List<Record> allNullVariants = ImmutableList.of(nullVariant1, nullVariant2);

    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();
    Map<Integer, Type> shreddedTypes =
        analyzer.analyzeVariantColumns(
            allNullVariants, VARIANT_AFTER_ID_SCHEMA, VARIANT_AFTER_ID_SCHEMA);

    assertThat(shreddedTypes).isEmpty();
  }

  @Test
  public void testAnalyzeVariantColumnsRejectsNonVariantValues() {
    GenericRecord invalidRecord = GenericRecord.create(VARIANT_AFTER_ID_SCHEMA);
    invalidRecord.setField("id", 1L);
    invalidRecord.setField("v", "not-a-variant");

    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();

    assertThatThrownBy(
            () ->
                analyzer.analyzeVariantColumns(
                    ImmutableList.of(invalidRecord),
                    VARIANT_AFTER_ID_SCHEMA,
                    VARIANT_AFTER_ID_SCHEMA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected Variant at index 1 but was: java.lang.String");
  }

  @Test
  public void testAnalyzeVariantColumnsFallsBackToIcebergSchemaWhenEngineSchemaNull() {
    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();

    Map<Integer, Type> shreddedTypes =
        analyzer.analyzeVariantColumns(records, VARIANT_AFTER_ID_SCHEMA, null);

    assertThat(shreddedTypes).containsOnlyKeys(VARIANT_FIELD_ID);
    assertThat(shreddedTypes.get(VARIANT_FIELD_ID).asGroupType().containsField("a")).isTrue();
    assertThat(shreddedTypes.get(VARIANT_FIELD_ID).asGroupType().containsField("b")).isTrue();
  }

  @Test
  public void testResolveColumnIndexRejectsNullEngineSchema() {
    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();

    assertThatThrownBy(() -> analyzer.resolveColumnIndex(null, "v"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid engine schema: null");
  }

  @Test
  public void testAnalyzeVariantColumnsSkipsColumnMissingFromEngineSchema() {
    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();

    Map<Integer, Type> shreddedTypes =
        analyzer.analyzeVariantColumns(records, VARIANT_AFTER_ID_SCHEMA, MISMATCHED_ENGINE_SCHEMA);

    assertThat(shreddedTypes).isEmpty();
  }

  @Test
  public void testAnalyzeVariantColumnsRejectsReorderedEngineSchema() {
    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();

    assertThatThrownBy(
            () ->
                analyzer.analyzeVariantColumns(
                    records, VARIANT_AFTER_ID_SCHEMA, VARIANT_BEFORE_ID_SCHEMA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Variant column v position mismatch between Iceberg and engine schemas: 1 vs 0");
  }

  @Test
  public void testAnalyzeVariantColumnsRejectsTwoSwappedVariantColumns() {
    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();
    Schema swappedEngineSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(4, "v2", Types.VariantType.get()),
            Types.NestedField.optional(3, "other", Types.StringType.get()),
            Types.NestedField.optional(2, "v1", Types.VariantType.get()));

    assertThatThrownBy(
            () ->
                analyzer.analyzeVariantColumns(
                    ImmutableList.<Record>of(), MULTI_VARIANT_SCHEMA, swappedEngineSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Variant column v1 position mismatch between Iceberg and engine schemas: 1 vs 3");
  }

  @Test
  public void testResolveColumnIndexResolvesPositionsAndReportsMisses() {
    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();

    assertThat(analyzer.resolveColumnIndex(VARIANT_AFTER_ID_SCHEMA, "id")).isEqualTo(0);
    assertThat(analyzer.resolveColumnIndex(VARIANT_AFTER_ID_SCHEMA, "v")).isEqualTo(1);
    assertThat(analyzer.resolveColumnIndex(VARIANT_BEFORE_ID_SCHEMA, "v")).isEqualTo(0);
    assertThat(analyzer.resolveColumnIndex(VARIANT_AFTER_ID_SCHEMA, "missing")).isEqualTo(-1);
  }

  @Test
  public void testGenericFileWriterFactoryShreddingRoundTrip() throws IOException {
    Table table =
        TestTables.create(
            temp.resolve("table").toFile(),
            "variant",
            VARIANT_AFTER_ID_SCHEMA,
            PartitionSpec.unpartitioned(),
            3);
    try {
      GenericFileWriterFactory writerFactory =
          new GenericFileWriterFactory.Builder(table)
              .dataFileFormat(FileFormat.PARQUET)
              .dataSchema(VARIANT_AFTER_ID_SCHEMA)
              .writerProperties(
                  ImmutableMap.of(
                      TableProperties.PARQUET_SHRED_VARIANTS,
                      "true",
                      TableProperties.PARQUET_VARIANT_BUFFER_SIZE,
                      BUFFER_SIZE))
              .build();

      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
      EncryptedOutputFile encryptedOutputFile = fileFactory.newOutputFile();

      // GenericFileWriterFactory does not forward an inputSchema, so engineSchema is null and the
      // analyzer falls back to the Iceberg schema to resolve variant columns.
      try (DataWriter<Record> writer =
          writerFactory.newDataWriter(encryptedOutputFile, table.spec(), null)) {
        for (Record rec : records) {
          writer.write(rec);
        }
      }

      OutputFile outputFile = encryptedOutputFile.encryptingOutputFile();
      try (ParquetFileReader reader =
          ParquetFileReader.open(ParquetFileTestUtils.file(outputFile.toInputFile()))) {
        assertShreddedVariantParquetSchema(reader.getFooter().getFileMetaData().getSchema());
      }

      assertAllRawParquetRowsShredded(outputFile);
      assertRecordsRoundTrip(outputFile, records);
    } finally {
      TestTables.clearTables();
    }
  }

  @Test
  public void testFormatModelRegistryShreddingRoundTrip() throws IOException {
    OutputFile outputFile = Files.localOutput(temp.resolve("variant-shredded.parquet").toFile());
    EncryptedOutputFile encryptedOutputFile = EncryptedFiles.plainAsEncryptedOutput(outputFile);

    FileWriterBuilder<DataWriter<Record>, Schema> writeBuilder =
        FormatModelRegistry.dataWriteBuilder(FileFormat.PARQUET, Record.class, encryptedOutputFile);

    try (DataWriter<Record> writer =
        writeBuilder
            .schema(VARIANT_AFTER_ID_SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .setAll(
                ImmutableMap.of(
                    TableProperties.PARQUET_SHRED_VARIANTS,
                    "true",
                    TableProperties.PARQUET_VARIANT_BUFFER_SIZE,
                    BUFFER_SIZE))
            .build()) {
      for (Record rec : records) {
        writer.write(rec);
      }
    }

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetFileTestUtils.file(outputFile.toInputFile()))) {
      assertShreddedVariantParquetSchema(reader.getFooter().getFileMetaData().getSchema());
    }

    assertAllRawParquetRowsShredded(outputFile);
    assertRecordsRoundTrip(outputFile, records);
  }

  @Test
  public void testExplicitNullEngineSchemaFallsBackAndShreds() throws IOException {
    OutputFile outputFile =
        Files.localOutput(temp.resolve("explicit-null-engine.parquet").toFile());
    EncryptedOutputFile encryptedOutputFile = EncryptedFiles.plainAsEncryptedOutput(outputFile);

    FileWriterBuilder<DataWriter<Record>, Schema> writeBuilder =
        FormatModelRegistry.dataWriteBuilder(FileFormat.PARQUET, Record.class, encryptedOutputFile);

    // Passing null explicitly behaves like not calling engineSchema at all: the analyzer falls
    // back.
    try (DataWriter<Record> writer =
        writeBuilder
            .engineSchema(null)
            .schema(VARIANT_AFTER_ID_SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .setAll(
                ImmutableMap.of(
                    TableProperties.PARQUET_SHRED_VARIANTS,
                    "true",
                    TableProperties.PARQUET_VARIANT_BUFFER_SIZE,
                    BUFFER_SIZE))
            .build()) {
      for (Record rec : records) {
        writer.write(rec);
      }
    }

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetFileTestUtils.file(outputFile.toInputFile()))) {
      assertShreddedVariantParquetSchema(reader.getFooter().getFileMetaData().getSchema());
    }

    assertRecordsRoundTrip(outputFile, records);
  }

  @Test
  public void testPostBufferRowWithUnsampledFieldPreservedInResidual() throws IOException {
    Variant variantAbc =
        VariantTestUtil.variant(
            ImmutableMap.of(
                "a", Variants.of(7),
                "b", Variants.of("x"),
                "c", Variants.of(99)));

    GenericRecord recordBuilder = GenericRecord.create(VARIANT_AFTER_ID_SCHEMA);
    // Buffer size is 2, so the layout is inferred from rows 1-2 (fields a, b). Row 3 adds field c,
    // which is not in the inferred typed_value and must survive in the residual value column.
    List<Record> mixedRecords =
        ImmutableList.of(
            recordBuilder.copy(ImmutableMap.of("id", 1L, "v", variant)),
            recordBuilder.copy(ImmutableMap.of("id", 2L, "v", variant)),
            recordBuilder.copy(ImmutableMap.of("id", 3L, "v", variantAbc)));

    OutputFile outputFile = Files.localOutput(temp.resolve("residual.parquet").toFile());
    EncryptedOutputFile encryptedOutputFile = EncryptedFiles.plainAsEncryptedOutput(outputFile);

    FileWriterBuilder<DataWriter<Record>, Schema> writeBuilder =
        FormatModelRegistry.dataWriteBuilder(FileFormat.PARQUET, Record.class, encryptedOutputFile);
    try (DataWriter<Record> writer =
        writeBuilder
            .schema(VARIANT_AFTER_ID_SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .setAll(
                ImmutableMap.of(
                    TableProperties.PARQUET_SHRED_VARIANTS,
                    "true",
                    TableProperties.PARQUET_VARIANT_BUFFER_SIZE,
                    BUFFER_SIZE))
            .build()) {
      for (Record rec : mixedRecords) {
        writer.write(rec);
      }
    }

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetFileTestUtils.file(outputFile.toInputFile()))) {
      MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
      assertShreddedVariantParquetSchema(parquetSchema);
      GroupType typedValue =
          parquetSchema.getType("v").asGroupType().getType("typed_value").asGroupType();
      assertThat(typedValue.containsField("c")).isFalse();
    }

    assertResidualValuePresentOnRow(outputFile, 2);
    assertRecordsRoundTrip(outputFile, mixedRecords);
  }

  @Test
  public void testExplicitEngineSchemaSurvivesSchemaCall() throws IOException {
    assertExplicitEngineSchemaSuppressesShredding(true);
  }

  @Test
  public void testExplicitEngineSchemaSurvivesReverseCallOrder() throws IOException {
    assertExplicitEngineSchemaSuppressesShredding(false);
  }

  private void assertExplicitEngineSchemaSuppressesShredding(boolean engineSchemaFirst)
      throws IOException {
    // Explicit engine schema names the variant "w", so "v" is not resolved and shredding is
    // skipped.
    assertThat(writeAndDetectShredding(MISMATCHED_ENGINE_SCHEMA, engineSchemaFirst)).isFalse();
    // Positive control: the same fixture with no explicit engine schema derives it and shreds "v",
    // so the suppression above is the explicit schema surviving, not shredding being disabled.
    assertThat(writeAndDetectShredding(null, engineSchemaFirst)).isTrue();
  }

  private boolean writeAndDetectShredding(Schema explicitEngineSchema, boolean engineSchemaFirst)
      throws IOException {
    OutputFile outputFile =
        Files.localOutput(
            temp.resolve(
                    "engine-"
                        + (explicitEngineSchema == null ? "derived" : "explicit")
                        + ".parquet")
                .toFile());
    EncryptedOutputFile encryptedOutputFile = EncryptedFiles.plainAsEncryptedOutput(outputFile);

    FileWriterBuilder<DataWriter<Record>, Schema> writeBuilder =
        FormatModelRegistry.dataWriteBuilder(FileFormat.PARQUET, Record.class, encryptedOutputFile);
    if (explicitEngineSchema == null) {
      writeBuilder.schema(VARIANT_AFTER_ID_SCHEMA);
    } else if (engineSchemaFirst) {
      writeBuilder.engineSchema(explicitEngineSchema).schema(VARIANT_AFTER_ID_SCHEMA);
    } else {
      writeBuilder.schema(VARIANT_AFTER_ID_SCHEMA).engineSchema(explicitEngineSchema);
    }

    try (DataWriter<Record> writer =
        writeBuilder
            .spec(PartitionSpec.unpartitioned())
            .setAll(
                ImmutableMap.of(
                    TableProperties.PARQUET_SHRED_VARIANTS,
                    "true",
                    TableProperties.PARQUET_VARIANT_BUFFER_SIZE,
                    BUFFER_SIZE))
            .build()) {
      for (Record rec : records) {
        writer.write(rec);
      }
    }

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetFileTestUtils.file(outputFile.toInputFile()))) {
      return reader
          .getFooter()
          .getFileMetaData()
          .getSchema()
          .getType("v")
          .asGroupType()
          .containsField("typed_value");
    }
  }

  private void assertShreddedVariantParquetSchema(MessageType parquetSchema) {
    GroupType variantGroup = parquetSchema.getType("v").asGroupType();
    assertThat(variantGroup.containsField("typed_value")).isTrue();

    GroupType typedValue = variantGroup.getType("typed_value").asGroupType();
    assertThat(typedValue.containsField("a")).isTrue();
    assertThat(typedValue.containsField("b")).isTrue();
  }

  private void assertShreddedTypedValueOnRow(Group row) {
    Group variantData = row.getGroup("v", 0);
    assertThat(variantData.getFieldRepetitionCount("value")).isEqualTo(0);

    Group typedValue = variantData.getGroup("typed_value", 0);
    assertThat(typedValue.getGroup("a", 0).getInteger("typed_value", 0)).isEqualTo(42);
    assertThat(typedValue.getGroup("b", 0).getString("typed_value", 0)).isEqualTo("hello");
  }

  private void assertAllRawParquetRowsShredded(OutputFile outputFile) throws IOException {
    int rowCount = 0;
    try (ParquetReader<Group> rawReader =
        ParquetReader.builder(new GroupReadSupport(), new Path(outputFile.location())).build()) {
      Group row = rawReader.read();
      while (row != null) {
        assertShreddedTypedValueOnRow(row);
        rowCount++;
        row = rawReader.read();
      }
    }

    assertThat(rowCount).isEqualTo(records.size());
  }

  private void assertResidualValuePresentOnRow(OutputFile outputFile, int residualRowIndex)
      throws IOException {
    int rowIndex = 0;
    try (ParquetReader<Group> rawReader =
        ParquetReader.builder(new GroupReadSupport(), new Path(outputFile.location())).build()) {
      for (Group row = rawReader.read(); row != null; row = rawReader.read()) {
        Group variantData = row.getGroup("v", 0);
        if (rowIndex == residualRowIndex) {
          assertThat(variantData.getFieldRepetitionCount("value")).isGreaterThan(0);
        } else {
          assertThat(variantData.getFieldRepetitionCount("value")).isEqualTo(0);
        }
        rowIndex++;
      }
    }
  }

  private void assertRecordsRoundTrip(OutputFile outputFile, List<Record> expected)
      throws IOException {
    try (CloseableIterable<Record> reader =
        Parquet.read(outputFile.toInputFile())
            .project(VARIANT_AFTER_ID_SCHEMA)
            .createReaderFunc(
                fileSchema ->
                    GenericParquetReaders.buildReader(VARIANT_AFTER_ID_SCHEMA, fileSchema))
            .build()) {
      List<Record> actual = Lists.newArrayList(reader);
      assertThat(actual).hasSameSizeAs(expected);
      for (int index = 0; index < expected.size(); index++) {
        InternalTestHelpers.assertEquals(
            VARIANT_AFTER_ID_SCHEMA.asStruct(), expected.get(index), actual.get(index));
      }
    }
  }
}
