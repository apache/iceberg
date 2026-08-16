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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.InternalTestHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
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
import org.apache.iceberg.variants.VariantMetadata;
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

  private Variant variant;
  private List<Record> records;

  @TempDir private Path temp;

  @BeforeEach
  public void before() {
    ByteBuffer metadataBuffer = VariantTestUtil.createMetadata(ImmutableList.of("a", "b"), true);
    VariantMetadata metadata = Variants.metadata(metadataBuffer);
    ByteBuffer objectBuffer =
        VariantTestUtil.createObject(
            metadataBuffer,
            ImmutableMap.of(
                "a", Variants.of(42),
                "b", Variants.of("hello")));
    variant = Variant.of(metadata, Variants.value(metadata, objectBuffer));

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

    assertThat(shreddedTypes).containsOnlyKeys(2);
    GroupType typedValue = shreddedTypes.get(2).asGroupType();
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

    assertThat(shreddedTypes).containsOnlyKeys(1);
    assertThat(shreddedTypes.get(1).asGroupType().containsField("a")).isTrue();
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

    assertThat(shreddedTypes).containsOnlyKeys(2);
    assertThat(shreddedTypes.get(2).asGroupType().containsField("a")).isTrue();
    assertThat(shreddedTypes.get(2).asGroupType().containsField("b")).isTrue();
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
  public void testAnalyzeVariantColumnsRejectsNullEngineSchema() {
    RecordVariantShreddingAnalyzer analyzer = new RecordVariantShreddingAnalyzer();

    assertThatThrownBy(() -> analyzer.analyzeVariantColumns(records, VARIANT_AFTER_ID_SCHEMA, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Invalid engine schema: null");
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
                      TableProperties.PARQUET_SHRED_VARIANTS, "true",
                      TableProperties.PARQUET_VARIANT_BUFFER_SIZE, "2"))
              .build();

      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
      EncryptedOutputFile encryptedOutputFile = fileFactory.newOutputFile();

      // KC path: RegistryBasedFileWriterFactory passes inputSchema=null as engineSchema.
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
      assertRecordsRoundTrip(outputFile);
    } finally {
      TestTables.clearTables();
    }
  }

  @Test
  public void testFormatModelRegistryShreddingRoundTrip() throws IOException {
    OutputFile outputFile = Files.localOutput(temp.resolve("variant-shredded.parquet").toFile());
    EncryptedOutputFile encryptedOutputFile = EncryptedFiles.plainAsEncryptedOutput(outputFile);

    FileWriterBuilder<DataWriter<Record>, Object> writeBuilder =
        FormatModelRegistry.dataWriteBuilder(FileFormat.PARQUET, Record.class, encryptedOutputFile);

    try (DataWriter<Record> writer =
        writeBuilder
            .schema(VARIANT_AFTER_ID_SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .setAll(
                ImmutableMap.of(
                    TableProperties.PARQUET_SHRED_VARIANTS, "true",
                    TableProperties.PARQUET_VARIANT_BUFFER_SIZE, "2"))
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
    assertRecordsRoundTrip(outputFile);
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
    try (ParquetReader<Group> rawReader =
        ParquetReader.builder(
                new GroupReadSupport(), new org.apache.hadoop.fs.Path(outputFile.location()))
            .build()) {
      Group row;
      int rowCount = 0;
      while ((row = rawReader.read()) != null) {
        assertShreddedTypedValueOnRow(row);
        rowCount++;
      }
      assertThat(rowCount).isEqualTo(records.size());
    }
  }

  private void assertRecordsRoundTrip(OutputFile outputFile) throws IOException {
    List<Record> writtenRecords;
    try (CloseableIterable<Record> reader =
        Parquet.read(outputFile.toInputFile())
            .project(VARIANT_AFTER_ID_SCHEMA)
            .createReaderFunc(
                fileSchema ->
                    org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                        VARIANT_AFTER_ID_SCHEMA, fileSchema))
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    assertThat(writtenRecords).hasSameSizeAs(records);
    for (int i = 0; i < records.size(); i++) {
      InternalTestHelpers.assertEquals(
          VARIANT_AFTER_ID_SCHEMA.asStruct(), records.get(i), writtenRecords.get(i));
    }
  }
}
