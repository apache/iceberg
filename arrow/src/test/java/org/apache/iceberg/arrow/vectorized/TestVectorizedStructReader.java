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
package org.apache.iceberg.arrow.vectorized;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.GeometryType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestVectorizedStructReader {

  private static final int NUM_ROWS = 250;
  private static final int BATCH_SIZE = 100;

  @TempDir private File tempDir;

  private static final Schema STRUCT_SCHEMA =
      new Schema(
          NestedField.optional(
              1, "s", StructType.of(NestedField.optional(2, "id", LongType.get()))));

  @Test
  void structRequiresBuilderOptIn() {
    MessageType fileSchema = ParquetSchemaUtil.convert(STRUCT_SCHEMA, "table");
    VectorizedReaderBuilder defaultBuilder =
        new VectorizedReaderBuilder(
            STRUCT_SCHEMA, fileSchema, false, ImmutableMap.of(), readers -> readers.get(0));

    assertThatThrownBy(
            () -> TypeWithSchemaVisitor.visit(STRUCT_SCHEMA.asStruct(), fileSchema, defaultBuilder))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Vectorized reads are not supported yet for struct fields");
  }

  @Test
  void structBuildsStructReaderWhenSupported() {
    MessageType fileSchema = ParquetSchemaUtil.convert(STRUCT_SCHEMA, "table");
    VectorizedReader<?> reader =
        TypeWithSchemaVisitor.visit(
            STRUCT_SCHEMA.asStruct(),
            fileSchema,
            new StructReaderBuilder(STRUCT_SCHEMA, fileSchema));

    try {
      assertThat(reader).isInstanceOf(VectorizedArrowReader.VectorizedStructReader.class);
    } finally {
      reader.close();
    }
  }

  @Test
  void structAssemblesChildHoldersInOrder() {
    NestedField lat = NestedField.required(2, "lat", IntegerType.get());
    NestedField lng = NestedField.optional(3, "lng", IntegerType.get());
    NestedField structField = NestedField.required(1, "s", StructType.of(lat, lng));

    VectorizedArrowReader latReader = new VectorizedArrowReader.ConstantVectorReader<>(lat, 7);
    VectorizedArrowReader lngReader = new VectorizedArrowReader.ConstantVectorReader<>(lng, 9);
    VectorizedArrowReader.VectorizedStructReader reader =
        new VectorizedArrowReader.VectorizedStructReader(
            structField, ImmutableList.<VectorizedReader<?>>of(latReader, lngReader), 0, null);

    reader.setBatchSize(128);
    VectorHolder.StructVectorHolder holder = (VectorHolder.StructVectorHolder) reader.read(null, 3);

    assertThat(holder.numValues()).isEqualTo(3);
    assertThat(holder.childHolders()).hasSize(2);
    assertThat(((VectorHolder.ConstantVectorHolder<?>) holder.childHolders().get(0)).getConstant())
        .isEqualTo(7);
    assertThat(((VectorHolder.ConstantVectorHolder<?>) holder.childHolders().get(1)).getConstant())
        .isEqualTo(9);
  }

  @Test
  void nestedStructReusesDescendantPresenceReader() {
    NestedField added = NestedField.optional(4, "added", IntegerType.get());
    NestedField inner = NestedField.optional(2, "inner", StructType.of(added));
    NestedField outer = NestedField.optional(1, "outer", StructType.of(inner));
    ColumnDescriptor presenceColumn =
        new ColumnDescriptor(
            new String[] {"outer", "inner", "innerId"},
            Types.optional(PrimitiveTypeName.INT64).named("innerId"),
            0,
            2);
    VectorizedArrowReader presenceLeaf = new VectorizedArrowReader.PresenceReader(presenceColumn);
    VectorizedArrowReader constantChild =
        new VectorizedArrowReader.ConstantVectorReader<>(added, 7);
    VectorizedArrowReader.VectorizedStructReader innerReader =
        new VectorizedArrowReader.VectorizedStructReader(
            inner, ImmutableList.<VectorizedReader<?>>of(constantChild), 2, presenceLeaf);
    VectorizedArrowReader.VectorizedStructReader outerReader =
        new VectorizedArrowReader.VectorizedStructReader(
            outer, ImmutableList.<VectorizedReader<?>>of(innerReader), 1, null);

    // outer has no file-backed child, so it reuses inner's presence reader rather than build one
    assertThat(outerReader.fileBackedLeaf()).isSameAs(presenceLeaf);
  }

  @Test
  void booleanPresenceLeafOnV2Pages() throws IOException {
    assertStructNullsFromUnprojectedLeaf(
        BooleanType.get(), row -> row % 2 == 0, WriterVersion.PARQUET_2_0);
  }

  @Test
  void geometryPresenceLeaf() throws IOException {
    assertStructNullsFromUnprojectedLeaf(
        GeometryType.crs84(), TestVectorizedStructReader::point, WriterVersion.PARQUET_1_0);
  }

  private void assertStructNullsFromUnprojectedLeaf(
      Type leafType, IntFunction<Object> leafValue, WriterVersion writerVersion)
      throws IOException {
    StructType writeStruct = StructType.of(NestedField.optional(2, "leaf", leafType));
    Schema writeSchema = new Schema(NestedField.optional(1, "s", writeStruct));
    List<GenericRecord> records = Lists.newArrayList();
    for (int row = 0; row < NUM_ROWS; row++) {
      GenericRecord record = GenericRecord.create(writeSchema);
      if (row % 3 != 0) {
        GenericRecord struct = GenericRecord.create(writeStruct);
        struct.setField("leaf", leafValue.apply(row));
        record.setField("s", struct);
      }

      records.add(record);
    }

    File dataFile = new File(tempDir, "presence.parquet");
    try (FileAppender<GenericRecord> writer =
        Parquet.write(Files.localOutput(dataFile))
            .schema(writeSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .writerVersion(writerVersion)
            .set(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
            .build()) {
      writer.addAll(records);
    }

    // project only a field the file does not have, so presence comes from the unprojected leaf
    Schema readSchema =
        new Schema(
            NestedField.optional(
                1, "s", StructType.of(NestedField.optional(3, "added", IntegerType.get()))));
    int rowsRead = 0;
    try (CloseableIterable<VectorHolder> batches =
        Parquet.read(Files.localInput(dataFile))
            .project(readSchema)
            .recordsPerBatch(BATCH_SIZE)
            .createBatchedReaderFunc(
                fileSchema ->
                    TypeWithSchemaVisitor.visit(
                        readSchema.asStruct(),
                        fileSchema,
                        new StructReaderBuilder(readSchema, fileSchema)))
            .build()) {
      for (VectorHolder batch : batches) {
        for (int idx = 0; idx < batch.numValues(); idx++) {
          int row = rowsRead + idx;
          assertThat(batch.nullabilityHolder().isNullAt(idx))
              .as("struct null flag at row %s", row)
              .isEqualTo(row % 3 == 0 ? (byte) 1 : (byte) 0);
        }

        rowsRead += batch.numValues();
      }
    }

    assertThat(rowsRead).isEqualTo(NUM_ROWS);
  }

  private static ByteBuffer point(int row) {
    ByteBuffer wkb = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
    wkb.put((byte) 1).putInt(1).putDouble(row).putDouble(row * 2.0);
    wkb.flip();
    return wkb;
  }

  private static class StructReaderBuilder extends VectorizedReaderBuilder {
    StructReaderBuilder(Schema schema, MessageType fileSchema) {
      super(schema, fileSchema, false, ImmutableMap.of(), readers -> readers.get(0));
    }

    @Override
    protected boolean supportsStructs() {
      return true;
    }
  }
}
