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

import static org.apache.iceberg.Files.localInput;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Test cases for vectorized reads of struct (nested) fields via the Arrow reader. */
public class TestArrowReaderStruct {

  private static final int NUM_ROWS = 20;

  @TempDir private File tempDir;

  private HadoopTables tables;
  private String tableLocation;

  @BeforeEach
  public void before() {
    tables = new HadoopTables();
    tableLocation = tempDir.toURI().toString();
  }

  /** Required struct with required primitive children. */
  @Test
  public void testRequiredStructWithRequiredFields() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(3, "lat", Types.DoubleType.get()),
                    Types.NestedField.required(4, "lon", Types.DoubleType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);
      GenericRecord location = GenericRecord.create(schema.findType("location").asStructType());
      location.setField("lat", 37.0 + i * 0.1);
      location.setField("lon", -122.0 + i * 0.1);
      rec.setField("location", location);
      records.add(rec);
    }

    appendData(table, records);

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        assertThat(batch.numRows()).isEqualTo(NUM_ROWS);
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();

        StructVector structVec = (StructVector) root.getVector("location");
        assertThat(structVec).isNotNull();
        assertThat(structVec.size()).isEqualTo(2);
        assertThat(structVec.getValueCount()).isEqualTo(NUM_ROWS);

        Float8Vector latVec = (Float8Vector) structVec.getChild("lat");
        Float8Vector lonVec = (Float8Vector) structVec.getChild("lon");
        for (int i = 0; i < NUM_ROWS; i++) {
          assertThat(latVec.get(i)).isEqualTo(37.0 + i * 0.1);
          assertThat(lonVec.get(i)).isEqualTo(-122.0 + i * 0.1);
        }

        root.close();
      }
    }
  }

  /** Optional struct with a mix of required and optional children. */
  @Test
  public void testOptionalStructWithMixedFields() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(
                2,
                "info",
                Types.StructType.of(
                    Types.NestedField.required(3, "name", Types.StringType.get()),
                    Types.NestedField.optional(4, "count", Types.IntegerType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);
      GenericRecord info = GenericRecord.create(schema.findType("info").asStructType());
      info.setField("name", "item-" + i);
      info.setField("count", i % 3 == 0 ? null : i * 10);
      rec.setField("info", info);
      records.add(rec);
    }

    appendData(table, records);

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();
        StructVector infoVec = (StructVector) root.getVector("info");
        VarCharVector nameVec = (VarCharVector) infoVec.getChild("name");
        IntVector countVec = (IntVector) infoVec.getChild("count");

        for (int i = 0; i < NUM_ROWS; i++) {
          assertThat(new String(nameVec.get(i), StandardCharsets.UTF_8)).isEqualTo("item-" + i);
          if (i % 3 == 0) {
            assertThat(countVec.isNull(i)).isTrue();
          } else {
            assertThat(countVec.get(i)).isEqualTo(i * 10);
          }
        }

        root.close();
      }
    }
  }

  /** Struct with many different primitive child types. */
  @Test
  public void testStructWithVariousChildTypes() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "data",
                Types.StructType.of(
                    Types.NestedField.required(3, "int_val", Types.IntegerType.get()),
                    Types.NestedField.required(4, "long_val", Types.LongType.get()),
                    Types.NestedField.required(5, "double_val", Types.DoubleType.get()),
                    Types.NestedField.required(6, "string_val", Types.StringType.get()),
                    Types.NestedField.required(7, "date_val", Types.DateType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);
      GenericRecord data = GenericRecord.create(schema.findType("data").asStructType());
      data.setField("int_val", i * 10);
      data.setField("long_val", (long) i * 100);
      data.setField("double_val", i * 1.5);
      data.setField("string_val", "row-" + i);
      data.setField("date_val", LocalDate.of(2024, 1, 1).plusDays(i));
      rec.setField("data", data);
      records.add(rec);
    }

    appendData(table, records);

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();
        StructVector dataVec = (StructVector) root.getVector("data");

        assertThat(dataVec.size()).isEqualTo(5);

        IntVector intChild = (IntVector) dataVec.getChild("int_val");
        BigIntVector longChild = (BigIntVector) dataVec.getChild("long_val");
        Float8Vector doubleChild = (Float8Vector) dataVec.getChild("double_val");
        VarCharVector stringChild = (VarCharVector) dataVec.getChild("string_val");

        for (int i = 0; i < NUM_ROWS; i++) {
          assertThat(intChild.get(i)).isEqualTo(i * 10);
          assertThat(longChild.get(i)).isEqualTo((long) i * 100);
          assertThat(doubleChild.get(i)).isEqualTo(i * 1.5);
          assertThat(new String(stringChild.get(i), StandardCharsets.UTF_8)).isEqualTo("row-" + i);
        }

        root.close();
      }
    }
  }

  /** Nested struct: struct containing another struct. */
  @Test
  public void testNestedStruct() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "outer",
                Types.StructType.of(
                    Types.NestedField.required(3, "tag", Types.StringType.get()),
                    Types.NestedField.required(
                        4,
                        "inner",
                        Types.StructType.of(
                            Types.NestedField.required(5, "x", Types.IntegerType.get()),
                            Types.NestedField.required(6, "y", Types.IntegerType.get()))))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);

      Types.StructType innerType = schema.findType("outer.inner").asStructType();
      GenericRecord inner = GenericRecord.create(innerType);
      inner.setField("x", i);
      inner.setField("y", i * 2);

      GenericRecord outer = GenericRecord.create(schema.findType("outer").asStructType());
      outer.setField("tag", "t-" + i);
      outer.setField("inner", inner);
      rec.setField("outer", outer);
      records.add(rec);
    }

    appendData(table, records);

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();

        StructVector outerVec = (StructVector) root.getVector("outer");
        assertThat(outerVec.size()).isEqualTo(2);

        VarCharVector tagVec = (VarCharVector) outerVec.getChild("tag");
        StructVector innerVec = (StructVector) outerVec.getChild("inner");
        assertThat(innerVec.size()).isEqualTo(2);

        IntVector xVec = (IntVector) innerVec.getChild("x");
        IntVector yVec = (IntVector) innerVec.getChild("y");

        for (int i = 0; i < NUM_ROWS; i++) {
          assertThat(new String(tagVec.get(i), StandardCharsets.UTF_8)).isEqualTo("t-" + i);
          assertThat(xVec.get(i)).isEqualTo(i);
          assertThat(yVec.get(i)).isEqualTo(i * 2);
        }

        root.close();
      }
    }
  }

  /** Multiple struct columns alongside primitive columns. */
  @Test
  public void testMultipleStructColumns() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "point",
                Types.StructType.of(
                    Types.NestedField.required(3, "x", Types.DoubleType.get()),
                    Types.NestedField.required(4, "y", Types.DoubleType.get()))),
            Types.NestedField.required(5, "label", Types.StringType.get()),
            Types.NestedField.required(
                6,
                "metadata",
                Types.StructType.of(
                    Types.NestedField.required(7, "key", Types.StringType.get()),
                    Types.NestedField.required(8, "value", Types.IntegerType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);

      GenericRecord point = GenericRecord.create(schema.findType("point").asStructType());
      point.setField("x", (double) i);
      point.setField("y", (double) i * 2);
      rec.setField("point", point);

      rec.setField("label", "label-" + i);

      GenericRecord metadata = GenericRecord.create(schema.findType("metadata").asStructType());
      metadata.setField("key", "k" + i);
      metadata.setField("value", i * 100);
      rec.setField("metadata", metadata);

      records.add(rec);
    }

    appendData(table, records);

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        assertThat(batch.numCols()).isEqualTo(4);

        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();

        // Verify primitive columns
        IntVector idVec = (IntVector) root.getVector("id");
        VarCharVector labelVec = (VarCharVector) root.getVector("label");

        // Verify first struct
        StructVector pointVec = (StructVector) root.getVector("point");
        Float8Vector xVec = (Float8Vector) pointVec.getChild("x");
        Float8Vector yVec = (Float8Vector) pointVec.getChild("y");

        // Verify second struct
        StructVector metaVec = (StructVector) root.getVector("metadata");
        VarCharVector keyVec = (VarCharVector) metaVec.getChild("key");
        IntVector valueVec = (IntVector) metaVec.getChild("value");

        for (int i = 0; i < NUM_ROWS; i++) {
          assertThat(idVec.get(i)).isEqualTo(i);
          assertThat(new String(labelVec.get(i), StandardCharsets.UTF_8)).isEqualTo("label-" + i);
          assertThat(xVec.get(i)).isEqualTo((double) i);
          assertThat(yVec.get(i)).isEqualTo((double) i * 2);
          assertThat(new String(keyVec.get(i), StandardCharsets.UTF_8)).isEqualTo("k" + i);
          assertThat(valueVec.get(i)).isEqualTo(i * 100);
        }

        root.close();
      }
    }
  }

  /** Struct data is read correctly when the batch size is smaller than the total row count. */
  @Test
  public void testStructWithSmallerBatchSize() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "pair",
                Types.StructType.of(
                    Types.NestedField.required(3, "a", Types.IntegerType.get()),
                    Types.NestedField.required(4, "b", Types.IntegerType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);
      GenericRecord pair = GenericRecord.create(schema.findType("pair").asStructType());
      pair.setField("a", i);
      pair.setField("b", i + 100);
      rec.setField("pair", pair);
      records.add(rec);
    }

    appendData(table, records);

    int batchSize = 7;
    int totalRows = 0;
    int batchIndex = 0;
    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), batchSize, false)) {
      for (ColumnarBatch batch : reader) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();
        StructVector pairVec = (StructVector) root.getVector("pair");
        IntVector aVec = (IntVector) pairVec.getChild("a");
        IntVector bVec = (IntVector) pairVec.getChild("b");

        for (int i = 0; i < batch.numRows(); i++) {
          int rowIdx = batchIndex * batchSize + i;
          assertThat(aVec.get(i)).isEqualTo(rowIdx);
          assertThat(bVec.get(i)).isEqualTo(rowIdx + 100);
        }

        totalRows += batch.numRows();
        batchIndex++;
        root.close();
      }
    }

    assertThat(totalRows).isEqualTo(NUM_ROWS);
    assertThat(batchIndex).as("Should have multiple batches").isGreaterThan(1);
  }

  /** Struct accessed through the ColumnVector.getChildColumn() API. */
  @Test
  public void testStructAccessViaColumnVector() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "point",
                Types.StructType.of(
                    Types.NestedField.required(3, "x", Types.IntegerType.get()),
                    Types.NestedField.required(4, "y", Types.IntegerType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);
      GenericRecord point = GenericRecord.create(schema.findType("point").asStructType());
      point.setField("x", i * 10);
      point.setField("y", i * 20);
      rec.setField("point", point);
      records.add(rec);
    }

    appendData(table, records);

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        ColumnVector structCol = batch.column(1);
        ColumnVector xChild = structCol.getChildColumn(0);
        ColumnVector yChild = structCol.getChildColumn(1);

        for (int i = 0; i < NUM_ROWS; i++) {
          assertThat(xChild.getInt(i)).isEqualTo(i * 10);
          assertThat(yChild.getInt(i)).isEqualTo(i * 20);
        }
      }
    }
  }

  /** Nullable child fields report null correctly through the ColumnVector API. */
  @Test
  public void testStructChildNullabilityViaColumnVector() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "data",
                Types.StructType.of(
                    Types.NestedField.required(3, "name", Types.StringType.get()),
                    Types.NestedField.optional(4, "value", Types.IntegerType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);
      GenericRecord data = GenericRecord.create(schema.findType("data").asStructType());
      data.setField("name", "n" + i);
      data.setField("value", i % 2 == 0 ? null : i);
      rec.setField("data", data);
      records.add(rec);
    }

    appendData(table, records);

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        ColumnVector structCol = batch.column(1);
        ColumnVector nameChild = structCol.getChildColumn(0);
        ColumnVector valueChild = structCol.getChildColumn(1);

        for (int i = 0; i < NUM_ROWS; i++) {
          assertThat(nameChild.isNullAt(i)).isFalse();
          assertThat(nameChild.getString(i)).isEqualTo("n" + i);
          if (i % 2 == 0) {
            assertThat(valueChild.isNullAt(i))
                .as("Row %d should be null", i)
                .isTrue();
          } else {
            assertThat(valueChild.isNullAt(i)).isFalse();
            assertThat(valueChild.getInt(i)).isEqualTo(i);
          }
        }
      }
    }
  }

  /** Struct field is returned as StructVector in the VectorSchemaRoot. */
  @Test
  public void testStructArrowSchemaInVectorSchemaRoot() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(
                2,
                "attrs",
                Types.StructType.of(
                    Types.NestedField.required(3, "name", Types.StringType.get()),
                    Types.NestedField.optional(4, "score", Types.DoubleType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);
      GenericRecord attrs = GenericRecord.create(schema.findType("attrs").asStructType());
      attrs.setField("name", "n" + i);
      attrs.setField("score", i * 0.5);
      rec.setField("attrs", attrs);
      records.add(rec);
    }

    appendData(table, records);

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), 1024, false)) {
      for (ColumnarBatch batch : reader) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();

        // Verify the Arrow schema reflects the struct type
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = root.getSchema();
        assertThat(arrowSchema.getFields()).hasSize(2);

        org.apache.arrow.vector.types.pojo.Field structField = arrowSchema.getFields().get(1);
        assertThat(structField.getName()).isEqualTo("attrs");
        assertThat(structField.getType())
            .isInstanceOf(org.apache.arrow.vector.types.pojo.ArrowType.Struct.class);
        assertThat(structField.getChildren()).hasSize(2);
        assertThat(structField.getChildren().get(0).getName()).isEqualTo("name");
        assertThat(structField.getChildren().get(1).getName()).isEqualTo("score");

        // Verify the vector type
        FieldVector attrsVec = root.getVector("attrs");
        assertThat(attrsVec).isInstanceOf(StructVector.class);

        root.close();
      }
    }
  }

  /** Reading with container reuse enabled. */
  @Test
  public void testStructWithReuseContainers() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "pair",
                Types.StructType.of(
                    Types.NestedField.required(3, "a", Types.IntegerType.get()),
                    Types.NestedField.required(4, "b", Types.IntegerType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);
      GenericRecord pair = GenericRecord.create(schema.findType("pair").asStructType());
      pair.setField("a", i);
      pair.setField("b", i + 1);
      rec.setField("pair", pair);
      records.add(rec);
    }

    appendData(table, records);

    int batchSize = 7;
    int totalRows = 0;
    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), batchSize, true)) {
      for (ColumnarBatch batch : reader) {
        // Access struct data through the ColumnarBatch API (not VectorSchemaRoot)
        // since VectorSchemaRoot.close() would release the vectors that are being reused
        ColumnVector structCol = batch.column(1);
        ColumnVector aChild = structCol.getChildColumn(0);
        ColumnVector bChild = structCol.getChildColumn(1);

        for (int i = 0; i < batch.numRows(); i++) {
          int rowIdx = totalRows + i;
          assertThat(aChild.getInt(i)).isEqualTo(rowIdx);
          assertThat(bChild.getInt(i)).isEqualTo(rowIdx + 1);
        }

        totalRows += batch.numRows();
      }
    }

    assertThat(totalRows).isEqualTo(NUM_ROWS);
  }

  /** Only the struct column is projected (no primitive siblings). */
  @Test
  public void testSelectOnlyStructColumn() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "pair",
                Types.StructType.of(
                    Types.NestedField.required(3, "a", Types.IntegerType.get()),
                    Types.NestedField.required(4, "b", Types.IntegerType.get()))),
            Types.NestedField.required(5, "name", Types.StringType.get()));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);
      GenericRecord pair = GenericRecord.create(schema.findType("pair").asStructType());
      pair.setField("a", i * 3);
      pair.setField("b", i * 4);
      rec.setField("pair", pair);
      rec.setField("name", "n" + i);
      records.add(rec);
    }

    appendData(table, records);

    // Project only the struct column
    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(
            table.newScan().select("pair"), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        assertThat(batch.numCols()).isEqualTo(1);

        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();
        assertThat(root.getSchema().getFields()).hasSize(1);

        StructVector pairVec = (StructVector) root.getVector("pair");
        IntVector aVec = (IntVector) pairVec.getChild("a");
        IntVector bVec = (IntVector) pairVec.getChild("b");

        for (int i = 0; i < NUM_ROWS; i++) {
          assertThat(aVec.get(i)).isEqualTo(i * 3);
          assertThat(bVec.get(i)).isEqualTo(i * 4);
        }

        root.close();
      }
    }
  }

  /** Struct alongside a filter on a primitive column. */
  @Test
  public void testStructWithRowFilter() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "pair",
                Types.StructType.of(
                    Types.NestedField.required(3, "a", Types.IntegerType.get()),
                    Types.NestedField.required(4, "b", Types.IntegerType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("id", i);
      GenericRecord pair = GenericRecord.create(schema.findType("pair").asStructType());
      pair.setField("a", i);
      pair.setField("b", i + 1);
      rec.setField("pair", pair);
      records.add(rec);
    }

    appendData(table, records);

    // The filter here is a residual that still gets applied; the scan should succeed
    // even with struct columns present
    int totalRows = 0;
    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();
        StructVector pairVec = (StructVector) root.getVector("pair");
        assertThat(pairVec).isNotNull();
        totalRows += batch.numRows();
        root.close();
      }
    }

    assertThat(totalRows).isEqualTo(NUM_ROWS);
  }

  /** Struct-only schema (no top-level primitive columns). */
  @Test
  public void testStructOnlySchema() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "point",
                Types.StructType.of(
                    Types.NestedField.required(2, "x", Types.IntegerType.get()),
                    Types.NestedField.required(3, "y", Types.IntegerType.get()))));

    Table table = createTable(schema);
    List<GenericRecord> records = Lists.newArrayList();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      GenericRecord point = GenericRecord.create(schema.findType("point").asStructType());
      point.setField("x", i);
      point.setField("y", i * 10);
      rec.setField("point", point);
      records.add(rec);
    }

    appendData(table, records);

    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        assertThat(batch.numCols()).isEqualTo(1);
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();
        StructVector pointVec = (StructVector) root.getVector("point");

        IntVector xVec = (IntVector) pointVec.getChild("x");
        IntVector yVec = (IntVector) pointVec.getChild("y");

        for (int i = 0; i < NUM_ROWS; i++) {
          assertThat(xVec.get(i)).isEqualTo(i);
          assertThat(yVec.get(i)).isEqualTo(i * 10);
        }

        root.close();
      }
    }
  }

  /** Data written across multiple Parquet files is read correctly. */
  @Test
  public void testStructAcrossMultipleFiles() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "pair",
                Types.StructType.of(
                    Types.NestedField.required(3, "a", Types.IntegerType.get()),
                    Types.NestedField.required(4, "b", Types.IntegerType.get()))));

    Table table = createTable(schema);

    // Write two separate files
    for (int file = 0; file < 2; file++) {
      List<GenericRecord> records = Lists.newArrayList();
      for (int i = 0; i < NUM_ROWS; i++) {
        int val = file * NUM_ROWS + i;
        GenericRecord rec = GenericRecord.create(schema);
        rec.setField("id", val);
        GenericRecord pair = GenericRecord.create(schema.findType("pair").asStructType());
        pair.setField("a", val);
        pair.setField("b", val * 2);
        rec.setField("pair", pair);
        records.add(rec);
      }
      appendData(table, records);
    }

    int totalRows = 0;
    try (VectorizedTableScanIterable reader =
        new VectorizedTableScanIterable(table.newScan(), NUM_ROWS, false)) {
      for (ColumnarBatch batch : reader) {
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();
        StructVector pairVec = (StructVector) root.getVector("pair");
        IntVector aVec = (IntVector) pairVec.getChild("a");
        IntVector idVec = (IntVector) root.getVector("id");

        for (int i = 0; i < batch.numRows(); i++) {
          // struct child values should match the id column
          assertThat(aVec.get(i)).isEqualTo(idVec.get(i));
        }

        totalRows += batch.numRows();
        root.close();
      }
    }

    assertThat(totalRows).isEqualTo(NUM_ROWS * 2);
  }

  private Table createTable(Schema schema) {
    return tables.create(
        schema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"),
        tableLocation);
  }

  private void appendData(Table table, List<GenericRecord> records) throws IOException {
    File parquetFile = File.createTempFile("struct-test", null, tempDir);
    assertThat(parquetFile.delete()).isTrue();

    FileAppender<GenericRecord> appender =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::create)
            .build();
    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(localInput(parquetFile))
            .withMetrics(appender.metrics())
            .withFormat(FileFormat.PARQUET)
            .build();

    table.newAppend().appendFile(dataFile).commit();
  }
}
