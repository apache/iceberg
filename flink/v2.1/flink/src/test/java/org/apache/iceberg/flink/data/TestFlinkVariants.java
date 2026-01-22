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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.VariantTestHelper;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestFlinkVariants {

  @TempDir private Path temp;

  @Test
  public void testIcebergVariantTypeToFlinkVariantType() {
    // Test that Iceberg's VariantType converts to Flink's VariantType
    Types.VariantType icebergVariantType = Types.VariantType.get();
    LogicalType flinkVariantType = FlinkSchemaUtil.convert(icebergVariantType);

    assertThat(flinkVariantType)
        .isInstanceOf(org.apache.flink.table.types.logical.VariantType.class);
  }

  @Test
  public void testFlinkVariantTypeToIcebergVariantType() {
    VariantType flinkVariantType = new VariantType(false);
    org.apache.iceberg.types.Type icebergType = FlinkSchemaUtil.convert(flinkVariantType);

    assertThat(icebergType).isInstanceOf(Types.VariantType.class);
    assertThat(icebergType).isEqualTo(Types.VariantType.get());
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.variants.VariantTestHelper#PRIMITIVES")
  public void testVariantPrimitiveRoundTrip(VariantPrimitive<?> primitive) {
    VariantTestHelper.testVariantPrimitiveRoundTrip(this::testVariantRoundTrip, primitive);
  }

  @Test
  public void testVariantArrayRoundTrip() {
    VariantTestHelper.testVariantArrayRoundTrip(this::testVariantRoundTrip);
  }

  @Test
  public void testVariantObjectRoundTrip() {
    VariantTestHelper.testVariantObjectRoundTrip(this::testVariantRoundTrip);
  }

  @Test
  public void testVariantNestedStructures() {
    VariantTestHelper.testVariantNestedStructures(this::testVariantRoundTrip);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.variants.VariantTestHelper#UNSUPPORTED_PRIMITIVES")
  public void testUnsupportedOperations(VariantPrimitive<?> primitive) {
    ByteBuffer valueBuffer =
        ByteBuffer.allocate(primitive.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    primitive.writeTo(valueBuffer, 0);

    BinaryVariant flinkVariant =
        new BinaryVariant(valueBuffer.array(), VariantTestUtil.emptyMetadata().array());

    assertThat(flinkVariant).isNotNull();
    assertThat(flinkVariant.getValue()).isNotNull();
    assertThat(flinkVariant.getMetadata()).isNotNull();
  }

  @Test
  public void testVariantWriteAndRead() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.VariantType.get()));

    LogicalType logicalType = FlinkSchemaUtil.convert(schema);
    assertThat(logicalType).isNotNull();

    assertThat(logicalType).isInstanceOf(org.apache.flink.table.types.logical.RowType.class);
    assertThat(logicalType.getChildren().get(1))
        .isInstanceOf(org.apache.flink.table.types.logical.VariantType.class);
  }

  @Test
  public void testVariantBinaryVariantSupport() {
    Schema schema = new Schema(Types.NestedField.required(1, "data", Types.VariantType.get()));

    LogicalType logicalType = FlinkSchemaUtil.convert(schema);
    assertThat(logicalType).isNotNull();

    assertThat(logicalType).isInstanceOf(org.apache.flink.table.types.logical.RowType.class);
  }

  @Test
  public void testVariantSchemaVisitor() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.VariantType.get()));

    LogicalType logicalType = FlinkSchemaUtil.convert(schema);

    assertThat(logicalType).isNotNull();

    OutputFile outputFile = new InMemoryOutputFile();

    FileAppender<RowData> writer =
        Parquet.write(outputFile)
            .schema(schema)
            .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(logicalType, msgType))
            .build();

    writer.close();

    assertThat(writer).isNotNull();
  }

  private void testVariantRoundTrip(VariantMetadata metadata, VariantValue value) {
    Variant icebergVariant = Variant.of(metadata, value);

    ByteBuffer metadataBuffer =
        ByteBuffer.allocate(metadata.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    metadata.writeTo(metadataBuffer, 0);

    ByteBuffer valueBuffer =
        ByteBuffer.allocate(value.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    value.writeTo(valueBuffer, 0);

    byte[] metadataBytes = ByteBuffers.toByteArray(metadataBuffer);
    byte[] valueBytes = ByteBuffers.toByteArray(valueBuffer);
    BinaryVariant flinkVariant = new BinaryVariant(valueBytes, metadataBytes);

    assertThat(icebergVariant).isNotNull();
    assertThat(icebergVariant.value()).isNotNull();
    assertThat(icebergVariant.metadata()).isNotNull();

    assertThat(flinkVariant).isNotNull();
    assertThat(flinkVariant.getValue()).isNotNull();
    assertThat(flinkVariant.getMetadata()).isNotNull();

    assertThat(flinkVariant.getValue()).isEqualTo(valueBytes);
    assertThat(flinkVariant.getMetadata()).isEqualTo(metadataBytes);
  }
}
