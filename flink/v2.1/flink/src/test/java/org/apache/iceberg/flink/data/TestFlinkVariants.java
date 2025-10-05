/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the License, Version 2.0 (the
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.variant.Variant;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestFlinkVariants {

  @TempDir private Path temp;

  private static final VariantPrimitive<?>[] PRIMITIVES =
      new VariantPrimitive[] {
        Variants.ofNull(),
        Variants.of(true),
        Variants.of(false),
        Variants.of((byte) 34),
        Variants.of((byte) -34),
        Variants.of((short) 1234),
        Variants.of((short) -1234),
        Variants.of(12345),
        Variants.of(-12345),
        Variants.of(9876543210L),
        Variants.of(-9876543210L),
        Variants.of(10.11F),
        Variants.of(-10.11F),
        Variants.of(14.3D),
        Variants.of(-14.3D),
        Variants.ofIsoDate("2024-11-07"),
        Variants.ofIsoDate("1957-11-07"),
        Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00"),
        Variants.ofIsoTimestamptz("1957-11-07T12:33:54.123456+00:00"),
        Variants.ofIsoTimestampntz("2024-11-07T12:33:54.123456"),
        Variants.ofIsoTimestampntz("1957-11-07T12:33:54.123456"),
        Variants.of(new BigDecimal("12345.6789")), // decimal4
        Variants.of(new BigDecimal("-12345.6789")), // decimal4
        Variants.of(new BigDecimal("123456789.987654321")), // decimal8
        Variants.of(new BigDecimal("-123456789.987654321")), // decimal8
        Variants.of(new BigDecimal("9876543210.123456789")), // decimal16
        Variants.of(new BigDecimal("-9876543210.123456789")), // decimal16
        Variants.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d})),
        Variants.of("iceberg"),
        Variants.ofUUID("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"),
      };

  private static final VariantPrimitive<?>[] UNSUPPORTED_PRIMITIVES =
      new VariantPrimitive[] {
        Variants.ofIsoTime("12:33:54.123456"),
        Variants.ofIsoTimestamptzNanos("2024-11-07T12:33:54.123456789+00:00"),
        Variants.ofIsoTimestampntzNanos("2024-11-07T12:33:54.123456789"),
      };

  @Test
  public void testIcebergVariantTypeToFlinkVariantType() {
    // Test that Iceberg's VariantType converts to Flink's VariantType
    Types.VariantType icebergVariantType = Types.VariantType.get();
    LogicalType flinkVariantType = FlinkSchemaUtil.convert(icebergVariantType);
    
    assertThat(flinkVariantType).isInstanceOf(org.apache.flink.table.types.logical.VariantType.class);
  }

  @Test
  public void testFlinkVariantTypeToIcebergVariantType() {
    org.apache.flink.table.types.logical.VariantType flinkVariantType = 
        new org.apache.flink.table.types.logical.VariantType(false);
    Types.VariantType icebergVariantType = (Types.VariantType) FlinkSchemaUtil.convert(flinkVariantType);
    
    assertThat(icebergVariantType).isEqualTo(Types.VariantType.get());
  }

  @ParameterizedTest
  @FieldSource("PRIMITIVES")
  public void testVariantPrimitiveRoundTrip(VariantPrimitive<?> primitive) {
    testVariantRoundTrip(Variants.emptyMetadata(), primitive);
  }

  @Test
  public void testVariantArrayRoundTrip() {
    VariantMetadata metadata = Variants.emptyMetadata();
    ValueArray array = Variants.array();
    array.add(Variants.of("hello"));
    array.add(Variants.of((byte) 42));
    array.add(Variants.ofNull());

    testVariantRoundTrip(metadata, array);
  }

  @Test
  public void testVariantObjectRoundTrip() {
    VariantMetadata metadata = Variants.metadata("name", "age", "active");
    ShreddedObject object = Variants.object(metadata);
    object.put("name", Variants.of("John Doe"));
    object.put("age", Variants.of((byte) 30));
    object.put("active", Variants.of(true));

    testVariantRoundTrip(metadata, object);
  }

  @Test
  public void testVariantNestedStructures() {
    VariantMetadata metadata = Variants.metadata("user", "scores", "address", "city", "state");

    // Create nested object: address
    ShreddedObject address = Variants.object(metadata);
    address.put("city", Variants.of("Anytown"));
    address.put("state", Variants.of("CA"));

    // Create array of scores
    ValueArray scores = Variants.array();
    scores.add(Variants.of((byte) 95));
    scores.add(Variants.of((byte) 87));
    scores.add(Variants.of((byte) 92));

    // Create main object
    ShreddedObject mainObject = Variants.object(metadata);
    mainObject.put("user", Variants.of("Jane"));
    mainObject.put("scores", scores);
    mainObject.put("address", address);

    testVariantRoundTrip(metadata, mainObject);
  }

  @ParameterizedTest
  @FieldSource("UNSUPPORTED_PRIMITIVES")
  public void testUnsupportedOperations(VariantPrimitive<?> primitive) {
    ByteBuffer valueBuffer =
        ByteBuffer.allocate(primitive.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    primitive.writeTo(valueBuffer, 0);

    org.apache.flink.types.variant.BinaryVariant flinkVariant =
        new org.apache.flink.types.variant.BinaryVariant(
            valueBuffer.array(), VariantTestUtil.emptyMetadata().array());

    assertThat(flinkVariant).isNotNull();
    assertThat(flinkVariant.getValue()).isNotNull();
    assertThat(flinkVariant.getMetadata()).isNotNull();
  }

  @Test
  public void testVariantWriteAndRead() throws IOException {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "data", Types.VariantType.get())
    );

    LogicalType logicalType = FlinkSchemaUtil.convert(schema);
    assertThat(logicalType).isNotNull();
    
    assertThat(logicalType).isInstanceOf(org.apache.flink.table.types.logical.RowType.class);
    org.apache.flink.table.types.logical.RowType rowType = 
        (org.apache.flink.table.types.logical.RowType) logicalType;
    
    assertThat(rowType.getChildren().get(1))
        .isInstanceOf(org.apache.flink.table.types.logical.VariantType.class);
  }

  @Test
  public void testVariantBinaryVariantSupport() {   
    Schema schema = new Schema(
        Types.NestedField.required(1, "data", Types.VariantType.get())
    );
    
    LogicalType logicalType = FlinkSchemaUtil.convert(schema);
    assertThat(logicalType).isNotNull();
    
    assertThat(logicalType).isInstanceOf(org.apache.flink.table.types.logical.RowType.class);
  }

  @Test
  public void testVariantSchemaVisitor() throws IOException {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "data", Types.VariantType.get())
    );

    LogicalType logicalType = FlinkSchemaUtil.convert(schema);
    
    assertThat(logicalType).isNotNull();
    
    OutputFile outputFile = new InMemoryOutputFile();
    
    FileAppender<RowData> writer = Parquet.write(outputFile)
        .schema(schema)
        .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(logicalType, msgType))
        .build();
    
    writer.close();
    
    assertThat(writer).isNotNull();
  }

  private void testVariantRoundTrip(VariantMetadata metadata, VariantValue value) {
    org.apache.iceberg.variants.Variant icebergVariant = org.apache.iceberg.variants.Variant.of(metadata, value);

    ByteBuffer metadataBuffer =
        ByteBuffer.allocate(metadata.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    metadata.writeTo(metadataBuffer, 0);

    ByteBuffer valueBuffer =
        ByteBuffer.allocate(value.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    value.writeTo(valueBuffer, 0);

    byte[] metadataBytes = ByteBuffers.toByteArray(metadataBuffer);
    byte[] valueBytes = ByteBuffers.toByteArray(valueBuffer);
    org.apache.flink.types.variant.BinaryVariant flinkVariant = 
        new org.apache.flink.types.variant.BinaryVariant(valueBytes, metadataBytes);

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
