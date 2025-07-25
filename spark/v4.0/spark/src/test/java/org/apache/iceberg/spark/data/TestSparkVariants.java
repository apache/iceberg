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
package org.apache.iceberg.spark.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.spark.SparkRuntimeException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.VariantType$;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestSparkVariants extends TestBase {
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
  public void testIcebergVariantTypeToSparkVariantType() {
    // Test that Iceberg's VariantType converts to Spark's VariantType
    Types.VariantType icebergVariantType = Types.VariantType.get();
    DataType sparkVariantType = SparkSchemaUtil.convert(icebergVariantType);

    assertThat(sparkVariantType).isEqualTo(VariantType$.MODULE$);
  }

  @Test
  public void testSparkVariantTypeToIcebergVariantType() {
    // Test that Spark's VariantType converts to Iceberg's VariantType
    org.apache.spark.sql.types.DataType sparkVariantType = VariantType$.MODULE$;
    Type icebergVariantType = SparkSchemaUtil.convert(sparkVariantType);

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
    // This tests the current state where Spark integration is not fully implemented
    // TIME, nano timestamps are not supported in Spark
    ByteBuffer valueBuffer =
        ByteBuffer.allocate(primitive.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    primitive.writeTo(valueBuffer, 0);

    org.apache.spark.types.variant.Variant sparkVariant =
        new org.apache.spark.types.variant.Variant(
            valueBuffer.array(), VariantTestUtil.emptyMetadata().array());

    assertThatThrownBy(sparkVariant::getType)
        .as("Unsupported variant type in Spark")
        .isInstanceOf(SparkRuntimeException.class);
  }

  private void testVariantRoundTrip(VariantMetadata metadata, VariantValue value) {
    // Create Iceberg variant
    Variant icebergVariant = Variant.of(metadata, value);

    // Serialize to bytes
    ByteBuffer metadataBuffer =
        ByteBuffer.allocate(metadata.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    metadata.writeTo(metadataBuffer, 0);

    ByteBuffer valueBuffer =
        ByteBuffer.allocate(value.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    value.writeTo(valueBuffer, 0);

    // Create Spark VariantVal from the same bytes
    byte[] metadataBytes = metadataBuffer.array();
    byte[] valueBytes = valueBuffer.array();
    VariantVal sparkVariant = new VariantVal(valueBytes, metadataBytes);

    GenericsHelpers.assertEquals(icebergVariant, sparkVariant);

    // TODO: Round-trip: use Spark VariantBuilder to build a Spark variant from Iceberg variant and
    // deserialize back to Iceberg variant currently VariantBuilder doesn't have an easy way to
    // construct array/object.
  }
}
