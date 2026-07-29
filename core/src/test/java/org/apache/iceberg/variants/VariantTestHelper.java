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
package org.apache.iceberg.variants;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public final class VariantTestHelper {

  private VariantTestHelper() {}

  public static final VariantPrimitive<?>[] PRIMITIVES =
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

  public static final VariantPrimitive<?>[] UNSUPPORTED_PRIMITIVES =
      new VariantPrimitive[] {
        Variants.ofIsoTime("12:33:54.123456"),
        Variants.ofIsoTimestamptzNanos("2024-11-07T12:33:54.123456789+00:00"),
        Variants.ofIsoTimestampntzNanos("2024-11-07T12:33:54.123456789"),
      };

  /**
   * Tests round-trip conversion of all primitive variant types.
   *
   * @param testFunction the engine-specific test function
   * @param primitive the primitive variant value to test
   */
  public static void testVariantPrimitiveRoundTrip(
      VariantTestFunction testFunction, VariantPrimitive<?> primitive) {
    testFunction.test(Variants.emptyMetadata(), primitive);
  }

  /**
   * Tests round-trip conversion of array variants.
   *
   * @param testFunction the engine-specific test function
   */
  public static void testVariantArrayRoundTrip(VariantTestFunction testFunction) {
    VariantMetadata metadata = Variants.emptyMetadata();
    ValueArray array = Variants.array();
    array.add(Variants.of("hello"));
    array.add(Variants.of((byte) 42));
    array.add(Variants.ofNull());

    testFunction.test(metadata, array);
  }

  /**
   * Tests round-trip conversion of object variants.
   *
   * @param testFunction the engine-specific test function
   */
  public static void testVariantObjectRoundTrip(VariantTestFunction testFunction) {
    VariantMetadata metadata = Variants.metadata("name", "age", "active");
    ShreddedObject object = Variants.object(metadata);
    object.put("name", Variants.of("John Doe"));
    object.put("age", Variants.of((byte) 30));
    object.put("active", Variants.of(true));

    testFunction.test(metadata, object);
  }

  /**
   * Tests round-trip conversion of nested variant structures.
   *
   * @param testFunction the engine-specific test function
   */
  public static void testVariantNestedStructures(VariantTestFunction testFunction) {
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

    testFunction.test(metadata, mainObject);
  }

  @FunctionalInterface
  public interface VariantTestFunction {
    void test(VariantMetadata metadata, VariantValue value);
  }
}
