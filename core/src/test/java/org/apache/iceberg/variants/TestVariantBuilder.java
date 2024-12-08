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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;
import net.minidev.json.JSONArray;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestVariantBuilder {
  @ParameterizedTest
  @MethodSource("primitiveInputs")
  public void testPrimitive(String input, Variants.PhysicalType expectedType, Object expectedValue) throws IOException {
    Variant variant = VariantBuilder.parseJson(input);

    SerializedPrimitive primitive = SerializedPrimitive.from(variant);

    assertThat(primitive.type()).isEqualTo(expectedType);
    assertThat(primitive.get()).isEqualTo(expectedValue);
  }

  private static Stream<Arguments> primitiveInputs() {
    return Stream.of(
        Arguments.of("null", Variants.PhysicalType.NULL, null),
        Arguments.of("true", Variants.PhysicalType.BOOLEAN_TRUE, true),
        Arguments.of("false", Variants.PhysicalType.BOOLEAN_FALSE, false),
        Arguments.of("34", Variants.PhysicalType.INT8, (byte)34),
        Arguments.of("1234", Variants.PhysicalType.INT16, (short)1234),
        Arguments.of("1234567890", Variants.PhysicalType.INT32, 1234567890),
        Arguments.of("1234567890987654321", Variants.PhysicalType.INT64, 1234567890987654321L),
        Arguments.of("1234e-2", Variants.PhysicalType.DOUBLE, 12.34),
        Arguments.of("123456.789", Variants.PhysicalType.DECIMAL4, new BigDecimal("123456.789")),
        Arguments.of("123456789.987654321", Variants.PhysicalType.DECIMAL8, new BigDecimal("123456789.987654321")),
        Arguments.of("12345678901234567890.987654321", Variants.PhysicalType.DECIMAL16, new BigDecimal("12345678901234567890.987654321")),
        Arguments.of("\"This test string is used to generate a primitive string type of variant\"", Variants.PhysicalType.STRING, "This test string is used to generate a primitive string type of variant")

        );
  }

  @Test
  public void testPrimitiveFloat() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendFloat(12.34f);
    Variant variant = builder.build();
    SerializedPrimitive primitive = SerializedPrimitive.from(variant);

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.FLOAT);
    assertThat(primitive.get()).isEqualTo(12.34f);
  }

  @Test
  public void testPrimitiveDate() {
    String dateString = "2017-08-18";
    int daysSinceEpoch = DateTimeUtil.isoDateToDays(dateString);

    VariantBuilder builder = new VariantBuilder();
    builder.appendDate(daysSinceEpoch);
    Variant variant = builder.build();
    SerializedPrimitive primitive = SerializedPrimitive.from(variant);

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.DATE);
    assertThat(DateTimeUtil.daysToIsoDate((int)primitive.get())).isEqualTo(dateString);
  }

  @Test
  public void testPrimitiveTimestampTZ() {
    String tzString = "2017-08-18T14:21:01.919+00:00";
    long microsSinceEpoch = DateTimeUtil.isoTimestamptzToMicros(tzString);

    VariantBuilder builder = new VariantBuilder();
    builder.appendTimestampTz(microsSinceEpoch);
    Variant variant = builder.build();
    SerializedPrimitive primitive = SerializedPrimitive.from(variant);

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.TIMESTAMPTZ);
    assertThat(DateTimeUtil.microsToIsoTimestamptz((long)primitive.get())).isEqualTo(tzString);
  }

  @Test
  public void testPrimitiveTimestampNTZ() {
    String ntzString = "2017-08-18T14:21:01.919";
    long microsSinceEpoch = DateTimeUtil.isoTimestampToMicros(ntzString);

    VariantBuilder builder = new VariantBuilder();
    builder.appendTimestampNtz(microsSinceEpoch);
    Variant variant = builder.build();
    SerializedPrimitive primitive = SerializedPrimitive.from(variant);

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.TIMESTAMPNTZ);
    assertThat(DateTimeUtil.microsToIsoTimestamp((long)primitive.get())).isEqualTo(ntzString);
  }

  @Test
  public void testPrimitiveBinary() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendBinary("iceberg".getBytes());
    Variant variant = builder.build();
    SerializedPrimitive primitive = SerializedPrimitive.from(variant);

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.BINARY);
    assertThat(primitive.get()).isEqualTo(ByteBuffer.wrap("iceberg".getBytes()));
  }

  @Test
  public void testShortString() throws IOException {
    Variant variant = VariantBuilder.parseJson("\"iceberg\"");
    SerializedShortString shortString = SerializedShortString.from(variant);

    assertThat(shortString.type()).isEqualTo(Variants.PhysicalType.STRING);
    assertThat(shortString.get()).isEqualTo("iceberg");
  }

  @Test
  public void testArray() throws IOException {
    List<String> input = List.of("Ford", "BMW", "Fiat");
    Variant variant = VariantBuilder.parseJson(JSONArray.toJSONString(input));
    SerializedArray arr = SerializedArray.from(variant);

    assertThat(arr.type()).isEqualTo(Variants.PhysicalType.ARRAY);
    for (int i = 0; i < arr.numElements(); i++) {
      assertThat(arr.get(i).asPrimitive().get()).isEqualTo(input.get(i));
    }
  }

  @Test
  public void testEmptyObject() throws IOException {
    Variant variant = VariantBuilder.parseJson("{}");
    SerializedObject object = SerializedObject.from(variant);

    assertThat(object.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(0);
  }

  @Test
  public void testObject() throws IOException {
    Variant variant = VariantBuilder.parseJson("{ \"id\": 1234, \"firstName\": \"Joe\", \"lastName\": \"Smith\", \"phones\":[\"123-456-7890\", \"789-123-4560\"] }");
    SerializedObject object = SerializedObject.from(variant);

    assertThat(object.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(4);

    assertThat(object.get("id").asPrimitive().get()).isEqualTo((short)1234);
    assertThat(object.get("firstName").asPrimitive().get()).isEqualTo("Joe");
    assertThat(object.get("lastName").asPrimitive().get()).isEqualTo("Smith");

    VariantArray phones = object.get("phones").asArray();
    assertThat(phones.numElements()).isEqualTo(2);
    assertThat(phones.get(0).asPrimitive().get()).isEqualTo("123-456-7890");
    assertThat(phones.get(1).asPrimitive().get()).isEqualTo("789-123-4560");
  }
}
