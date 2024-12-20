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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import net.minidev.json.JSONArray;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

public class TestVariantBuilderArray {
  @Test
  public void testSimpleArrayJson() throws IOException {
    List<String> input = List.of("Ford", "BMW", "Fiat");
    Variant variant = VariantBuilder.parseJson(JSONArray.toJSONString(input));
    SerializedArray arr = SerializedArray.from(variant);

    assertThat(arr.type()).isEqualTo(Variants.PhysicalType.ARRAY);
    for (int i = 0; i < arr.numElements(); i++) {
      assertThat(arr.get(i).asPrimitive().get()).isEqualTo(input.get(i));
    }
  }

  @Test
  public void testArrayJson() throws IOException {
    String input =
        "[{\n"
            + "   \"firstName\": \"John\","
            + "   \"lastName\": \"Smith\","
            + "   \"age\": 25,\n"
            + "   \"address\" : {\n"
            + "       \"streetAddress\": \"21 2nd Street\",\n"
            + "       \"city\": \"New York\",\n"
            + "       \"state\": \"NY\",\n"
            + "       \"postalCode\": \"10021\"\n"
            + "   },\n"
            + "   \"phoneNumber\": [\n"
            + "       {\"type\": \"home\", \"number\": \"212 555-1234\"},\n"
            + "       {\"type\": \"fax\", \"number\": \"646 555-4567\"}\n"
            + "    ]\n"
            + " }]";
    validateVariant(VariantBuilder.parseJson(input));
  }

  @Test
  public void testBuildSimpleArray() {
    List<String> input = List.of("Ford", "BMW", "Fiat");
    VariantArrayBuilder builder = new VariantBuilder().startArray();
    for (String str : input) {
      builder.writeString(str);
    }
    builder.endArray();

    Variant variant = builder.build();
    SerializedArray arr = SerializedArray.from(variant);

    assertThat(arr.type()).isEqualTo(Variants.PhysicalType.ARRAY);
    assertThat(arr.numElements()).isEqualTo(3);
    for (int i = 0; i < arr.numElements(); i++) {
      assertThat(arr.get(i).asPrimitive().get()).isEqualTo(input.get(i));
    }
  }

  @Test
  public void testBuildArray() {
    VariantArrayBuilder builder = new VariantBuilder().startArray();
    builder
        .writeNull()
        .writeBoolean(true)
        .writeBoolean(false)
        .writeNumeric(34)
        .writeNumeric(1234)
        .writeNumeric(1234567890)
        .writeNumeric(1234567890987654321L)
        .writeDouble(1234e-2)
        .writeDecimal(new BigDecimal("123456.789"))
        .writeDecimal(new BigDecimal("123456789.987654321"))
        .writeDecimal(new BigDecimal("12345678901234567890.987654321"))
        .writeDate(LocalDate.parse("2017-08-18"))
        .writeTimestampTz(OffsetDateTime.parse("2017-08-18T14:21:01.919+00:00"))
        .writeTimestampNtz(LocalDateTime.parse("2017-08-18T14:21:01.919"))
        .writeFloat(12.34f)
        .writeBinary("iceberg".getBytes())
        .writeString("This test string is used to generate a primitive string type of variant")
        .writeString("iceberg");
    builder.startArray().writeString("Ford").writeString("BMW").writeString("Fiat").endArray();

    builder
        .startObject()
        .writeString("firstName", "John")
        .writeString("lastName", "Smith")
        .writeNumeric("age", 25)
        .endObject();
    builder.endArray();

    Variant variant = builder.build();
    SerializedArray array = SerializedArray.from(variant);
    assertThat(array.type()).isEqualTo(Variants.PhysicalType.ARRAY);
    assertThat(array.numElements()).isEqualTo(20);
    assertThat(array.get(0).asPrimitive().get()).isNull();
    assertThat(array.get(1).asPrimitive().get()).isEqualTo(true);
    assertThat(array.get(2).asPrimitive().get()).isEqualTo(false);
    assertThat(array.get(3).asPrimitive().get()).isEqualTo((byte) 34);
    assertThat(array.get(4).asPrimitive().get()).isEqualTo((short) 1234);
    assertThat(array.get(5).asPrimitive().get()).isEqualTo(1234567890);
    assertThat(array.get(6).asPrimitive().get()).isEqualTo(1234567890987654321L);
    assertThat(array.get(7).asPrimitive().get()).isEqualTo(12.34);
    assertThat(array.get(8).asPrimitive().get()).isEqualTo(new BigDecimal("123456.789"));
    assertThat(array.get(9).asPrimitive().get()).isEqualTo(new BigDecimal("123456789.987654321"));
    assertThat(array.get(10).asPrimitive().get())
        .isEqualTo(new BigDecimal("12345678901234567890.987654321"));
    assertThat(array.get(11).asPrimitive().get())
        .isEqualTo(DateTimeUtil.daysFromDate(LocalDate.parse("2017-08-18")));
    assertThat(array.get(12).asPrimitive().get())
        .isEqualTo(
            DateTimeUtil.microsFromTimestamptz(
                OffsetDateTime.parse("2017-08-18T14:21:01.919+00:00")));
    assertThat(array.get(13).asPrimitive().get())
        .isEqualTo(
            DateTimeUtil.microsFromTimestamp(LocalDateTime.parse("2017-08-18T14:21:01.919")));
    assertThat(array.get(14).asPrimitive().get()).isEqualTo(12.34f);
    assertThat(array.get(15).asPrimitive().get()).isEqualTo(ByteBuffer.wrap("iceberg".getBytes()));
    assertThat(array.get(16).asPrimitive().get())
        .isEqualTo("This test string is used to generate a primitive string type of variant");
    assertThat(array.get(17).asPrimitive().get()).isEqualTo("iceberg");
    assertThat(array.get(18).type()).isEqualTo(Variants.PhysicalType.ARRAY);

    assertThat(array.get(19).type()).isEqualTo(Variants.PhysicalType.OBJECT);
  }

  private void validateVariant(Variant variant) {
    SerializedArray arr = SerializedArray.from(variant);
    assertThat(arr.numElements()).isEqualTo(1);

    VariantObject object = arr.get(0).asObject();
    assertThat(object.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(5);

    assertThat(object.get("firstName").asPrimitive().get()).isEqualTo("John");
    assertThat(object.get("lastName").asPrimitive().get()).isEqualTo("Smith");
    assertThat(object.get("age").asPrimitive().get()).isEqualTo((byte) 25);

    VariantObject address = object.get("address").asObject();
    assertThat(address.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    assertThat(address.numElements()).isEqualTo(4);
    assertThat(address.get("streetAddress").asPrimitive().get()).isEqualTo("21 2nd Street");
    assertThat(address.get("city").asPrimitive().get()).isEqualTo("New York");
    assertThat(address.get("state").asPrimitive().get()).isEqualTo("NY");
    assertThat(address.get("postalCode").asPrimitive().get()).isEqualTo("10021");

    VariantArray phoneNumbers = object.get("phoneNumber").asArray();
    assertThat(phoneNumbers.numElements()).isEqualTo(2);
    VariantObject phoneNumber1 = phoneNumbers.get(0).asObject();
    assertThat(phoneNumber1.get("type").asPrimitive().get()).isEqualTo("home");
    assertThat(phoneNumber1.get("number").asPrimitive().get()).isEqualTo("212 555-1234");
    VariantObject phoneNumber2 = phoneNumbers.get(1).asObject();
    assertThat(phoneNumber2.get("type").asPrimitive().get()).isEqualTo("fax");
    assertThat(phoneNumber2.get("number").asPrimitive().get()).isEqualTo("646 555-4567");
  }
}
