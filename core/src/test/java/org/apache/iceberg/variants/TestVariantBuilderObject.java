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
import org.junit.jupiter.api.Test;

public class TestVariantBuilderObject {
  @Test
  public void testEmptyObjectJson() throws IOException {
    Variant variant = VariantBuilder.parseJson("{}");
    VariantObject object = variant.value().asObject();

    assertThat(object.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(0);
  }

  @Test
  public void testObjectJson() throws IOException {
    String input =
        "{\n"
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
            + " }";

    validateVariant(VariantBuilder.parseJson(input));
  }

  @Test
  public void testBuildObject() {
    VariantObjectBuilder builder =
        new VariantBuilder()
            .startObject()
            .writeString("firstName", "John")
            .writeString("lastName", "Smith")
            .writeNumeric("age", 25);
    builder
        .startObject("address")
        .writeString("streetAddress", "21 2nd Street")
        .writeString("city", "New York")
        .writeString("state", "NY")
        .writeString("postalCode", "10021")
        .endObject();
    VariantArrayBuilder phoneNumberBuilder = builder.startArray("phoneNumber");
    phoneNumberBuilder
        .startObject()
        .writeString("type", "home")
        .writeString("number", "212 555-1234")
        .endObject();
    phoneNumberBuilder
        .startObject()
        .writeString("type", "fax")
        .writeString("number", "646 555-4567")
        .endObject();
    phoneNumberBuilder.endArray();
    builder.endObject();

    validateVariant(builder.build());
  }

  private void validateVariant(Variant variant) {
    VariantObject object = variant.value().asObject();

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
