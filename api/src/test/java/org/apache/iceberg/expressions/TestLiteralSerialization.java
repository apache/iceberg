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
package org.apache.iceberg.expressions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.UUID;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLiteralSerialization {
  @Test
  public void testLiterals() throws Exception {
    Literal[] literals =
        new Literal[] {
          Literal.of(false),
          Literal.of(34),
          Literal.of(35L),
          Literal.of(36.75F),
          Literal.of(8.75D),
          Literal.of("2017-11-29").to(Types.DateType.get()),
          Literal.of("11:30:07").to(Types.TimeType.get()),
          Literal.of("2017-11-29T11:30:07.123456").to(Types.TimestampType.withoutZone()),
          Literal.of("2017-11-29T11:30:07.123456+01:00").to(Types.TimestampType.withZone()),
          Literal.of("2017-11-29T11:30:07.123456789").to(Types.TimestampNanoType.withoutZone()),
          Literal.of("2017-11-29T11:30:07.123456789+01:00").to(Types.TimestampNanoType.withZone()),
          Literal.of("abc"),
          Literal.of(UUID.randomUUID()),
          Literal.of(new byte[] {1, 2, 3}).to(Types.FixedType.ofLength(3)),
          Literal.of(new byte[] {3, 4, 5, 6}).to(Types.BinaryType.get()),
          Literal.of(new BigDecimal("122.50")),
        };

    for (Literal<?> lit : literals) {
      checkValue(lit);
    }
  }

  @Test
  public void testVariantLiteral() {
    Variant variantValue = mock(Variant.class);
    Literals.VariantLiteral literal = new Literals.VariantLiteral(variantValue);
    Assertions.assertEquals(
        variantValue, literal.value(), "The value should match the initialized variant");
    Assertions.assertEquals(Type.TypeID.VARIANT, literal.typeId(), "The typeId should be VARIANT");
  }

  @Test
  public void testVariantLiteralComparator() {
    Variant variantValue1 = VariantTestUtil.createMockVariant("field1", 45);
    Variant variantValue2 = VariantTestUtil.createMockVariant("field1", 90);
    Literals.VariantLiteral literal1 = new Literals.VariantLiteral(variantValue1);
    Comparator<Variant> comparator = literal1.comparator();
    Assertions.assertTrue(
        comparator.compare(variantValue1, variantValue2) <= 0,
        "Comparator should order values correctly");
  }

  private <T> void checkValue(Literal<T> lit) throws Exception {
    Literal<T> copy = TestHelpers.roundTripSerialize(lit);
    assertThat(lit.comparator().compare(lit.value(), copy.value()))
        .as("Literal's comparator should consider values equal")
        .isZero();
    assertThat(copy.comparator().compare(lit.value(), copy.value()))
        .as("Copy's comparator should consider values equal")
        .isZero();
  }
}
