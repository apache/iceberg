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

import java.math.BigDecimal;
import java.util.UUID;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.types.Types;
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
