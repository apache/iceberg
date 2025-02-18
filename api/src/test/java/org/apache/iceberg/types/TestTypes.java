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
package org.apache.iceberg.types;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

public class TestTypes {

  @Test
  public void fromTypeName() {
    assertThat(Types.fromTypeName("boolean")).isSameAs(Types.BooleanType.get());
    assertThat(Types.fromTypeName("BooLean")).isSameAs(Types.BooleanType.get());

    assertThat(Types.fromTypeName("timestamp")).isSameAs(Types.TimestampType.withoutZone());
    assertThat(Types.fromTypeName("timestamptz")).isSameAs(Types.TimestampType.withZone());
    assertThat(Types.fromTypeName("timestamp_ns")).isSameAs(Types.TimestampNanoType.withoutZone());
    assertThat(Types.fromTypeName("timestamptz_ns")).isSameAs(Types.TimestampNanoType.withZone());

    assertThat(Types.fromTypeName("Fixed[ 3 ]")).isEqualTo(Types.FixedType.ofLength(3));

    assertThat(Types.fromTypeName("Decimal( 2 , 3 )")).isEqualTo(Types.DecimalType.of(2, 3));

    assertThat(Types.fromTypeName("Decimal(2,3)")).isEqualTo(Types.DecimalType.of(2, 3));

    assertThat(Types.fromTypeName("variant")).isSameAs(Types.VariantType.get());
    assertThat(Types.fromTypeName("Variant")).isSameAs(Types.VariantType.get());

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Types.fromTypeName("abcdefghij"))
        .withMessage("Cannot parse type string to primitive: abcdefghij");
  }

  @Test
  public void fromPrimitiveString() {
    assertThat(Types.fromPrimitiveString("boolean")).isSameAs(Types.BooleanType.get());
    assertThat(Types.fromPrimitiveString("BooLean")).isSameAs(Types.BooleanType.get());

    assertThat(Types.fromPrimitiveString("timestamp")).isSameAs(Types.TimestampType.withoutZone());
    assertThat(Types.fromPrimitiveString("timestamptz")).isSameAs(Types.TimestampType.withZone());
    assertThat(Types.fromPrimitiveString("timestamp_ns"))
        .isSameAs(Types.TimestampNanoType.withoutZone());
    assertThat(Types.fromPrimitiveString("timestamptz_ns"))
        .isSameAs(Types.TimestampNanoType.withZone());

    assertThat(Types.fromPrimitiveString("Fixed[ 3 ]")).isEqualTo(Types.FixedType.ofLength(3));

    assertThat(Types.fromPrimitiveString("Decimal( 2 , 3 )")).isEqualTo(Types.DecimalType.of(2, 3));

    assertThat(Types.fromPrimitiveString("Decimal(2,3)")).isEqualTo(Types.DecimalType.of(2, 3));

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Types.fromPrimitiveString("variant"))
        .withMessage("Cannot parse type string: variant is not a primitive type");
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Types.fromPrimitiveString("Variant"))
        .withMessage("Cannot parse type string: variant is not a primitive type");

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Types.fromPrimitiveString("abcdefghij"))
        .withMessage("Cannot parse type string to primitive: abcdefghij");
  }

  @Test
  public void testNestedFieldBuilderIdCheck() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> optional("field").ofType(Types.StringType.get()).build())
        .withMessage("Id cannot be null");

    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> required("field").ofType(Types.StringType.get()).build())
        .withMessage("Id cannot be null");
  }
}
