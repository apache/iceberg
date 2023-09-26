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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTypes {

  @Test
  public void fromPrimitiveString() {
    Assertions.assertThat(Types.fromPrimitiveString("boolean")).isSameAs(Types.BooleanType.get());
    Assertions.assertThat(Types.fromPrimitiveString("BooLean")).isSameAs(Types.BooleanType.get());

    Assertions.assertThat(Types.fromPrimitiveString("timestamp"))
        .isSameAs(Types.TimestampType.microsWithoutZone());
    Assertions.assertThat(Types.fromPrimitiveString("timestamp_ns"))
        .isSameAs(Types.TimestampType.nanosWithoutZone());

    Assertions.assertThat(Types.fromPrimitiveString("Fixed[ 3 ]"))
        .isEqualTo(Types.FixedType.ofLength(3));

    Assertions.assertThat(Types.fromPrimitiveString("Decimal( 2 , 3 )"))
        .isEqualTo(Types.DecimalType.of(2, 3));

    Assertions.assertThat(Types.fromPrimitiveString("Decimal(2,3)"))
        .isEqualTo(Types.DecimalType.of(2, 3));

    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Types.fromPrimitiveString("Unknown"))
        .withMessageContaining("Unknown");
  }
}
