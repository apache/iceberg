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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.UUID;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestComparators {

  private <T> void assertComparesCorrectly(Comparator<T> cmp, T less, T greater) {
    assertThat(cmp.compare(greater, greater)).isZero();
    assertThat(cmp.compare(less, less)).isZero();
    assertThat(Integer.signum(cmp.compare(less, greater))).isEqualTo(-1);
    assertThat(Integer.signum(cmp.compare(greater, less))).isOne();
  }

  @Test
  public void testBoolean() {
    assertComparesCorrectly(Comparators.forType(Types.BooleanType.get()), false, true);
  }

  @Test
  public void testInt() {
    assertComparesCorrectly(Comparators.forType(Types.IntegerType.get()), 0, 1);
  }

  @Test
  public void testLong() {
    assertComparesCorrectly(Comparators.forType(Types.LongType.get()), 0L, 1L);
  }

  @Test
  public void testFloat() {
    assertComparesCorrectly(Comparators.forType(Types.FloatType.get()), 0.1f, 0.2f);
  }

  @Test
  public void testDouble() {
    assertComparesCorrectly(Comparators.forType(Types.DoubleType.get()), 0.1d, 0.2d);
  }

  @Test
  public void testDate() {
    assertComparesCorrectly(Comparators.forType(Types.DateType.get()), 111, 222);
  }

  @Test
  public void testTime() {
    assertComparesCorrectly(Comparators.forType(Types.TimeType.get()), 111, 222);
  }

  @Test
  public void testTimestamp() {
    assertComparesCorrectly(Comparators.forType(Types.TimestampType.withoutZone()), 111, 222);
    assertComparesCorrectly(Comparators.forType(Types.TimestampType.withZone()), 111, 222);
  }

  @Test
  public void testTimestampNanos() {
    assertComparesCorrectly(Comparators.forType(Types.TimestampNanoType.withoutZone()), 111, 222);
    assertComparesCorrectly(Comparators.forType(Types.TimestampNanoType.withZone()), 111, 222);
  }

  @Test
  public void testString() {
    assertComparesCorrectly(Comparators.forType(Types.StringType.get()), "a", "b");
  }

  @Test
  public void testUuid() {
    assertComparesCorrectly(
        Comparators.forType(Types.UUIDType.get()),
        UUID.fromString("81873e7d-1374-4493-8e1d-9095eff7046c"),
        UUID.fromString("fd02441d-1423-4a3f-8785-c7dd5647e26b"));
  }

  @Test
  public void testFixed() {
    assertComparesCorrectly(
        Comparators.forType(Types.FixedType.ofLength(3)),
        ByteBuffer.wrap(new byte[] {1, 1, 3}),
        ByteBuffer.wrap(new byte[] {1, 2, 1}));
  }

  @Test
  public void testBinary() {
    assertComparesCorrectly(
        Comparators.forType(Types.BinaryType.get()),
        ByteBuffer.wrap(new byte[] {1, 1}),
        ByteBuffer.wrap(new byte[] {1, 1, 1}));
  }

  @Test
  public void testDecimal() {
    assertComparesCorrectly(
        Comparators.forType(Types.DecimalType.of(5, 7)),
        BigDecimal.valueOf(0.1),
        BigDecimal.valueOf(0.2));
  }

  @Test
  public void testList() {
    assertComparesCorrectly(
        Comparators.forType(Types.ListType.ofRequired(18, Types.IntegerType.get())),
        ImmutableList.of(1, 1, 1),
        ImmutableList.of(1, 1, 2));

    assertComparesCorrectly(
        Comparators.forType(Types.ListType.ofRequired(18, Types.IntegerType.get())),
        ImmutableList.of(1, 1),
        ImmutableList.of(1, 1, 1));

    assertComparesCorrectly(
        Comparators.forType(Types.ListType.ofOptional(18, Types.IntegerType.get())),
        Collections.singletonList(null),
        Collections.singletonList(1));
  }

  @Test
  public void testStruct() {
    assertComparesCorrectly(
        Comparators.forType(
            Types.StructType.of(
                Types.NestedField.required(18, "str19", Types.StringType.get()),
                Types.NestedField.required(19, "int19", Types.IntegerType.get()))),
        TestHelpers.Row.of("a", 1),
        TestHelpers.Row.of("a", 2));

    assertComparesCorrectly(
        Comparators.forType(
            Types.StructType.of(Types.NestedField.optional(18, "str19", Types.StringType.get()))),
        TestHelpers.Row.of((String) null),
        TestHelpers.Row.of("a"));
  }
}
