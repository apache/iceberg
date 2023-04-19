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

import java.math.BigDecimal;
import java.util.stream.IntStream;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestNumericLiteralConversions {
  @Test
  public void testIntegerToLongConversion() {
    Literal<Integer> lit = Literal.of(34);
    Literal<Long> longLit = lit.to(Types.LongType.get());

    Assert.assertEquals("Value should match", 34L, (long) longLit.value());
  }

  @Test
  public void testIntegerToFloatConversion() {
    Literal<Integer> lit = Literal.of(34);
    Literal<Float> floatLit = lit.to(Types.FloatType.get());

    Assert.assertEquals("Value should match", 34.0F, floatLit.value(), 0.0000000001D);
  }

  @Test
  public void testIntegerToDoubleConversion() {
    Literal<Integer> lit = Literal.of(34);
    Literal<Double> doubleLit = lit.to(Types.DoubleType.get());

    Assert.assertEquals("Value should match", 34.0D, doubleLit.value(), 0.0000000001D);
  }

  @Test
  public void testIntegerToDecimalConversion() {
    Literal<Integer> lit = Literal.of(34);

    Assert.assertEquals(
        "Value should match", new BigDecimal("34"), lit.to(Types.DecimalType.of(9, 0)).value());
    Assert.assertEquals(
        "Value should match", new BigDecimal("34.00"), lit.to(Types.DecimalType.of(9, 2)).value());
    Assert.assertEquals(
        "Value should match",
        new BigDecimal("34.0000"),
        lit.to(Types.DecimalType.of(9, 4)).value());
  }

  @Test
  public void testLongToIntegerConversion() {
    Literal<Long> lit = Literal.of(34L);
    Literal<Integer> intLit = lit.to(Types.IntegerType.get());

    Assert.assertEquals("Value should match", 34, (int) intLit.value());

    Assert.assertEquals(
        "Values above Integer.MAX_VALUE should be Literals.aboveMax()",
        Literals.aboveMax(),
        Literal.of((long) Integer.MAX_VALUE + 1L).to(Types.IntegerType.get()));
    Assert.assertEquals(
        "Values below Integer.MIN_VALUE should be Literals.belowMin()",
        Literals.belowMin(),
        Literal.of((long) Integer.MIN_VALUE - 1L).to(Types.IntegerType.get()));
  }

  @Test
  public void testLongToFloatConversion() {
    Literal<Long> lit = Literal.of(34L);
    Literal<Float> floatLit = lit.to(Types.FloatType.get());

    Assert.assertEquals("Value should match", 34.0F, floatLit.value(), 0.0000000001D);
  }

  @Test
  public void testLongToDoubleConversion() {
    Literal<Long> lit = Literal.of(34L);
    Literal<Double> doubleLit = lit.to(Types.DoubleType.get());

    Assert.assertEquals("Value should match", 34.0D, doubleLit.value(), 0.0000000001D);
  }

  @Test
  public void testLongToDecimalConversion() {
    Literal<Long> lit = Literal.of(34L);

    Assert.assertEquals(
        "Value should match", new BigDecimal("34"), lit.to(Types.DecimalType.of(9, 0)).value());
    Assert.assertEquals(
        "Value should match", new BigDecimal("34.00"), lit.to(Types.DecimalType.of(9, 2)).value());
    Assert.assertEquals(
        "Value should match",
        new BigDecimal("34.0000"),
        lit.to(Types.DecimalType.of(9, 4)).value());
  }

  @Test
  public void testFloatToDoubleConversion() {
    Literal<Float> lit = Literal.of(34.56F);
    Literal<Double> doubleLit = lit.to(Types.DoubleType.get());

    Assert.assertEquals("Value should match", 34.56D, doubleLit.value(), 0.001D);
  }

  @Test
  public void testFloatToDecimalConversion() {
    Literal<Float> lit = Literal.of(34.56F);

    Assert.assertEquals(
        "Value should round using HALF_UP",
        new BigDecimal("34.6"),
        lit.to(Types.DecimalType.of(9, 1)).value());
    Assert.assertEquals(
        "Value should match", new BigDecimal("34.56"), lit.to(Types.DecimalType.of(9, 2)).value());
    Assert.assertEquals(
        "Value should match",
        new BigDecimal("34.5600"),
        lit.to(Types.DecimalType.of(9, 4)).value());
  }

  @Test
  public void testDoubleToFloatConversion() {
    Literal<Double> lit = Literal.of(34.56D);
    Literal<Float> doubleLit = lit.to(Types.FloatType.get());

    Assert.assertEquals("Value should match", 34.56F, doubleLit.value(), 0.001D);

    // this adjusts Float.MAX_VALUE using multipliers because most integer adjustments are lost by
    // floating point precision.
    Assert.assertEquals(
        "Values above Float.MAX_VALUE should be Literals.aboveMax()",
        Literals.aboveMax(),
        Literal.of(2 * ((double) Float.MAX_VALUE)).to(Types.FloatType.get()));
    Assert.assertEquals(
        "Values below Float.MIN_VALUE should be Literals.belowMin()",
        Literals.belowMin(),
        Literal.of(-2 * ((double) Float.MAX_VALUE)).to(Types.FloatType.get()));
  }

  @Test
  public void testDoubleToDecimalConversion() {
    Literal<Double> lit = Literal.of(34.56D);

    Assert.assertEquals(
        "Value should round using HALF_UP",
        new BigDecimal("34.6"),
        lit.to(Types.DecimalType.of(9, 1)).value());
    Assert.assertEquals(
        "Value should match", new BigDecimal("34.56"), lit.to(Types.DecimalType.of(9, 2)).value());
    Assert.assertEquals(
        "Value should match",
        new BigDecimal("34.5600"),
        lit.to(Types.DecimalType.of(9, 4)).value());
  }

  @Test
  public void testDecimalToDecimalConversion() {
    Literal<BigDecimal> lit = Literal.of(new BigDecimal("34.11"));

    IntStream.range(0, 10)
        .forEach(
            scale -> {
              Assert.assertSame(
                  "Should return identical object", lit, lit.to(Types.DecimalType.of(9, scale)));
              Assert.assertSame(
                  "Should return identical object", lit, lit.to(Types.DecimalType.of(11, scale)));
            });
  }

  @Test
  public void testIntegerToDateConversion() {
    Literal<Integer> lit = Literal.of(0);
    Assert.assertEquals(
        "Dates should be equal", lit.to(Types.DateType.get()), new Literals.DateLiteral(0));
    lit = Literal.of(365 * 50);
    Assert.assertEquals(
        "Dates should be equal", lit.to(Types.DateType.get()), new Literals.DateLiteral(365 * 50));
  }

  @Test
  public void testLongToDateConversion() {
    Literal<Long> lit = Literal.of(0L);
    Assert.assertEquals(
        "Dates should be equal", lit.to(Types.DateType.get()), new Literals.DateLiteral(0));
    lit = Literal.of(365L * 50);
    Assert.assertEquals(
        "Dates should be equal", lit.to(Types.DateType.get()), new Literals.DateLiteral(365 * 50));
  }
}
