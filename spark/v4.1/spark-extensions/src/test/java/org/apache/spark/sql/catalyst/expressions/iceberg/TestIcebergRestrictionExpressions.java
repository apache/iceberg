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
package org.apache.spark.sql.catalyst.expressions.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.rest.restrictions.Action;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import scala.jdk.javaapi.CollectionConverters;

public class TestIcebergRestrictionExpressions {

  private static Literal str(String value) {
    return Literal.create(UTF8String.fromString(value), StringType$.MODULE$);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static IcebergRestricted restricted(
      org.apache.spark.sql.catalyst.expressions.Expression child, SerializableFunction<?, ?> fn) {
    return new IcebergRestricted(child, (SerializableFunction) fn);
  }

  private static UnsafeProjection codegenProjection(Expression expr) {
    UnsafeProjection proj =
        UnsafeProjection.create(
            CollectionConverters.asScala(Arrays.<Expression>asList(expr)).toSeq());
    // UnsafeProjection.create falls back to interpretation only if codegen compile fails;
    // asserting the fallback wasn't used verifies the generated Java source is valid.
    assertThat(proj.getClass().getSimpleName()).doesNotContain("Interpreted");
    return proj;
  }

  @Test
  public void prettyNameIsOpaque() {
    SerializableFunction<?, ?> fn = new Action.MaskAlphanum(1).bind(Types.StringType.get());
    IcebergRestricted expr = restricted(str("anything"), fn);
    assertThat(expr.prettyName()).isEqualTo("iceberg_restricted");
  }

  @Test
  public void stringTypeConversionRoundTrip() {
    SerializableFunction<?, ?> fn = new Action.MaskAlphanum(1).bind(Types.StringType.get());
    IcebergRestricted expr = restricted(str("prashant010696@gmail.com"), fn);
    Object result = expr.eval(InternalRow.empty());
    assertThat(result).isInstanceOf(UTF8String.class);
    assertThat(result.toString()).isEqualTo("xxxxxxxxnnnnnn@xxxxx.xxx");
  }

  @Test
  public void binaryTypeConversionRoundTrip() {
    SerializableFunction<?, ?> fn = new Action.Sha256Global(1).bind(Types.BinaryType.get());
    IcebergRestricted expr =
        restricted(Literal.create(new byte[] {1, 2, 3}, BinaryType$.MODULE$), fn);
    Object result = expr.eval(InternalRow.empty());
    assertThat(result).isInstanceOf(byte[].class);
    assertThat((byte[]) result).hasSize(32);
  }

  @Test
  public void decimalTypeConversionRoundTrip() {
    SerializableFunction<?, ?> fn = new Action.MaskToDefault(1).bind(Types.DecimalType.of(10, 2));
    DecimalType sparkType = DecimalType.apply(10, 2);
    IcebergRestricted expr = restricted(Literal.create(Decimal.apply(1234, 10, 2), sparkType), fn);
    Object result = expr.eval(InternalRow.empty());
    assertThat(result).isInstanceOf(Decimal.class);
    assertThat(((Decimal) result).toBigDecimal().bigDecimal().unscaledValue().longValueExact())
        .isEqualTo(0L);
  }

  @Test
  public void nullInputReturnsNull() {
    SerializableFunction<?, ?> fn = new Action.MaskAlphanum(1).bind(Types.StringType.get());
    IcebergRestricted expr = restricted(Literal.create(null, StringType$.MODULE$), fn);
    assertThat(expr.eval(InternalRow.empty())).isNull();
  }

  @Test
  public void replaceWithNullAlwaysReturnsNull() {
    SerializableFunction<?, ?> fn = new Action.ReplaceWithNull(1).bind(Types.IntegerType.get());
    IcebergRestricted expr = restricted(Literal.create(42, DataTypes.IntegerType), fn);
    assertThat(expr.eval(InternalRow.empty())).isNull();
  }

  @Test
  public void applyExpressionFailsOnEval() {
    SerializableFunction<?, ?> fn =
        new Action.ApplyExpression(1, Expressions.alwaysTrue()).bind(Types.StringType.get());
    IcebergRestricted expr = restricted(str("any"), fn);
    assertThatThrownBy(() -> expr.eval(InternalRow.empty()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("apply-expression");
  }

  @Test
  public void maskToDefaultIntThroughSpark() {
    SerializableFunction<?, ?> fn = new Action.MaskToDefault(1).bind(Types.IntegerType.get());
    IcebergRestricted expr = restricted(Literal.create(42, DataTypes.IntegerType), fn);
    assertThat(expr.eval(InternalRow.empty())).isEqualTo(999999999);
  }

  @Test
  public void sha256GlobalStringThroughSpark() {
    SerializableFunction<?, ?> fn = new Action.Sha256Global(1).bind(Types.StringType.get());
    IcebergRestricted expr = restricted(str("hello"), fn);
    Object result = expr.eval(InternalRow.empty());
    assertThat(result).isInstanceOf(UTF8String.class);
    assertThat(result.toString())
        .isEqualTo("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
  }

  @Test
  public void rowFilterExprHidesChildInPlan() {
    IcebergRowFilterExpr filter =
        new IcebergRowFilterExpr(Literal.create(true, DataTypes.BooleanType));
    assertThat(filter.sql()).isEqualTo("iceberg_row_filter()");
    assertThat(filter.toString()).isEqualTo("iceberg_row_filter()");
  }

  @Test
  public void rowFilterExprPassesThroughValue() {
    IcebergRowFilterExpr filter =
        new IcebergRowFilterExpr(Literal.create(true, DataTypes.BooleanType));
    assertThat(filter.eval(InternalRow.empty())).isEqualTo(true);
  }

  @Test
  public void codegenStringMaskMatchesEval() {
    SerializableFunction<?, ?> fn = new Action.MaskAlphanum(1).bind(Types.StringType.get());
    IcebergRestricted expr = restricted(str("prashant010696@gmail.com"), fn);
    InternalRow result = codegenProjection(expr).apply(InternalRow.empty());
    assertThat(result.getUTF8String(0).toString()).isEqualTo("xxxxxxxxnnnnnn@xxxxx.xxx");
  }

  @Test
  public void codegenIntegerMaskMatchesEval() {
    SerializableFunction<?, ?> fn = new Action.MaskToDefault(1).bind(Types.IntegerType.get());
    IcebergRestricted expr = restricted(Literal.create(42, DataTypes.IntegerType), fn);
    InternalRow result = codegenProjection(expr).apply(InternalRow.empty());
    assertThat(result.getInt(0)).isEqualTo(999999999);
  }

  @Test
  public void codegenLongMaskMatchesEval() {
    SerializableFunction<?, ?> fn = new Action.MaskToDefault(1).bind(Types.LongType.get());
    IcebergRestricted expr = restricted(Literal.create(42L, DataTypes.LongType), fn);
    InternalRow result = codegenProjection(expr).apply(InternalRow.empty());
    assertThat(result.getLong(0)).isEqualTo(999999999L);
  }

  @Test
  public void codegenBinaryMaskMatchesEval() {
    SerializableFunction<?, ?> fn = new Action.Sha256Global(1).bind(Types.BinaryType.get());
    IcebergRestricted expr =
        restricted(Literal.create(new byte[] {1, 2, 3}, BinaryType$.MODULE$), fn);
    InternalRow result = codegenProjection(expr).apply(InternalRow.empty());
    assertThat(result.getBinary(0)).hasSize(32);
  }

  @Test
  public void codegenDecimalMaskMatchesEval() {
    SerializableFunction<?, ?> fn = new Action.MaskToDefault(1).bind(Types.DecimalType.of(10, 2));
    DecimalType sparkType = DecimalType.apply(10, 2);
    IcebergRestricted expr = restricted(Literal.create(Decimal.apply(1234, 10, 2), sparkType), fn);
    InternalRow result = codegenProjection(expr).apply(InternalRow.empty());
    assertThat(
            result
                .getDecimal(0, 10, 2)
                .toBigDecimal()
                .bigDecimal()
                .unscaledValue()
                .longValueExact())
        .isEqualTo(0L);
  }

  @Test
  public void codegenNullInputReturnsNull() {
    SerializableFunction<?, ?> fn = new Action.MaskAlphanum(1).bind(Types.StringType.get());
    IcebergRestricted expr = restricted(Literal.create(null, StringType$.MODULE$), fn);
    InternalRow result = codegenProjection(expr).apply(InternalRow.empty());
    assertThat(result.isNullAt(0)).isTrue();
  }

  @Test
  public void codegenRowFilterDelegatesToChild() {
    IcebergRowFilterExpr filter =
        new IcebergRowFilterExpr(Literal.create(true, DataTypes.BooleanType));
    InternalRow result = codegenProjection(filter).apply(InternalRow.empty());
    assertThat(result.getBoolean(0)).isTrue();
  }
}
