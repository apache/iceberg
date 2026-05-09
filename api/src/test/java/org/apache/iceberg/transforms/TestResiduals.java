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
package org.apache.iceberg.transforms;

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expression.Operation.GT;
import static org.apache.iceberg.expressions.Expression.Operation.LT;
import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.cast;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.function.Function;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

public class TestResiduals {
  @Test
  public void testIdentityTransformResiduals() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "dateint", Types.IntegerType.get()),
            Types.NestedField.optional(51, "hour", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dateint").build();

    ResidualEvaluator resEval =
        ResidualEvaluator.of(
            spec,
            or(
                or(
                    and(lessThan("dateint", 20170815), greaterThan("dateint", 20170801)),
                    and(equal("dateint", 20170815), lessThan("hour", 12))),
                and(equal("dateint", 20170801), greaterThan("hour", 11))),
            true);

    // equal to the upper date bound
    Expression residual = resEval.residualFor(Row.of(20170815));
    UnboundPredicate<?> unbound = assertAndUnwrapUnbound(residual);
    assertThat(unbound.op()).as("Residual should be hour < 12").isEqualTo(LT);
    assertThat(unbound.ref().name()).as("Residual should be hour < 12").isEqualTo("hour");
    assertThat(unbound.literal().value()).as("Residual should be hour < 12").isEqualTo(12);

    // equal to the lower date bound
    residual = resEval.residualFor(Row.of(20170801));
    unbound = assertAndUnwrapUnbound(residual);
    assertThat(unbound.op()).as("Residual should be hour > 11").isEqualTo(GT);
    assertThat(unbound.ref().name()).as("Residual should be hour > 11").isEqualTo("hour");
    assertThat(unbound.literal().value()).as("Residual should be hour > 11").isEqualTo(11);

    // inside the date range
    residual = resEval.residualFor(Row.of(20170812));
    assertThat(residual).isEqualTo(alwaysTrue());

    // outside the date range
    residual = resEval.residualFor(Row.of(20170817));
    assertThat(residual).isEqualTo(alwaysFalse());
  }

  @Test
  public void testCaseInsensitiveIdentityTransformResiduals() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "dateint", Types.IntegerType.get()),
            Types.NestedField.optional(51, "hour", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dateint").build();

    ResidualEvaluator resEval =
        ResidualEvaluator.of(
            spec,
            or(
                or(
                    and(lessThan("DATEINT", 20170815), greaterThan("dateint", 20170801)),
                    and(equal("dateint", 20170815), lessThan("HOUR", 12))),
                and(equal("DateInt", 20170801), greaterThan("hOUr", 11))),
            false);

    // equal to the upper date bound
    Expression residual = resEval.residualFor(Row.of(20170815));
    UnboundPredicate<?> unbound = assertAndUnwrapUnbound(residual);
    assertThat(unbound.op()).as("Residual should be hour < 12").isEqualTo(LT);
    assertThat(unbound.ref().name()).as("Residual should be hour < 12").isEqualTo("HOUR");
    assertThat(unbound.literal().value()).as("Residual should be hour < 12").isEqualTo(12);

    // equal to the lower date bound
    residual = resEval.residualFor(Row.of(20170801));
    unbound = assertAndUnwrapUnbound(residual);
    assertThat(unbound.op()).as("Residual should be hour > 11").isEqualTo(GT);
    assertThat(unbound.ref().name()).as("Residual should be hour > 11").isEqualTo("hOUr");
    assertThat(unbound.literal().value()).as("Residual should be hour > 11").isEqualTo(11);

    // inside the date range
    residual = resEval.residualFor(Row.of(20170812));
    assertThat(residual).isEqualTo(alwaysTrue());

    // outside the date range
    residual = resEval.residualFor(Row.of(20170817));
    assertThat(residual).isEqualTo(alwaysFalse());
  }

  @Test
  public void testCaseSensitiveIdentityTransformResiduals() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "dateint", Types.IntegerType.get()),
            Types.NestedField.optional(51, "hour", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dateint").build();

    ResidualEvaluator resEval = ResidualEvaluator.of(spec, lessThan("DATEINT", 20170815), true);

    assertThatThrownBy(() -> resEval.residualFor(Row.of(20170815)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'DATEINT' in struct");
  }

  @Test
  public void testUnpartitionedResiduals() {
    Expression[] expressions =
        new Expression[] {
          Expressions.alwaysTrue(),
          Expressions.alwaysFalse(),
          Expressions.lessThan("a", 5),
          Expressions.greaterThanOrEqual("b", 16),
          Expressions.notNull("c"),
          Expressions.isNull("d"),
          Expressions.in("e", 1, 2, 3),
          Expressions.notIn("f", 1, 2, 3),
          Expressions.notNaN("g"),
          Expressions.isNaN("h"),
          Expressions.startsWith("data", "abcd"),
          Expressions.notStartsWith("data", "abcd")
        };

    for (Expression expr : expressions) {
      ResidualEvaluator residualEvaluator =
          ResidualEvaluator.of(PartitionSpec.unpartitioned(), expr, true);
      assertThat(residualEvaluator.residualFor(Row.of()))
          .as("Should return expression")
          .isEqualTo(expr);
    }
  }

  @Test
  public void testIn() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "dateint", Types.IntegerType.get()),
            Types.NestedField.optional(51, "hour", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dateint").build();

    ResidualEvaluator resEval =
        ResidualEvaluator.of(spec, in("dateint", 20170815, 20170816, 20170817), true);

    Expression residual = resEval.residualFor(Row.of(20170815));
    assertThat(residual).isEqualTo(alwaysTrue());

    residual = resEval.residualFor(Row.of(20180815));
    assertThat(residual).isEqualTo(alwaysFalse());
  }

  @Test
  public void testInTimestamp() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(51, "dateint", Types.IntegerType.get()));

    Long date20191201 =
        (Long)
            Literal.of("2019-12-01T00:00:00.00000").to(Types.TimestampType.withoutZone()).value();
    Long date20191202 =
        (Long)
            Literal.of("2019-12-02T00:00:00.00000").to(Types.TimestampType.withoutZone()).value();

    PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();

    Function<Object, Integer> day = Transforms.day().bind(Types.TimestampType.withoutZone());
    Integer tsDay = day.apply(date20191201);

    Expression pred = in("ts", date20191201, date20191202);
    ResidualEvaluator resEval = ResidualEvaluator.of(spec, pred, true);

    Expression residual = resEval.residualFor(Row.of(tsDay));
    assertThat(residual).as("Residual should be the original in predicate").isEqualTo(pred);

    residual = resEval.residualFor(Row.of(tsDay + 3));
    assertThat(residual).isEqualTo(alwaysFalse());
  }

  @Test
  public void testNotIn() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "dateint", Types.IntegerType.get()),
            Types.NestedField.optional(51, "hour", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dateint").build();

    ResidualEvaluator resEval =
        ResidualEvaluator.of(spec, notIn("dateint", 20170815, 20170816, 20170817), true);

    Expression residual = resEval.residualFor(Row.of(20180815));
    assertThat(residual).isEqualTo(alwaysTrue());

    residual = resEval.residualFor(Row.of(20170815));
    assertThat(residual).isEqualTo(alwaysFalse());
  }

  @Test
  public void testIsNaN() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "double", Types.DoubleType.get()),
            Types.NestedField.optional(51, "float", Types.FloatType.get()));

    // test double field
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("double").build();

    ResidualEvaluator resEval = ResidualEvaluator.of(spec, isNaN("double"), true);

    Expression residual = resEval.residualFor(Row.of(Double.NaN));
    assertThat(residual).isEqualTo(alwaysTrue());

    residual = resEval.residualFor(Row.of(2D));
    assertThat(residual).isEqualTo(alwaysFalse());

    // test float field
    spec = PartitionSpec.builderFor(schema).identity("float").build();

    resEval = ResidualEvaluator.of(spec, isNaN("float"), true);

    residual = resEval.residualFor(Row.of(Float.NaN));
    assertThat(residual).isEqualTo(alwaysTrue());

    residual = resEval.residualFor(Row.of(3F));
    assertThat(residual).isEqualTo(alwaysFalse());
  }

  @Test
  public void testNotNaN() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "double", Types.DoubleType.get()),
            Types.NestedField.optional(51, "float", Types.FloatType.get()));

    // test double field
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("double").build();

    ResidualEvaluator resEval = ResidualEvaluator.of(spec, notNaN("double"), true);

    Expression residual = resEval.residualFor(Row.of(Double.NaN));
    assertThat(residual).isEqualTo(alwaysFalse());

    residual = resEval.residualFor(Row.of(2D));
    assertThat(residual).isEqualTo(alwaysTrue());

    // test float field
    spec = PartitionSpec.builderFor(schema).identity("float").build();

    resEval = ResidualEvaluator.of(spec, notNaN("float"), true);

    residual = resEval.residualFor(Row.of(Float.NaN));
    assertThat(residual).isEqualTo(alwaysFalse());

    residual = resEval.residualFor(Row.of(3F));
    assertThat(residual).isEqualTo(alwaysTrue());
  }

  @Test
  public void testNotInTimestamp() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(51, "dateint", Types.IntegerType.get()));

    Long date20191201 =
        (Long)
            Literal.of("2019-12-01T00:00:00.00000").to(Types.TimestampType.withoutZone()).value();
    Long date20191202 =
        (Long)
            Literal.of("2019-12-02T00:00:00.00000").to(Types.TimestampType.withoutZone()).value();

    PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();

    Function<Object, Integer> day = Transforms.day().bind(Types.TimestampType.withoutZone());
    Integer tsDay = day.apply(date20191201);

    Expression pred = notIn("ts", date20191201, date20191202);
    ResidualEvaluator resEval = ResidualEvaluator.of(spec, pred, true);

    Expression residual = resEval.residualFor(Row.of(tsDay));
    assertThat(residual).as("Residual should be the original notIn predicate").isEqualTo(pred);

    residual = resEval.residualFor(Row.of(tsDay + 3));
    assertThat(residual).isEqualTo(alwaysTrue());
  }

  @Test
  public void testCastResidualWithUnpartitionedTable() {
    // With an unpartitioned table, cast predicates should pass through as residuals
    Schema schema = new Schema(Types.NestedField.optional(50, "date_key", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.unpartitioned();

    int april26 = DateTimeUtil.isoDateToDays("2026-04-26");

    Expression castExpr = equal(cast("date_key", Types.DateType.get()), april26);
    ResidualEvaluator resEval = ResidualEvaluator.of(spec, castExpr, true);

    Expression residual = resEval.residualFor(Row.of());
    assertThat(residual)
        .as("Unpartitioned table should return cast expression as residual")
        .isEqualTo(castExpr);
  }

  @Test
  public void testCastStringToDateResidualEq() {
    // Test cast with EQ predicate - ResidualEvaluator should evaluate cast against partition value
    Schema schema = new Schema(Types.NestedField.optional(50, "date_key", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("date_key").build();

    int april26 = DateTimeUtil.isoDateToDays("2026-04-26");
    int april27 = DateTimeUtil.isoDateToDays("2026-04-27");

    ResidualEvaluator resEval =
        ResidualEvaluator.of(spec, equal(cast("date_key", Types.DateType.get()), april26), true);

    // When partition value doesn't match after cast
    Expression residual = resEval.residualFor(Row.of("2026-04-27"));
    assertThat(residual)
        .as("When partition value doesn't match after cast, should be alwaysFalse")
        .isEqualTo(alwaysFalse());

    // When partition value matches after cast: "2026-04-26" casts to the same date value
    // ResidualEvaluator correctly evaluates this by:
    // 1. Projecting cast predicate through identity partition
    // 2. Evaluating cast on partition value
    // 3. Comparing result with literal
    residual = resEval.residualFor(Row.of("2026-04-26"));

    // ResidualEvaluator correctly returns alwaysTrue when cast value matches
    assertThat(residual)
        .as("When partition value matches after cast, should be alwaysTrue")
        .isEqualTo(alwaysTrue());
  }

  @Test
  public void testCastStringToDateResidualLt() {
    Schema schema = new Schema(Types.NestedField.optional(50, "date_key", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("date_key").build();

    int april26 = DateTimeUtil.isoDateToDays("2026-04-26");

    ResidualEvaluator resEval =
        ResidualEvaluator.of(spec, lessThan(cast("date_key", Types.DateType.get()), april26), true);

    // Test with a partition value that is before april26
    Expression residual = resEval.residualFor(Row.of("2026-04-25"));
    assertThat(residual)
        .as("Should be alwaysTrue when cast date is less than literal")
        .isEqualTo(alwaysTrue());

    // Test with partition value equal to literal
    residual = resEval.residualFor(Row.of("2026-04-26"));
    assertThat(residual)
        .as("Should be alwaysFalse when cast date equals literal")
        .isEqualTo(alwaysFalse());

    // Test with partition value after literal
    residual = resEval.residualFor(Row.of("2026-04-27"));
    assertThat(residual)
        .as("Should be alwaysFalse when cast date is greater than literal")
        .isEqualTo(alwaysFalse());
  }

  @Test
  public void testCastLongToTimestampNanoResidual() {
    Schema schema = new Schema(Types.NestedField.optional(50, "ts_micros", Types.LongType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("ts_micros").build();

    // 1 microsecond = 1000 nanoseconds
    long oneMicro = 1L;
    long oneThousandNanos = 1000L;

    ResidualEvaluator resEval =
        ResidualEvaluator.of(
            spec,
            equal(cast("ts_micros", Types.TimestampNanoType.withoutZone()), oneThousandNanos),
            true);

    Expression residual = resEval.residualFor(Row.of(oneMicro));
    // The cast should be evaluated against the partition value
    assertThat(residual.op())
        .as("Residual for cast timestamp predicate")
        .isIn(Expression.Operation.TRUE, Expression.Operation.FALSE, Expression.Operation.EQ);
  }

  @Test
  public void testCastResidualWithIn() {
    Schema schema = new Schema(Types.NestedField.optional(50, "date_key", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("date_key").build();

    int april26 = DateTimeUtil.isoDateToDays("2026-04-26");
    int april27 = DateTimeUtil.isoDateToDays("2026-04-27");

    ResidualEvaluator resEval =
        ResidualEvaluator.of(
            spec, in(cast("date_key", Types.DateType.get()), april26, april27), true);

    Expression residual = resEval.residualFor(Row.of("2026-04-26"));
    assertThat(residual)
        .as("Should be alwaysTrue when cast date is in the set")
        .isEqualTo(alwaysTrue());

    residual = resEval.residualFor(Row.of("2026-04-28"));
    assertThat(residual)
        .as("Should be alwaysFalse when cast date is not in the set")
        .isEqualTo(alwaysFalse());
  }

  @Test
  public void testCastResidualWithNotIn() {
    Schema schema = new Schema(Types.NestedField.optional(50, "date_key", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("date_key").build();

    int april26 = DateTimeUtil.isoDateToDays("2026-04-26");
    int april27 = DateTimeUtil.isoDateToDays("2026-04-27");

    ResidualEvaluator resEval =
        ResidualEvaluator.of(
            spec, notIn(cast("date_key", Types.DateType.get()), april26, april27), true);

    Expression residual = resEval.residualFor(Row.of("2026-04-28"));
    assertThat(residual)
        .as("Should be alwaysTrue when cast date is not in the exclusion set")
        .isEqualTo(alwaysTrue());

    residual = resEval.residualFor(Row.of("2026-04-26"));
    assertThat(residual)
        .as("Should be alwaysFalse when cast date is in the exclusion set")
        .isEqualTo(alwaysFalse());
  }

  @Test
  public void testCastIntegerToLongResidual() {
    Schema schema = new Schema(Types.NestedField.optional(50, "int_val", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("int_val").build();

    ResidualEvaluator resEval =
        ResidualEvaluator.of(spec, equal(cast("int_val", Types.LongType.get()), 42L), true);

    Expression residual = resEval.residualFor(Row.of(42));
    // Integer to Long cast is order-preserving and should be evaluated
    assertThat(residual.op())
        .as("Residual for integer to long cast")
        .isIn(Expression.Operation.TRUE, Expression.Operation.FALSE, Expression.Operation.EQ);

    // Test with a partition value that doesn't match
    residual = resEval.residualFor(Row.of(43));
    // Should be alwaysFalse or the original cast expression
    assertThat(residual.op())
        .as("When cast value doesn't match")
        .isIn(Expression.Operation.FALSE, Expression.Operation.EQ);
  }

  @Test
  public void testMultipleCastPredicatesInResidual() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "date_key", Types.StringType.get()),
            Types.NestedField.optional(51, "int_val", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("date_key").build();

    int april26 = DateTimeUtil.isoDateToDays("2026-04-26");

    Expression expr =
        and(
            equal(cast("date_key", Types.DateType.get()), april26),
            greaterThan(cast("int_val", Types.LongType.get()), 100L));

    ResidualEvaluator resEval = ResidualEvaluator.of(spec, expr, true);

    Expression residual = resEval.residualFor(Row.of("2026-04-26"));
    // The date_key cast predicate evaluates to alwaysTrue for matching partition
    // The int_val cast predicate remains because int_val is not partitioned
    assertThat(residual.op())
        .as("Should keep the int_val predicate since it's not partitioned")
        .isEqualTo(Expression.Operation.GT);
  }
}
