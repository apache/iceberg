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
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.assertj.core.api.Assertions.assertThat;

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
import org.assertj.core.api.Assertions;
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

    Assertions.assertThatThrownBy(() -> resEval.residualFor(Row.of(20170815)))
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
            Types.NestedField.optional(50, "ts", Types.TimestampType.microsWithoutZone()),
            Types.NestedField.optional(51, "dateint", Types.IntegerType.get()));

    Long date20191201 =
        (Long)
            Literal.of("2019-12-01T00:00:00.00000")
                .to(Types.TimestampType.microsWithoutZone())
                .value();
    Long date20191202 =
        (Long)
            Literal.of("2019-12-02T00:00:00.00000")
                .to(Types.TimestampType.microsWithoutZone())
                .value();

    PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();

    Function<Object, Integer> day = Transforms.day().bind(Types.TimestampType.microsWithoutZone());
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
            Types.NestedField.optional(50, "ts", Types.TimestampType.microsWithoutZone()),
            Types.NestedField.optional(51, "dateint", Types.IntegerType.get()));

    Long date20191201 =
        (Long)
            Literal.of("2019-12-01T00:00:00.00000")
                .to(Types.TimestampType.microsWithoutZone())
                .value();
    Long date20191202 =
        (Long)
            Literal.of("2019-12-02T00:00:00.00000")
                .to(Types.TimestampType.microsWithoutZone())
                .value();

    PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();

    Function<Object, Integer> day = Transforms.day().bind(Types.TimestampType.microsWithoutZone());
    Integer tsDay = day.apply(date20191201);

    Expression pred = notIn("ts", date20191201, date20191202);
    ResidualEvaluator resEval = ResidualEvaluator.of(spec, pred, true);

    Expression residual = resEval.residualFor(Row.of(tsDay));
    assertThat(residual).as("Residual should be the original notIn predicate").isEqualTo(pred);

    residual = resEval.residualFor(Row.of(tsDay + 3));
    assertThat(residual).isEqualTo(alwaysTrue());
  }
}
