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

package com.netflix.iceberg.transforms;

import com.netflix.iceberg.Schema;
import com.netflix.iceberg.TestHelpers.Row;
import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.ResidualEvaluator;
import com.netflix.iceberg.expressions.UnboundPredicate;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static com.netflix.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static com.netflix.iceberg.expressions.Expression.Operation.GT;
import static com.netflix.iceberg.expressions.Expression.Operation.LT;
import static com.netflix.iceberg.expressions.Expressions.alwaysFalse;
import static com.netflix.iceberg.expressions.Expressions.alwaysTrue;
import static com.netflix.iceberg.expressions.Expressions.and;
import static com.netflix.iceberg.expressions.Expressions.equal;
import static com.netflix.iceberg.expressions.Expressions.greaterThan;
import static com.netflix.iceberg.expressions.Expressions.lessThan;
import static com.netflix.iceberg.expressions.Expressions.or;

public class TestResiduals {
  @Test
  public void testIdentityTransformResiduals() {
    Schema schema = new Schema(
        Types.NestedField.optional(50, "dateint", Types.IntegerType.get()),
        Types.NestedField.optional(51, "hour", Types.IntegerType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("dateint")
        .build();

    ResidualEvaluator resEval = new ResidualEvaluator(spec, or(or(
        and(lessThan("dateint", 20170815), greaterThan("dateint", 20170801)),
        and(equal("dateint", 20170815), lessThan("hour", 12))),
        and(equal("dateint", 20170801), greaterThan("hour", 11))),
        true
    );

    // equal to the upper date bound
    Expression residual = resEval.residualFor(Row.of(20170815));
    UnboundPredicate<?> unbound = assertAndUnwrapUnbound(residual);
    Assert.assertEquals("Residual should be hour < 12", LT, unbound.op());
    Assert.assertEquals("Residual should be hour < 12", "hour", unbound.ref().name());
    Assert.assertEquals("Residual should be hour < 12", 12, unbound.literal().value());

    // equal to the lower date bound
    residual = resEval.residualFor(Row.of(20170801));
    unbound = assertAndUnwrapUnbound(residual);
    Assert.assertEquals("Residual should be hour > 11", GT, unbound.op());
    Assert.assertEquals("Residual should be hour > 11", "hour", unbound.ref().name());
    Assert.assertEquals("Residual should be hour > 11", 11, unbound.literal().value());

    // inside the date range
    residual = resEval.residualFor(Row.of(20170812));
    Assert.assertEquals("Residual should be alwaysTrue", alwaysTrue(), residual);

    // outside the date range
    residual = resEval.residualFor(Row.of(20170817));
    Assert.assertEquals("Residual should be alwaysFalse", alwaysFalse(), residual);
  }

  @Test
  public void testCaseInsensitiveIdentityTransformResiduals() {
    Schema schema = new Schema(
            Types.NestedField.optional(50, "dateint", Types.IntegerType.get()),
            Types.NestedField.optional(51, "hour", Types.IntegerType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
            .identity("dateint")
            .build();

    ResidualEvaluator resEval = new ResidualEvaluator(spec, or(or(
            and(lessThan("DATEINT", 20170815), greaterThan("dateint", 20170801)),
            and(equal("dateint", 20170815), lessThan("HOUR", 12))),
            and(equal("DateInt", 20170801), greaterThan("hOUr", 11))),
            false
    );

    // equal to the upper date bound
    Expression residual = resEval.residualFor(Row.of(20170815));
    UnboundPredicate<?> unbound = assertAndUnwrapUnbound(residual);
    Assert.assertEquals("Residual should be hour < 12", LT, unbound.op());
    Assert.assertEquals("Residual should be hour < 12", "HOUR", unbound.ref().name());
    Assert.assertEquals("Residual should be hour < 12", 12, unbound.literal().value());

    // equal to the lower date bound
    residual = resEval.residualFor(Row.of(20170801));
    unbound = assertAndUnwrapUnbound(residual);
    Assert.assertEquals("Residual should be hour > 11", GT, unbound.op());
    Assert.assertEquals("Residual should be hour > 11", "hOUr", unbound.ref().name());
    Assert.assertEquals("Residual should be hour > 11", 11, unbound.literal().value());

    // inside the date range
    residual = resEval.residualFor(Row.of(20170812));
    Assert.assertEquals("Residual should be alwaysTrue", alwaysTrue(), residual);

    // outside the date range
    residual = resEval.residualFor(Row.of(20170817));
    Assert.assertEquals("Residual should be alwaysFalse", alwaysFalse(), residual);
  }

  @Test(expected = ValidationException.class)
  public void testCaseSensitiveIdentityTransformResiduals() {
    Schema schema = new Schema(
            Types.NestedField.optional(50, "dateint", Types.IntegerType.get()),
            Types.NestedField.optional(51, "hour", Types.IntegerType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
            .identity("dateint")
            .build();

    ResidualEvaluator resEval = new ResidualEvaluator(spec, lessThan("DATEINT", 20170815),
            true
    );

    resEval.residualFor(Row.of(20170815));
  }
}
