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
package org.apache.iceberg.spark.broadcastvar.expressions.transforms;

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;

import java.util.function.Function;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.spark.broadcastvar.expressions.RangeInTestUtils;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;

public class TestResidualsWithRangeIn {

  @Test
  public void testUnpartitionedResiduals() {
    Expression expr = RangeInTestUtils.createPredicate("e", new Object[] {1, 2, 3});
    ResidualEvaluator residualEvaluator =
        ResidualEvaluator.of(PartitionSpec.unpartitioned(), expr, true);
    Assert.assertEquals(
        "Should return expression", expr, residualEvaluator.residualFor(TestHelpers.Row.of()));
  }

  @Test
  public void testRangeIn() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(50, "dateint", Types.IntegerType.get()),
            Types.NestedField.optional(51, "hour", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dateint").build();

    ResidualEvaluator resEval =
        ResidualEvaluator.of(
            spec,
            RangeInTestUtils.createPredicate(
                "dateint", new Object[] {20170815, 20170816, 20170817}),
            true);

    Expression residual = resEval.residualFor(TestHelpers.Row.of(20170815));
    Assert.assertEquals("Residual should be alwaysTrue", alwaysTrue(), residual);

    residual = resEval.residualFor(TestHelpers.Row.of(20180815));
    Assert.assertEquals("Residual should be alwaysFalse", alwaysFalse(), residual);
  }

  @Test
  public void testRangeInTimestamp() {
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

    Expression pred =
        RangeInTestUtils.<Long>createPredicate(
            "ts", new Object[] {date20191201, date20191202}, DataTypes.TimestampType);
    ResidualEvaluator resEval = ResidualEvaluator.of(spec, pred, true);

    Expression residual = resEval.residualFor(TestHelpers.Row.of(tsDay));
    Assert.assertEquals("Residual should be the original in predicate", pred, residual);

    residual = resEval.residualFor(TestHelpers.Row.of(tsDay + 3));
    Assert.assertEquals("Residual should be alwaysFalse", alwaysFalse(), residual);
  }
}
