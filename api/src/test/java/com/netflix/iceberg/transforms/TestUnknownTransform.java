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

import static com.netflix.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static com.netflix.iceberg.expressions.Expressions.greaterThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.lessThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.or;
import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;

import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.Or;
import com.netflix.iceberg.expressions.Projections;
import com.netflix.iceberg.expressions.UnboundPredicate;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestUnknownTransform {

  @Test
  public void testUnknownTransform() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        optional(2, "before", Types.DateType.get()),
        required(3, "after", Types.DateType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .day("before")
        .add(3, "after_unknown_transform", "unknown")
        .build();

    Expression filter = or(lessThanOrEqual("before", "2018-12-12"),
        greaterThanOrEqual("after", "2017-01-01"));
    Expression projection = Projections.inclusive(spec).project(filter);

    Assert.assertTrue(projection instanceof Or);
    Or or = (Or) projection;
    UnboundPredicate<?> left = assertAndUnwrapUnbound(or.left());
    Assert.assertEquals("Should be a before_day predicate", "before_day", left.ref().name());
    Assert.assertEquals("Should be 429060", 17877, left.literal().value());
    UnboundPredicate<?> right = assertAndUnwrapUnbound(or.right());
    Assert.assertEquals("Should be a after_unknown_transform predicate", "after_unknown_transform", right.ref().name());
    Assert.assertEquals("Should be always true", Expressions.alwaysTrue().op(), right.op());
  }
}
