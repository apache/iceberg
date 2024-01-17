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
package org.apache.iceberg.spark.broadcastvar.expressions;

import static org.apache.iceberg.TestHelpers.assertAndUnwrapBoundSet;
import static org.apache.iceberg.expressions.Expression.Operation.RANGE_IN;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.stream.Collectors;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.spark.source.broadcastvar.BroadcastHRUnboundPredicate;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class TestPredicateBindingWithRangeIn {

  @Test
  public void testRangeInPredicateBinding() {
    StructType struct =
        StructType.of(
            required(10, "x", Types.IntegerType.get()),
            required(11, "y", Types.IntegerType.get()),
            required(12, "z", Types.IntegerType.get()));

    BroadcastHRUnboundPredicate<Integer> unbound =
        RangeInTestUtils.createPredicate("y", new Object[] {6, 7, 11});

    Expression expr = unbound.bind(struct, true);
    BoundSetPredicate<Integer> bound = assertAndUnwrapBoundSet(expr);

    Assert.assertEquals("Should reference correct field ID", 11, bound.ref().fieldId());
    Assert.assertEquals("Should not change the RANGE IN operation", RANGE_IN, bound.op());
    Assert.assertArrayEquals(
        "Should not alter literal set values",
        new Integer[] {6, 7, 11},
        bound.literalSet().stream().sorted().collect(Collectors.toList()).toArray(new Integer[2]));
  }
}
