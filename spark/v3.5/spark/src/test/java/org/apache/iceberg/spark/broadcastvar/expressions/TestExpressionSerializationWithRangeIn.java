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

import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.BaseExpressionSerialization;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;

public class TestExpressionSerializationWithRangeIn extends BaseExpressionSerialization {

  @Test
  public void testExpressions() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.optional(34, "a", Types.IntegerType.get()),
            Types.NestedField.required(35, "s", Types.StringType.get()));

    Expression[] expressions =
        new Expression[] {
          RangeInTestUtils.<String>createPredicate(
              "col", new Object[] {"a", "b"}, DataTypes.StringType),
          RangeInTestUtils.createPredicate("a", new Object[] {5, 6, 7})
              .bind(schema.asStruct(), false)
        };

    for (Expression expression : expressions) {
      Expression copy = TestHelpers.roundTripSerialize(expression);
      Assert.assertTrue(
          "Expression should equal the deserialized copy: " + expression + " != " + copy,
          equals(expression, copy));
    }
  }
}
