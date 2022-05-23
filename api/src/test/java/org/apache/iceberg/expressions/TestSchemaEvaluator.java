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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSchemaEvaluator {
  private static final Schema OLD_SCHEMA = new Schema(
      required(1, "int_col", Types.IntegerType.get()),
      optional(2, "double_col", Types.DoubleType.get()),
      optional(3, "str_col", Types.StringType.get())
  );

  private static final Schema NEW_SCHEMA = new Schema(
      required(1, "int_col", Types.IntegerType.get()),
      optional(2, "double_col", Types.DoubleType.get()),
      optional(3, "str_col", Types.StringType.get()),
      optional(4, "new_int_col", Types.IntegerType.get()),
      optional(5, "new_double_col", Types.DoubleType.get()),
      optional(6, "new_str_col", Types.StringType.get()),
      optional(7, "struct_col", Types.StructType.of(
          Types.NestedField.optional(8, "nest_col1", Types.StructType.of(
              Types.NestedField.optional(9, "nest_col2", Types.FloatType.get())))))
  );

  @Test
  public void testLessThan() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), lessThan("int_col", 7));
    Assert.assertTrue("existed int_col could be less than 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), lessThan("double_col", 7));
    Assert.assertTrue("existed double_col could be less than 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), lessThan("new_int_col", 7));
    Assert.assertFalse("not existed new_int_col could not be less than 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), lessThan("new_double_col", 7));
    Assert.assertFalse("not existed new_double_col could not be less than 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), lessThan("struct_col.nest_col1.nest_col2", 7));
    Assert.assertFalse(
        "not existed struct_col.nest_col1.nest_col2 could not be less than 7", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testLessThanOrEqual() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), lessThanOrEqual("int_col", 7));
    Assert.assertTrue("existed int_col could be less than or equal 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), lessThanOrEqual("double_col", 7));
    Assert.assertTrue("existed double_col could be less than or equal 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), lessThanOrEqual("new_int_col", 7));
    Assert.assertFalse("not existed new_int_col could not be less than or equal 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), lessThanOrEqual("new_double_col", 7));
    Assert.assertFalse("not existed new_double_col could not be less than or equal 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), lessThanOrEqual("struct_col.nest_col1.nest_col2", 7));
    Assert.assertFalse(
        "not existed struct_col.nest_col1.nest_col2 could not be less than or equal 7", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testGreaterThan() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), greaterThan("int_col", 7));
    Assert.assertTrue("existed int_col could be greater than 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), greaterThan("double_col", 7));
    Assert.assertTrue("existed double_col could be greater than 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), greaterThan("new_int_col", 7));
    Assert.assertFalse("not existed new_int_col could not be greater than 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), greaterThan("new_double_col", 7));
    Assert.assertFalse("not existed new_double_col could not be greater than 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), greaterThan("struct_col.nest_col1.nest_col2", 7));
    Assert.assertFalse(
        "not existed struct_col.nest_col1.nest_col2 could not be greater than 7", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testGreaterThanOrEqual() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), greaterThanOrEqual("int_col", 7));
    Assert.assertTrue("existed int_col could be greater than or equal 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), greaterThanOrEqual("double_col", 7));
    Assert.assertTrue("existed double_col could be greater than or equal 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), greaterThanOrEqual("new_int_col", 7));
    Assert.assertFalse("not existed new_int_col could not be greater than or equal 7t", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), greaterThanOrEqual("new_double_col", 7));
    Assert.assertFalse("not existed new_double_col could not be greater than or equal 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), greaterThanOrEqual("struct_col.nest_col1.nest_col2", 7));
    Assert.assertFalse(
        "not existed struct_col.nest_col1.nest_col2 could not be greater than or equal 7", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testEqual() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), equal("int_col", 7));
    Assert.assertTrue("existed int_col could be equal to 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), equal("double_col", 7));
    Assert.assertTrue("existed double_col could be equal to 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), equal("str_col", "abc"));
    Assert.assertTrue("existed str_col could be equal to abc", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), equal("new_int_col", 7));
    Assert.assertFalse("not existed new_int_col could not be equal to 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), equal("new_double_col", 7));
    Assert.assertFalse("not existed new_double_col could not be equal to 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), equal("new_str_col", "abc"));
    Assert.assertFalse("not existed new_str_col could not be equal to abc", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), equal("struct_col.nest_col1.nest_col2", 7));
    Assert.assertFalse(
        "not existed struct_col.nest_col1.nest_col2 could not be equal to 7", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testNotEqual() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notEqual("int_col", 7));
    Assert.assertTrue("existed int_col could be not equal to 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notEqual("double_col", 7));
    Assert.assertTrue("existed double_col could be not equal to 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notEqual("str_col", "abc"));
    Assert.assertTrue("existed str_col could be not equal to abc", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notEqual("new_int_col", 7));
    Assert.assertTrue("not existed new_int_col should be not equal to 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notEqual("new_double_col", 7));
    Assert.assertTrue("not existed new_double_col should be not equal to 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notEqual("new_str_col", "abc"));
    Assert.assertTrue("not existed new_str_col should be not equal to abc", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notEqual("struct_col.nest_col1.nest_col2", 7));
    Assert.assertTrue(
        "not existed struct_col.nest_col1.nest_col2 should be not equal to 7", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testStartWith() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), startsWith("str_col", "abc"));
    Assert.assertTrue("existed str_col could be start with abc", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), startsWith("new_str_col", "abc"));
    Assert.assertFalse("not existed new_str_col could not be start with abc", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testNotStartWith() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notStartsWith("str_col", "abc"));
    Assert.assertTrue("existed str_col could be not start with abc", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notStartsWith("new_str_col", "abc"));
    Assert.assertTrue("not existed new_str_col should not start with exists", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testAlwaysTrue() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), alwaysTrue());
    Assert.assertTrue("always true", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testAlwaysFalse() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), alwaysFalse());
    Assert.assertFalse("always false", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testIsNull() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), isNull("double_col"));
    Assert.assertTrue("existed double_col could be null ", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), isNull("str_col"));
    Assert.assertTrue("existed str_col could be null", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), isNull("new_double_col"));
    Assert.assertTrue("not existed new_double_col col is null", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), isNull("new_str_col"));
    Assert.assertTrue("not existed new_str_col col is null", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), isNull("struct_col.nest_col1.nest_col2"));
    Assert.assertTrue("not existed struct_col.nest_col1.nest_col2 col is null", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testNotNull() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notNull("double_col"));
    Assert.assertTrue("existed double_col could be not null", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notNull("str_col"));
    Assert.assertTrue("existed str_col could be not null", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notNull("new_double_col"));
    Assert.assertFalse("not existed new_double_col could not be not null", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notNull("new_str_col"));
    Assert.assertFalse("not existed new_str_col could not be not null", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notNull("struct_col.nest_col1.nest_col2"));
    Assert.assertFalse("not existed struct_col.nest_col1.nest_col2 could not be not null", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testIsNaN() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), isNaN("double_col"));
    Assert.assertTrue("existed double_col could be NaN ", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), isNaN("new_double_col"));
    Assert.assertFalse("not existed new_double_col could not be NaN ", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testNotNaN() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notNaN("double_col"));
    Assert.assertTrue("existed double_col could be not NaN ", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notNaN("new_double_col"));
    Assert.assertTrue("not existed new_double_col should not be NaN ", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testIn() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), in("int_col", 5, 6, 7));
    Assert.assertTrue("existed int_col could be in 5, 6, 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), in("double_col", 5, 6, 7));
    Assert.assertTrue("existed double_col could be in 5, 6, 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), in("new_int_col", 5, 6, 7));
    Assert.assertFalse("not existed new_int_col could not be in 5, 6, 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), in("new_double_col", 5, 6, 7));
    Assert.assertFalse("not existed new_double_col could not be in 5, 6, 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), in("struct_col.nest_col1.nest_col2", 5, 6, 7));
    Assert.assertFalse(
        "not existed struct_col.nest_col1.nest_col2 could not be in 5, 6, 7", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testNotIn() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notIn("int_col", 5, 6, 7));
    Assert.assertTrue("existed int_col could be not in 5, 6, 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notIn("double_col", 5, 6, 7));
    Assert.assertTrue("existed double_col could be not in 5, 6, 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notIn("new_int_col", 5, 6, 7));
    Assert.assertTrue("not existed new_int_col could be not in 5, 6, 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notIn("new_double_col", 5, 6, 7));
    Assert.assertTrue("not existed new_double_col could be not in 5, 6, 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), notIn("struct_col.nest_col1.nest_col2", 5, 6, 7));
    Assert.assertTrue(
        "not existed struct_col.nest_col1.nest_col2 could be not in 5, 6, 7", evaluator.eval(OLD_SCHEMA));
  }


  @Test
  public void testAnd() {
    SchemaEvaluator evaluator = new SchemaEvaluator(
        NEW_SCHEMA.asStruct(), and(isNull("double_col"), startsWith("str_col", "abc")));
    Assert.assertTrue(
        "existed double_col could be null and existed str_col could be start with abc", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(
        NEW_SCHEMA.asStruct(), and(isNull("new_double_col"), startsWith("new_str_col", "abc")));
    Assert.assertFalse(
        "not existed double_col is null and not existed str_col should not be start with abc",
        evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testOr() {
    SchemaEvaluator evaluator = new SchemaEvaluator(
        NEW_SCHEMA.asStruct(), or(isNull("double_col"), startsWith("str_col", "abc")));
    Assert.assertTrue(
        "existed double_col could be null or existed str_col could be start with abc", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(
        NEW_SCHEMA.asStruct(), or(isNull("new_double_col"), startsWith("new_str_col", "abc")));
    Assert.assertTrue(
        "not existed double_col is null or not existed str_col should not be start with abc",
        evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testNot() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), not(lessThan("int_col", 7)));
    Assert.assertTrue("existed int_col could be not less than 7", evaluator.eval(OLD_SCHEMA));

    evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), not(lessThan("new_int_col", 7)));
    Assert.assertFalse("not existed new_int_col could not be not less than 7", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testCaseInsensitive() {
    SchemaEvaluator evaluator = new SchemaEvaluator(NEW_SCHEMA.asStruct(), startsWith("STR_COL", "abc"), false);
    Assert.assertTrue("existed str_col could be start with abc", evaluator.eval(OLD_SCHEMA));
  }

  @Test
  public void testCaseSensitive() {
    AssertHelpers.assertThrows(
        "INT_COL != int_col when case sensitivity is on",
        ValidationException.class,
        "Cannot find field 'INT_COL' in struct",
        () -> new SchemaEvaluator(NEW_SCHEMA.asStruct(), not(lessThan("INT_COL", 7)), true)
    );
  }
}
