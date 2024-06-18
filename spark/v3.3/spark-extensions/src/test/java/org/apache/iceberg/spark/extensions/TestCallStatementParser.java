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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.parser.extensions.IcebergParseException;
import org.apache.spark.sql.catalyst.plans.logical.CallArgument;
import org.apache.spark.sql.catalyst.plans.logical.CallStatement;
import org.apache.spark.sql.catalyst.plans.logical.NamedArgument;
import org.apache.spark.sql.catalyst.plans.logical.PositionalArgument;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.collection.JavaConverters;

public class TestCallStatementParser {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;
  private static ParserInterface parser = null;

  @BeforeClass
  public static void startSpark() {
    TestCallStatementParser.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.extra.prop", "value")
            .getOrCreate();
    TestCallStatementParser.parser = spark.sessionState().sqlParser();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestCallStatementParser.spark;
    TestCallStatementParser.spark = null;
    TestCallStatementParser.parser = null;
    currentSpark.stop();
  }

  @Test
  public void testCallWithPositionalArgs() throws ParseException {
    CallStatement call =
        (CallStatement) parser.parsePlan("CALL c.n.func(1, '2', 3L, true, 1.0D, 9.0e1, 900e-1BD)");
    Assert.assertEquals(
        ImmutableList.of("c", "n", "func"), JavaConverters.seqAsJavaList(call.name()));

    Assert.assertEquals(7, call.args().size());

    checkArg(call, 0, 1, DataTypes.IntegerType);
    checkArg(call, 1, "2", DataTypes.StringType);
    checkArg(call, 2, 3L, DataTypes.LongType);
    checkArg(call, 3, true, DataTypes.BooleanType);
    checkArg(call, 4, 1.0D, DataTypes.DoubleType);
    checkArg(call, 5, 9.0e1, DataTypes.DoubleType);
    checkArg(call, 6, new BigDecimal("900e-1"), DataTypes.createDecimalType(3, 1));
  }

  @Test
  public void testCallWithNamedArgs() throws ParseException {
    CallStatement call =
        (CallStatement) parser.parsePlan("CALL cat.system.func(c1 => 1, c2 => '2', c3 => true)");
    Assert.assertEquals(
        ImmutableList.of("cat", "system", "func"), JavaConverters.seqAsJavaList(call.name()));

    Assert.assertEquals(3, call.args().size());

    checkArg(call, 0, "c1", 1, DataTypes.IntegerType);
    checkArg(call, 1, "c2", "2", DataTypes.StringType);
    checkArg(call, 2, "c3", true, DataTypes.BooleanType);
  }

  @Test
  public void testCallWithMixedArgs() throws ParseException {
    CallStatement call = (CallStatement) parser.parsePlan("CALL cat.system.func(c1 => 1, '2')");
    Assert.assertEquals(
        ImmutableList.of("cat", "system", "func"), JavaConverters.seqAsJavaList(call.name()));

    Assert.assertEquals(2, call.args().size());

    checkArg(call, 0, "c1", 1, DataTypes.IntegerType);
    checkArg(call, 1, "2", DataTypes.StringType);
  }

  @Test
  public void testCallWithTimestampArg() throws ParseException {
    CallStatement call =
        (CallStatement)
            parser.parsePlan("CALL cat.system.func(TIMESTAMP '2017-02-03T10:37:30.00Z')");
    Assert.assertEquals(
        ImmutableList.of("cat", "system", "func"), JavaConverters.seqAsJavaList(call.name()));

    Assert.assertEquals(1, call.args().size());

    checkArg(
        call, 0, Timestamp.from(Instant.parse("2017-02-03T10:37:30.00Z")), DataTypes.TimestampType);
  }

  @Test
  public void testCallWithVarSubstitution() throws ParseException {
    CallStatement call =
        (CallStatement) parser.parsePlan("CALL cat.system.func('${spark.extra.prop}')");
    Assert.assertEquals(
        ImmutableList.of("cat", "system", "func"), JavaConverters.seqAsJavaList(call.name()));

    Assert.assertEquals(1, call.args().size());

    checkArg(call, 0, "value", DataTypes.StringType);
  }

  @Test
  public void testCallParseError() {
    assertThatThrownBy(() -> parser.parsePlan("CALL cat.system radish kebab"))
        .as("Should fail with a sensible parse error")
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("missing '(' at 'radish'");
  }

  @Test
  public void testCallStripsComments() throws ParseException {
    List<String> callStatementsWithComments =
        Lists.newArrayList(
            "/* bracketed comment */  CALL cat.system.func('${spark.extra.prop}')",
            "/**/  CALL cat.system.func('${spark.extra.prop}')",
            "-- single line comment \n CALL cat.system.func('${spark.extra.prop}')",
            "-- multiple \n-- single line \n-- comments \n CALL cat.system.func('${spark.extra.prop}')",
            "/* select * from multiline_comment \n where x like '%sql%'; */ CALL cat.system.func('${spark.extra.prop}')",
            "/* {\"app\": \"dbt\", \"dbt_version\": \"1.0.1\", \"profile_name\": \"profile1\", \"target_name\": \"dev\", "
                + "\"node_id\": \"model.profile1.stg_users\"} \n*/ CALL cat.system.func('${spark.extra.prop}')",
            "/* Some multi-line comment \n"
                + "*/ CALL /* inline comment */ cat.system.func('${spark.extra.prop}') -- ending comment",
            "CALL -- a line ending comment\n" + "cat.system.func('${spark.extra.prop}')");
    for (String sqlText : callStatementsWithComments) {
      CallStatement call = (CallStatement) parser.parsePlan(sqlText);
      Assert.assertEquals(
          ImmutableList.of("cat", "system", "func"), JavaConverters.seqAsJavaList(call.name()));

      Assert.assertEquals(1, call.args().size());

      checkArg(call, 0, "value", DataTypes.StringType);
    }
  }

  private void checkArg(
      CallStatement call, int index, Object expectedValue, DataType expectedType) {
    checkArg(call, index, null, expectedValue, expectedType);
  }

  private void checkArg(
      CallStatement call,
      int index,
      String expectedName,
      Object expectedValue,
      DataType expectedType) {

    if (expectedName != null) {
      NamedArgument arg = checkCast(call.args().apply(index), NamedArgument.class);
      Assert.assertEquals(expectedName, arg.name());
    } else {
      CallArgument arg = call.args().apply(index);
      checkCast(arg, PositionalArgument.class);
    }

    Expression expectedExpr = toSparkLiteral(expectedValue, expectedType);
    Expression actualExpr = call.args().apply(index).expr();
    Assert.assertEquals("Arg types must match", expectedExpr.dataType(), actualExpr.dataType());
    Assert.assertEquals("Arg must match", expectedExpr, actualExpr);
  }

  private Literal toSparkLiteral(Object value, DataType dataType) {
    return Literal$.MODULE$.create(value, dataType);
  }

  private <T> T checkCast(Object value, Class<T> expectedClass) {
    Assert.assertTrue(
        "Expected instance of " + expectedClass.getName(), expectedClass.isInstance(value));
    return expectedClass.cast(value);
  }
}
