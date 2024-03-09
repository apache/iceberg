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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExpressionParser {

  private static final Types.StructType SUPPORTED_PRIMITIVES =
      Types.StructType.of(
          required(100, "id", Types.LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          required(104, "l", Types.LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          required(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          required(108, "ts", Types.TimestampType.withoutZone()),
          required(110, "s", Types.StringType.get()),
          required(111, "uuid", Types.UUIDType.get()),
          required(112, "fixed", Types.FixedType.ofLength(7)),
          optional(113, "bytes", Types.BinaryType.get()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
          required(116, "dec_38_10", Types.DecimalType.of(38, 10)), // maximum precision
          required(117, "time", Types.TimeType.get()));
  private static final Schema SCHEMA = new Schema(SUPPORTED_PRIMITIVES.fields());

  @Test
  public void testSimpleExpressions() {
    Expression[] expressions =
        new Expression[] {
          Expressions.alwaysFalse(),
          Expressions.alwaysTrue(),
          Expressions.equal("id", 100),
          Expressions.equal("data", "abcd"),
          Expressions.equal("b", false),
          Expressions.equal("i", 34),
          Expressions.equal("l", 34L),
          Expressions.equal("f", 100.0f),
          Expressions.equal("d", 100.0d),
          Expressions.equal("date", "2022-08-14"),
          Expressions.equal("ts", "2022-08-14T10:00:00.123456"),
          Expressions.equal("uuid", UUID.randomUUID()),
          Expressions.equal("fixed", new byte[] {1, 2, 3, 4, 5, 6, 7}),
          Expressions.equal("bytes", ByteBuffer.wrap(new byte[] {1, 3, 5})),
          Expressions.equal("dec_11_2", new BigDecimal("34.56")),
          Expressions.equal("time", "23:59:59.654321"),
          Expressions.lessThan("id", 100),
          Expressions.lessThanOrEqual("id", 100),
          Expressions.greaterThan("id", 100),
          Expressions.greaterThanOrEqual("id", 100),
          Expressions.isNull("data"),
          Expressions.notNull("data"),
          Expressions.isNaN("d"),
          Expressions.notNaN("f"),
          Expressions.startsWith("s", "crackle"),
          Expressions.notStartsWith("s", "tackle"),
          Expressions.equal(Expressions.day("date"), "2022-08-14"),
          Expressions.equal(Expressions.bucket("id", 100), 0),
          Expressions.and(
              Expressions.or(
                  Expressions.equal("data", UUID.randomUUID().toString()),
                  Expressions.isNull("data")),
              Expressions.greaterThanOrEqual("id", 66)),
          Expressions.or(
              Expressions.greaterThan(Expressions.day("ts"), "2022-08-14"),
              Expressions.equal("date", "2022-08-14")),
          Expressions.not(Expressions.in("l", 1, 2, 3, 4))
        };

    for (Expression expr : expressions) {
      Expression bound = Binder.bind(SUPPORTED_PRIMITIVES, expr);
      String boundJson = ExpressionParser.toJson(bound, true);
      String unboundJson = ExpressionParser.toJson(expr, true);

      Assertions.assertThat(boundJson)
          .as("Bound and unbound should produce identical json")
          .isEqualTo(unboundJson);

      Expression parsed = ExpressionParser.fromJson(boundJson, SCHEMA);
      Assertions.assertThat(ExpressionUtil.equivalent(expr, parsed, SUPPORTED_PRIMITIVES, true))
          .as("Round-trip value should be equivalent")
          .isTrue();
    }
  }

  @Test
  public void nullExpression() {
    Assertions.assertThatThrownBy(() -> ExpressionParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid expression: null");

    Assertions.assertThatThrownBy(() -> ExpressionParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse expression from null object");
  }

  @Test
  public void trueExpression() {
    Assertions.assertThat(ExpressionParser.toJson(Expressions.alwaysTrue(), true))
        .isEqualTo("true");
    Assertions.assertThat(ExpressionParser.fromJson("true")).isEqualTo(Expressions.alwaysTrue());

    // type=literal is also supported
    String longJson = "{\n  \"type\" : \"literal\",\n  \"value\" : true\n}";
    Assertions.assertThat(ExpressionParser.fromJson(longJson)).isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void falseExpression() {
    Assertions.assertThat(ExpressionParser.toJson(Expressions.alwaysFalse(), true))
        .isEqualTo("false");
    Assertions.assertThat(ExpressionParser.fromJson("false")).isEqualTo(Expressions.alwaysFalse());

    // type=literal is also supported
    String longJson = "{\n  \"type\" : \"literal\",\n  \"value\" : false\n}";
    Assertions.assertThat(ExpressionParser.fromJson(longJson)).isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  public void eqExpression() {
    String expected =
        "{\n" + "  \"type\" : \"eq\",\n" + "  \"term\" : \"name\",\n" + "  \"value\" : 25\n" + "}";
    Assertions.assertThat(ExpressionParser.toJson(Expressions.equal("name", 25), true))
        .isEqualTo(expected);
    Expression expression = ExpressionParser.fromJson(expected);
    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
  }

  @Test
  public void testTransform() {
    String expected =
        "{\n"
            + "  \"type\" : \"lt-eq\",\n"
            + "  \"term\" : {\n"
            + "    \"type\" : \"transform\",\n"
            + "    \"transform\" : \"bucket[100]\",\n"
            + "    \"term\" : \"id\"\n"
            + "  },\n"
            + "  \"value\" : 50\n"
            + "}";

    Assertions.assertThat(
            ExpressionParser.toJson(
                Expressions.lessThanOrEqual(Expressions.bucket("id", 100), 50), true))
        .isEqualTo(expected);
    // schema is required to parse transform expressions
    Assertions.assertThat(
            ExpressionParser.toJson(ExpressionParser.fromJson(expected, SCHEMA), true))
        .isEqualTo(expected);
  }

  @Test
  public void extraFields() {
    Assertions.assertThat(
            ExpressionParser.toJson(
                ExpressionParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"in\",\n"
                        + "  \"term\" : \"column-name\",\n"
                        + "  \"extra-one\" : \"x\",\n"
                        + "  \"extra-twp\" : \"y\",\n"
                        + "  \"values\" : [ 1, 2, 3 ]\n"
                        + "}"),
                true))
        .isEqualTo(
            "{\n"
                + "  \"type\" : \"in\",\n"
                + "  \"term\" : \"column-name\",\n"
                + "  \"values\" : [ 1, 2, 3 ]\n"
                + "}");
  }

  @Test
  public void invalidTerm() {
    Assertions.assertThatThrownBy(
            () ->
                ExpressionParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"not\",\n"
                        + "  \"child\" : {\n"
                        + "    \"type\" : \"lt\",\n"
                        + "    \"term\" : 23,\n"
                        + "    \"values\" : [ \"a\" ]\n"
                        + "  }\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse reference (requires string or object): 23");
  }

  @Test
  public void invalidValues() {
    Assertions.assertThatThrownBy(
            () ->
                ExpressionParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"not-nan\",\n"
                        + "  \"term\" : \"x\",\n"
                        + "  \"value\" : 34.0\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse NOT_NAN predicate: has invalid value field");

    Assertions.assertThatThrownBy(
            () ->
                ExpressionParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"is-nan\",\n"
                        + "  \"term\" : \"x\",\n"
                        + "  \"values\" : [ 34.0, 35.0 ]\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse IS_NAN predicate: has invalid values field");

    Assertions.assertThatThrownBy(
            () ->
                ExpressionParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"lt\",\n"
                        + "  \"term\" : \"x\",\n"
                        + "  \"values\" : [ 1 ]\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse LT predicate: missing value");

    Assertions.assertThatThrownBy(
            () ->
                ExpressionParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"lt\",\n"
                        + "  \"term\" : \"x\",\n"
                        + "  \"value\" : 34,\n"
                        + "  \"values\" : [ 1 ]\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse LT predicate: has invalid values field");

    Assertions.assertThatThrownBy(
            () ->
                ExpressionParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"not-in\",\n"
                        + "  \"term\" : \"x\",\n"
                        + "  \"value\" : [ 1, 2 ]\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse NOT_IN predicate: missing values");

    Assertions.assertThatThrownBy(
            () ->
                ExpressionParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"in\",\n"
                        + "  \"term\" : \"x\",\n"
                        + "  \"value\" : \"min\",\n"
                        + "  \"values\" : [ 1, 2 ]\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse IN predicate: has invalid value field");
  }

  @Test
  public void invalidOperationType() {
    Assertions.assertThatThrownBy(
            () ->
                ExpressionParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"not\",\n"
                        + "  \"child\" : {\n"
                        + "    \"type\" : \"illegal\",\n"
                        + "    \"term\" : \"column-name\",\n"
                        + "    \"values\" : [ \"a\" ]\n"
                        + "  }\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid operation type: illegal");

    Assertions.assertThatThrownBy(
            () ->
                ExpressionParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"ILLEGAL\",\n"
                        + "  \"child\" : {\n"
                        + "    \"type\" : \"lt\",\n"
                        + "    \"term\" : \"column-name\",\n"
                        + "    \"values\" : [ \"a\" ]\n"
                        + "  }\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid operation type: ILLEGAL");
  }

  @Test
  public void invalidAnd() {
    Assertions.assertThatThrownBy(() -> ExpressionParser.fromJson("{\n  \"type\" : \"and\"\n}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: left");

    Assertions.assertThatThrownBy(
            () -> ExpressionParser.fromJson("{\n  \"type\" : \"and\",\n  \"left\": true}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: right");

    Assertions.assertThatThrownBy(
            () -> ExpressionParser.fromJson("{\n  \"type\" : \"and\",\n  \"right\": true}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: left");
  }

  @Test
  public void testPredicate() {
    String expected =
        "{\n"
            + "  \"type\" : \"lt-eq\",\n"
            + "  \"term\" : \"column-name\",\n"
            + "  \"value\" : 50\n"
            + "}";

    Assertions.assertThat(
            ExpressionParser.toJson(Expressions.lessThanOrEqual("column-name", 50), true))
        .isEqualTo(expected);
    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
        .isEqualTo(expected);
  }

  @Test
  public void testPredicateWithObjectLiteral() {
    String expected =
        "{\n"
            + "  \"type\" : \"lt-eq\",\n"
            + "  \"term\" : \"column-name\",\n"
            + "  \"value\" : 50\n"
            + "}";

    String json =
        "{\n"
            + "  \"type\" : \"lt-eq\",\n"
            + "  \"term\" : \"column-name\",\n"
            + "  \"value\" : {"
            + "    \"type\" : \"literal\",\n"
            + "    \"value\" : 50\n"
            + "  }\n"
            + "}";

    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(json), true))
        .isEqualTo(expected);
  }

  @Test
  public void testPredicateWithObjectReference() {
    String expected =
        "{\n"
            + "  \"type\" : \"lt-eq\",\n"
            + "  \"term\" : \"column-name\",\n"
            + "  \"value\" : 50\n"
            + "}";

    String json =
        "{\n"
            + "  \"type\" : \"lt-eq\",\n"
            + "  \"term\" : {\n"
            + "    \"type\" : \"reference\",\n"
            + "    \"term\" : \"column-name\"\n"
            + "  },\n"
            + "  \"value\" : 50\n"
            + "}";

    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(json), true))
        .isEqualTo(expected);
  }

  @Test
  public void testAnd() {
    String expected =
        "{\n"
            + "  \"type\" : \"and\",\n"
            + "  \"left\" : {\n"
            + "    \"type\" : \"gt-eq\",\n"
            + "    \"term\" : \"column-name-1\",\n"
            + "    \"value\" : 50\n"
            + "  },\n"
            + "  \"right\" : {\n"
            + "    \"type\" : \"in\",\n"
            + "    \"term\" : \"column-name-2\",\n"
            + "    \"values\" : [ \"one\", \"two\" ]\n"
            + "  }\n"
            + "}";

    Expression expression =
        Expressions.and(
            Expressions.greaterThanOrEqual("column-name-1", 50),
            Expressions.in("column-name-2", "one", "two"));

    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
        .isEqualTo(expected);
  }

  @Test
  public void testOr() {
    String expected =
        "{\n"
            + "  \"type\" : \"or\",\n"
            + "  \"left\" : {\n"
            + "    \"type\" : \"lt\",\n"
            + "    \"term\" : \"column-name-1\",\n"
            + "    \"value\" : 50\n"
            + "  },\n"
            + "  \"right\" : {\n"
            + "    \"type\" : \"not-null\",\n"
            + "    \"term\" : \"column-name-2\"\n"
            + "  }\n"
            + "}";

    Expression expression =
        Expressions.or(
            Expressions.lessThan("column-name-1", 50), Expressions.notNull("column-name-2"));
    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
        .isEqualTo(expected);
  }

  @Test
  public void testNot() {
    String expected =
        "{\n"
            + "  \"type\" : \"not\",\n"
            + "  \"child\" : {\n"
            + "    \"type\" : \"gt-eq\",\n"
            + "    \"term\" : \"column-name-1\",\n"
            + "    \"value\" : 50\n"
            + "  }\n"
            + "}";

    Expression expression = Expressions.not(Expressions.greaterThanOrEqual("column-name-1", 50));

    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
        .isEqualTo(expected);
  }

  @Test
  public void testNestedExpression() {
    String expected =
        "{\n"
            + "  \"type\" : \"or\",\n"
            + "  \"left\" : {\n"
            + "    \"type\" : \"and\",\n"
            + "    \"left\" : {\n"
            + "      \"type\" : \"in\",\n"
            + "      \"term\" : \"column-name-1\",\n"
            + "      \"values\" : [ 50, 51, 52 ]\n"
            + "    },\n"
            + "    \"right\" : {\n"
            + "      \"type\" : \"eq\",\n"
            + "      \"term\" : \"column-name-2\",\n"
            + "      \"value\" : \"test\"\n"
            + "    }\n"
            + "  },\n"
            + "  \"right\" : {\n"
            + "    \"type\" : \"is-nan\",\n"
            + "    \"term\" : \"column-name-3\"\n"
            + "  }\n"
            + "}";

    Expression and =
        Expressions.and(
            Expressions.in("column-name-1", 50, 51, 52),
            Expressions.equal("column-name-2", "test"));
    Expression expression = Expressions.or(and, Expressions.isNaN("column-name-3"));

    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
        .isEqualTo(expected);
  }

  @Test
  public void testFixedLiteral() {
    String expected =
        "{\n"
            + "  \"type\" : \"eq\",\n"
            + "  \"term\" : \"column-name\",\n"
            + "  \"value\" : \"010203\"\n"
            + "}";

    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    Expression expression = Expressions.equal("column-name", byteBuffer);

    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
        .isEqualTo(expected);
  }

  @Test
  public void testDecimalLiteral() {
    String expected =
        "{\n"
            + "  \"type\" : \"in\",\n"
            + "  \"term\" : \"column-name\",\n"
            + "  \"values\" : [ \"3.14\" ]\n"
            + "}";

    Expression expression = Expressions.in("column-name", new BigDecimal("3.14"));

    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
        .isEqualTo(expected);
  }

  @Test
  public void testNegativeScaleDecimalLiteral() {
    String expected =
        "{\n"
            + "  \"type\" : \"in\",\n"
            + "  \"term\" : \"column-name\",\n"
            + "  \"values\" : [ \"3.14E+4\" ]\n"
            + "}";

    Expression expression = Expressions.in("column-name", new BigDecimal("3.14E+4"));

    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
        .isEqualTo(expected);
  }
}
