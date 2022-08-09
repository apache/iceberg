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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

public class ExpressionParser {

  private static final String TYPE = "type";
  private static final String VALUE = "value";
  private static final String LITERALS = "literals";
  private static final String TERM = "term";
  private static final String LEFT = "left";
  private static final String RIGHT = "right";
  private static final String OPERAND = "operand";

  private static final Set<Expression.Operation> ONE_INPUTS =
      ImmutableSet.of(
          Expression.Operation.IS_NULL,
          Expression.Operation.NOT_NULL,
          Expression.Operation.IS_NAN,
          Expression.Operation.NOT_NAN);

  private ExpressionParser() {}

  public static String toJson(Expression expression) {
    return toJson(expression, false);
  }

  public static String toJson(Expression expression, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(expression, gen), pretty);
  }

  public static void toJson(Expression expression, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != expression, "Invalid expression: null");

    if (expression instanceof And) {
      toJson((And) expression, gen);
    } else if (expression instanceof Or) {
      toJson((Or) expression, gen);
    } else if (expression instanceof Not) {
      toJson((Not) expression, gen);
    } else if (expression instanceof True) {
      toJson((True) expression, gen);
    } else if (expression instanceof False) {
      toJson((False) expression, gen);
    } else if (expression instanceof UnboundPredicate) {
      toJson((UnboundPredicate<?>) expression, gen);
    } else {
      throw new IllegalArgumentException(String.format("Unsupported expression: %s", expression));
    }
  }

  private static void toJson(And expression, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(TYPE, Expression.Operation.AND.name().toLowerCase());
    gen.writeFieldName(LEFT);
    toJson(expression.left(), gen);
    gen.writeFieldName(RIGHT);
    toJson(expression.right(), gen);
    gen.writeEndObject();
  }

  private static void toJson(Or expression, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(TYPE, Expression.Operation.OR.name().toLowerCase());
    gen.writeFieldName(LEFT);
    toJson(expression.left(), gen);
    gen.writeFieldName(RIGHT);
    toJson(expression.right(), gen);
    gen.writeEndObject();
  }

  private static void toJson(Not expression, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(TYPE, Expression.Operation.NOT.name().toLowerCase());
    gen.writeFieldName(OPERAND);
    toJson(expression.child(), gen);
    gen.writeEndObject();
  }

  private static void toJson(True expression, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(TYPE, expression.op().name().toLowerCase());
    gen.writeEndObject();
  }

  private static void toJson(False expression, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(TYPE, expression.op().name().toLowerCase());
    gen.writeEndObject();
  }

  private static void toJson(UnboundPredicate<?> predicate, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(TYPE, predicate.op().name().toLowerCase());
    toJson(predicate.term(), gen);
    if (!ONE_INPUTS.contains(predicate.op())) {
      gen.writeFieldName(LITERALS);
      gen.writeStartArray();
      for (Literal<?> literal : predicate.literals()) {
        toJson(literal, gen);
      }
      gen.writeEndArray();
    }

    gen.writeEndObject();
  }

  private static void toJson(Term term, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(term instanceof NamedReference, "Unsupported term: %s", term);
    // TODO: add support for UnboundTransform
    gen.writeStringField(TERM, ((NamedReference<?>) term).name());
  }

  private static void toJson(Literal<?> literal, JsonGenerator gen) throws IOException {
    gen.writeStartObject();

    Object value = literal.value();
    Type type;
    if (value instanceof Boolean) {
      type = Types.BooleanType.get();
    } else if (value instanceof Integer) {
      type = Types.IntegerType.get();
    } else if (value instanceof Long) {
      type = Types.LongType.get();
    } else if (value instanceof Float) {
      type = Types.FloatType.get();
    } else if (value instanceof Double) {
      type = Types.DoubleType.get();
    } else if (value instanceof CharSequence) {
      type = Types.StringType.get();
    } else if (value instanceof UUID) {
      type = Types.UUIDType.get();
    } else if (value instanceof byte[]) {
      type = Types.FixedType.ofLength(((byte[]) value).length);
    } else if (value instanceof ByteBuffer) {
      if (literal instanceof Literals.FixedLiteral) {
        type = Types.FixedType.ofLength(((ByteBuffer) value).remaining());
      } else {
        type = Types.BinaryType.get();
      }
    } else if (value instanceof BigDecimal) {
      BigDecimal decimal = (BigDecimal) value;
      type = Types.DecimalType.of(decimal.precision(), decimal.scale());
    } else {
      throw new IllegalArgumentException(
          "Cannot find literal type for value class " + value.getClass().getName());
    }

    gen.writeStringField(TYPE, type.toString());
    gen.writeStringField(VALUE, StandardCharsets.UTF_8.decode(literal.toByteBuffer()).toString());
    gen.writeEndObject();
  }

  public static Expression fromJson(String json) {
    return JsonUtil.parse(json, ExpressionParser::fromJson);
  }

  public static Expression fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse expression from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse expression from non-object: %s", json);

    String expressionType = JsonUtil.getString(TYPE, json);

    if (Expression.Operation.AND.name().toLowerCase().equals(expressionType)) {
      return new And(fromJson(JsonUtil.get(LEFT, json)), fromJson(JsonUtil.get(RIGHT, json)));
    } else if (Expression.Operation.OR.name().toLowerCase().equals(expressionType)) {
      return new Or(fromJson(JsonUtil.get(LEFT, json)), fromJson(JsonUtil.get(RIGHT, json)));
    } else if (Expression.Operation.NOT.name().toLowerCase().equals(expressionType)) {
      return new Not(fromJson(JsonUtil.get(OPERAND, json)));
    } else if (Expression.Operation.TRUE.name().toLowerCase().equals(expressionType)) {
      return True.INSTANCE;
    } else if (Expression.Operation.FALSE.name().toLowerCase().equals(expressionType)) {
      return False.INSTANCE;
    } else {
      return fromJsonUnboundPredicate(json);
    }
  }

  private static UnboundPredicate<?> fromJsonUnboundPredicate(JsonNode json) {
    Expression.Operation operationType =
        Expression.Operation.valueOf(JsonUtil.getString(TYPE, json).toUpperCase());

    if (ONE_INPUTS.contains(operationType)) {
      return new UnboundPredicate<>(operationType, fromJsonToTerm(json));
    } else {
      return new UnboundPredicate(
          operationType,
          fromJsonToTerm(json),
          fromJsonToLiteralValues(JsonUtil.get(LITERALS, json)));
    }
  }

  private static UnboundTerm<?> fromJsonToTerm(JsonNode json) {
    // TODO: support UnboundTransform
    return new NamedReference<>(JsonUtil.getString(TERM, json));
  }

  private static List<?> fromJsonToLiteralValues(JsonNode json) {
    List<Object> literals = Lists.newArrayList();
    for (int i = 0; i < json.size(); i++) {
      String literalType = JsonUtil.get(TYPE, json.get(i)).textValue();
      Type primitiveType = Types.fromPrimitiveString(literalType);
      Object value =
          Conversions.fromByteBuffer(
              primitiveType,
              StandardCharsets.UTF_8.encode(JsonUtil.get(VALUE, json.get(i)).textValue()));
      if (primitiveType.typeId() == Type.TypeID.FIXED) {
        byte[] valueByteArray = new byte[((ByteBuffer) value).remaining()];
        ((ByteBuffer) value).get(valueByteArray);
        value = valueByteArray;
      }

      literals.add(value);
    }

    return literals;
  }
}
