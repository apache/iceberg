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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

public class ExpressionParser {
  private static final String TYPE = "type";
  private static final String VALUE = "value";
  private static final String OPERATION = "operation";
  private static final String LITERALS = "literals";
  private static final String TERM = "term";
  private static final String LEFT_OPERAND = "left-operand";
  private static final String RIGHT_OPERAND = "right-operand";
  private static final String OPERAND = "operand";
  private static final String AND = "and";
  private static final String OR = "or";
  private static final String NOT = "not";
  private static final String TRUE = "true";
  private static final String FALSE = "false";
  private static final String UNBOUNDED_PREDICATE = "unbounded-predicate";
  private static final String BOUNDED_LITERAL_PREDICATE = "bounded-literal-predicate";
  private static final String BOUNDED_SET_PREDICATE = "bounded-set-predicate";
  private static final String BOUNDED_UNARY_PREDICATE = "bounded-unary-predicate";
  private static final Set<String> PREDICATE_TYPES = Sets.newHashSet(
          UNBOUNDED_PREDICATE,
          BOUNDED_LITERAL_PREDICATE,
          BOUNDED_SET_PREDICATE,
          BOUNDED_UNARY_PREDICATE);
  private static final String NAMED_REFERENCE = "named-reference";
  private static final String BOUND_REFERENCE = "bound-reference";
  private static final String ABOVE_MAX = "above-max";
  private static final String BELOW_MIN = "below-min";

  private static final String IS_NULL = "is";
  private static final String NOT_NULL = "not_null";
  private static final String IS_NAN = "is_nan";
  private static final String NOT_NAN = "not_nan";

  private static final Set<Expression.Operation> ONE_INPUTS = ImmutableSet.of(
          Expression.Operation.IS_NULL,
          Expression.Operation.NOT_NULL,
          Expression.Operation.IS_NAN,
          Expression.Operation.NOT_NAN);

  private static final Set<String> ONE_INPUTS_STRINGS = Sets.newHashSet(
          Expression.Operation.IS_NULL.name().toLowerCase(),
          Expression.Operation.NOT_NULL.name().toLowerCase(),
          Expression.Operation.IS_NAN.name().toLowerCase(),
          Expression.Operation.NOT_NAN.name().toLowerCase());

  private static final String BOOLEAN = "boolean";
  private static final String INTEGER = "int";
  private static final String LONG = "long";
  private static final String FLOAT = "float";
  private static final String DOUBLE = "double";
  private static final String DATE = "date";
  private static final String TIME = "time";
  private static final String TIMESTAMP = "timestamp";
  private static final String STRING = "string";
  private static final String UUID = "uuid";
  private static final String FIXED = "fixed";
  private static final String BINARY = "binary";
  private static final String DECIMAL = "decimal";

  private ExpressionParser() {
  }

  public static String toJson(Expression expression, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(expression, generator);
      generator.flush();
      return writer.toString();

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write json", e);
    }
  }

  public static void toJson(Expression expression, JsonGenerator generator) throws IOException {
    if (expression instanceof And) {
      toJson((And) expression, generator);
    } else if (expression instanceof Or) {
      toJson((Or) expression, generator);
    } else if (expression instanceof Not) {
      toJson((Not) expression, generator);
    } else if (expression instanceof True) {
      toJson((True) expression, generator);
    } else if (expression instanceof False) {
      toJson((False) expression, generator);
    } else if (expression instanceof Predicate) {
      toJson((Predicate) expression, generator);
    } else {
      throw new IllegalArgumentException("Invalid Operation Type");
    }
  }

  public static void toJson(And expression, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, AND);
    generator.writeFieldName(LEFT_OPERAND);
    toJson(expression.left(), generator);
    generator.writeFieldName(RIGHT_OPERAND);
    toJson(expression.right(), generator);
    generator.writeEndObject();
  }

  public static void toJson(Or expression, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, OR);
    generator.writeFieldName(LEFT_OPERAND);
    toJson(expression.left(), generator);
    generator.writeFieldName(RIGHT_OPERAND);
    toJson(expression.right(), generator);
    generator.writeEndObject();
  }

  public static void toJson(Not expression, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, NOT);
    generator.writeFieldName(OPERAND);
    toJson(expression.child(), generator);
    generator.writeEndObject();
  }

  public static void toJson(True expression, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, TRUE);
    generator.writeEndObject();
  }

  public static void toJson(False expression, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, FALSE);
    generator.writeEndObject();
  }

  public static void toJson(Predicate predicate, JsonGenerator generator) throws IOException {
    if (predicate instanceof UnboundPredicate) {
      toJson((UnboundPredicate) predicate, generator);
    } else if (predicate instanceof BoundLiteralPredicate) {
      toJson((BoundLiteralPredicate) predicate, generator);
    } else if (predicate instanceof BoundSetPredicate) {
      toJson((BoundSetPredicate) predicate, generator);
    } else if (predicate instanceof BoundUnaryPredicate) {
      toJson((BoundUnaryPredicate) predicate, generator);
    } else {
      throw new IllegalArgumentException("Cannot find valid Predicate Type for " + predicate + ".");
    }
  }

  public static void toJson(UnboundPredicate predicate, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, UNBOUNDED_PREDICATE);
    generator.writeStringField(OPERATION, predicate.op().name().toLowerCase());
    generator.writeFieldName(TERM);
    toJson(predicate.term(), generator);
    if (!(ONE_INPUTS.contains(predicate.op()))) {
      generator.writeFieldName(LITERALS);
      toJson(predicate.literals(), generator);
    }

    generator.writeEndObject();
  }

  public static void toJson(BoundLiteralPredicate predicate, JsonGenerator generator) {
    throw new UnsupportedOperationException(
            "Serialization of Predicate type BoundLiteralPredicate is not currently supported.");
  }

  public static void toJson(BoundSetPredicate predicate, JsonGenerator generator) {
    throw new UnsupportedOperationException(
            "Serialization of Predicate type BoundSetPredicate is not currently supported.");
  }

  public static void toJson(BoundUnaryPredicate predicate, JsonGenerator generator) {
    throw new UnsupportedOperationException(
            "Serialization of Predicate type BoundUnaryPredicate is not currently supported.");
  }

  public static void toJson(Term term, JsonGenerator generator) throws IOException {
    if (term instanceof NamedReference) {
      toJson((NamedReference) term, generator);
    } else if (term instanceof BoundReference) {
      toJson((BoundReference) term, generator); // Need to Implement
    } else {
      throw new IllegalArgumentException("Cannot find valid Term Type for " + term + ".");
    }
  }

  public static void toJson(NamedReference term, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, NAMED_REFERENCE);
    generator.writeStringField(VALUE, term.name());
    generator.writeEndObject();
  }

  public static void toJson(BoundReference term, JsonGenerator generator) {
    throw new UnsupportedOperationException("Serialization of Term type BoundReference is not currently supported");
  }

  public static void toJson(List<Literal> literals, JsonGenerator generator) throws IOException {
    generator.writeStartArray();
    for (int i = 0; i < literals.size(); i++) {
      toJson(literals.get(i), generator);
    }
    generator.writeEndArray();
  }

  public static void toJson(Literal literal, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    if (literal instanceof Literals.AboveMax) {
      generator.writeStringField(TYPE, ABOVE_MAX);
    } else if (literal instanceof Literals.BelowMin) {
      generator.writeStringField(TYPE, BELOW_MIN);
    } else {
      generator.writeStringField(TYPE, Literals.typeFromLiteralValue(literal).toString());
      generator.writeStringField(VALUE, StandardCharsets.UTF_8.decode(literal.toByteBuffer()).toString());
    }

    generator.writeEndObject();
  }

  public static Expression fromJson(String json) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return fromJson(mapper.readTree(json));
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public static Expression fromJson(JsonNode json) {
    String expressionType = JsonUtil.getString("type", json);

    if (AND.equals(expressionType)) {
      return new And(fromJson(json.get(LEFT_OPERAND)), fromJson(json.get(RIGHT_OPERAND)));
    } else if (OR.equals(expressionType)) {
      return new Or(fromJson(json.get(LEFT_OPERAND)), fromJson(json.get(RIGHT_OPERAND)));
    } else if (NOT.equals(expressionType)) {
      return new Not(fromJson(json.get(OPERAND)));
    } else if (TRUE.equals(expressionType)) {
      return True.INSTANCE;
    } else if (FALSE.equals(expressionType)) {
      return False.INSTANCE;
    } else if (PREDICATE_TYPES.contains(expressionType)) {
      return fromJsonToPredicate(json, expressionType);
    } else {
      throw new IllegalArgumentException("Invalid Operation Type");
    }
  }

  public static Predicate fromJsonToPredicate(JsonNode json, String predicateType) {
    if (UNBOUNDED_PREDICATE.equals(predicateType)) {
      return fromJsonUnboundPredicate(json);
    } else if (BOUNDED_LITERAL_PREDICATE.equals(predicateType)) {
      throw new UnsupportedOperationException(
              "Serialization of Predicate type BoundLiteralPredicate is not currently supported.");
    } else if (BOUNDED_SET_PREDICATE.equals(predicateType)) {
      throw new UnsupportedOperationException(
              "Serialization of Predicate type BoundSetPredicate is not currently supported.");
    } else if (BOUNDED_UNARY_PREDICATE.equals(predicateType)) {
      throw new UnsupportedOperationException(
              "Serialization of Predicate type BoundUnaryPredicate is not currently supported.");
    } else {
      throw new IllegalArgumentException("Invalid Predicate Type");
    }
  }

  @SuppressWarnings("CyclomaticComplexity")
  public static UnboundPredicate fromJsonUnboundPredicate(JsonNode json) {
    String operation = json.get(OPERATION).textValue();

    if (ONE_INPUTS_STRINGS.contains(operation)) {
      switch (operation) {
        case IS_NULL:
          return new UnboundPredicate(
                  Expression.Operation.IS_NULL, fromJsonToTerm(json.get(TERM)));
        case NOT_NULL:
          return new UnboundPredicate(
                  Expression.Operation.NOT_NULL, fromJsonToTerm(json.get(TERM)));
        case IS_NAN:
          return new UnboundPredicate(
                  Expression.Operation.IS_NAN, fromJsonToTerm(json.get(TERM)));
        case NOT_NAN:
          return new UnboundPredicate(
                  Expression.Operation.NOT_NAN, fromJsonToTerm(json.get(TERM)));
        default:
          throw new IllegalArgumentException("Cannot find valid Operation Type for " + operation + ".");
      }

    } else if (operation.equals(Expression.Operation.LT.name().toLowerCase())) {
      return new UnboundPredicate(
              Expression.Operation.LT,
              fromJsonToTerm(json.get(TERM)),
              fromJsonToLiterals(json.get(LITERALS)));
    } else if (operation.equals(Expression.Operation.LT_EQ.name().toLowerCase())) {
      return new UnboundPredicate(
              Expression.Operation.LT_EQ,
              fromJsonToTerm(json.get(TERM)),
              fromJsonToLiterals(json.get(LITERALS)));
    } else if (operation.equals(Expression.Operation.GT.name().toLowerCase())) {
      return new UnboundPredicate(
              Expression.Operation.GT, fromJsonToTerm(json.get(TERM)),
              fromJsonToLiterals(json.get(LITERALS)));
    } else if (operation.equals(Expression.Operation.GT_EQ.name().toLowerCase())) {
      return new UnboundPredicate(
              Expression.Operation.GT_EQ,
              fromJsonToTerm(json.get(TERM)),
              fromJsonToLiterals(json.get(LITERALS)));
    } else if (operation.equals(Expression.Operation.EQ.name().toLowerCase())) {
      return new UnboundPredicate(
              Expression.Operation.EQ,
              fromJsonToTerm(json.get(TERM)),
              fromJsonToLiterals(json.get(LITERALS)));
    } else if (operation.equals(Expression.Operation.NOT_EQ.name().toLowerCase())) {
      return new UnboundPredicate(
              Expression.Operation.NOT_EQ,
              fromJsonToTerm(json.get(TERM)),
              fromJsonToLiterals(json.get(LITERALS)));
    } else if (operation.equals(Expression.Operation.IN.name().toLowerCase())) {
      return new UnboundPredicate(
              Expression.Operation.IN,
              fromJsonToTerm(json.get(TERM)),
              fromJsonToLiterals(json.get(LITERALS)));
    } else if (operation.equals(Expression.Operation.NOT_IN.name().toLowerCase())) {
      return new UnboundPredicate(
              Expression.Operation.NOT_IN,
              fromJsonToTerm(json.get(TERM)),
              fromJsonToLiterals(json.get(LITERALS)));
    } else if (operation.equals(Expression.Operation.STARTS_WITH.name().toLowerCase())) {
      return new UnboundPredicate(
              Expression.Operation.STARTS_WITH,
              fromJsonToTerm(json.get(TERM)),
              fromJsonToLiterals(json.get(LITERALS)));
    } else if (operation.equals(Expression.Operation.NOT_STARTS_WITH.name().toLowerCase())) {
      return new UnboundPredicate(
              Expression.Operation.NOT_STARTS_WITH,
              fromJsonToTerm(json.get(TERM)),
              fromJsonToLiterals(json.get(LITERALS)));
    } else {
      throw new IllegalArgumentException("Cannot find valid Operation Type for " + operation + ".");
    }
  }

  public static UnboundTerm fromJsonToTerm(JsonNode json) {
    String referenceType = json.get(TYPE).textValue();

    if (referenceType.equals(NAMED_REFERENCE)) {
      return new NamedReference(json.get(VALUE).textValue());
    } else if (referenceType.equals(BOUND_REFERENCE)) {
      throw new UnsupportedOperationException(
              "Serialization of Predicate type BoundReference is not currently supported.");
    } else {
      throw new IllegalArgumentException("Invalid Term Reference Type");
    }
  }

  public static List<?> fromJsonToLiterals(JsonNode json) {
    List<?> literals = Lists.newArrayList();
    String literalType;
    for (int i = 0; i < json.size(); i++) {
      literalType = json.get(i).get(TYPE).textValue();
      literals.add(Conversions.fromByteBuffer(
              Types.fromPrimitiveString(literalType),
              StandardCharsets.UTF_8.encode(json.get(i).get(VALUE).textValue())));
    }

    return literals;
  }
}
