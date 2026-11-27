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
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.catalog.CatalogObjectIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

public class ExpressionParser {

  private static final String TYPE = "type";
  private static final String VALUE = "value";
  private static final String VALUES = "values";
  private static final String TRANSFORM = "transform";
  private static final String TERM = "term";
  private static final String LEFT = "left";
  private static final String RIGHT = "right";
  private static final String CHILD = "child";
  private static final String REFERENCE = "reference";
  private static final String LITERAL = "literal";
  private static final String LITERALS = "literals";
  private static final String DATA_TYPE = "data-type";
  private static final String ID = "id";
  private static final String NAME = "name";
  private static final String APPLY = "apply";
  private static final String FUNCTION = "function";
  private static final String ARGUMENTS = "arguments";
  private static final String CATALOG = "catalog";
  private static final String IDENTIFIER = "identifier";
  private static final String ICEBERG_FUNCTIONS = "iceberg_functions";
  private static final Set<String> TRANSFORM_FUNCTIONS =
      Set.of("identity", "year", "month", "day", "hour", "bucket", "truncate");
  private static final Pattern HAS_WIDTH = Pattern.compile("(\\w+)\\[(\\d+)\\]");

  private ExpressionParser() {}

  public static String toJson(Expression expression) {
    return toJson(expression, false);
  }

  public static String toJson(Expression expression, boolean pretty) {
    Preconditions.checkArgument(expression != null, "Invalid expression: null");
    return JsonUtil.generate(gen -> toJson(expression, gen), pretty);
  }

  public static void toJson(Expression expression, JsonGenerator gen) {
    generate(expression, gen, true);
  }

  /**
   * Serializes an expression without the deprecated term-based predicate forms.
   *
   * <p>Predicates are written with value expression operands, bound references are written by field
   * ID, and transforms are written as {@code iceberg_functions} calls. Readers that only support
   * term-based predicates cannot parse the result.
   *
   * @param expression an expression
   * @return a JSON string
   */
  public static String toJsonV2(Expression expression) {
    Preconditions.checkArgument(expression != null, "Invalid expression: null");
    return JsonUtil.generate(gen -> toJsonV2(expression, gen), false);
  }

  /**
   * Serializes an expression without the deprecated term-based predicate forms.
   *
   * <p>Predicates are written with value expression operands, bound references are written by field
   * ID, and transforms are written as {@code iceberg_functions} calls. Readers that only support
   * term-based predicates cannot parse the result.
   *
   * @param expression an expression
   * @param gen a JSON generator
   */
  public static void toJsonV2(Expression expression, JsonGenerator gen) {
    generate(expression, gen, false);
  }

  private static void generate(Expression expression, JsonGenerator gen, boolean useTerms) {
    JsonGeneratorVisitor visitor = new JsonGeneratorVisitor(gen, useTerms);
    if (expression instanceof ValueExpression) {
      visitor.generate(() -> visitor.valueExpression(expression));
    } else {
      ExpressionVisitors.visit(expression, visitor);
    }
  }

  private static class JsonGeneratorVisitor
      extends ExpressionVisitors.CustomOrderExpressionVisitor<Void> {
    private final JsonGenerator gen;
    private final boolean useTerms;

    private JsonGeneratorVisitor(JsonGenerator gen, boolean useTerms) {
      this.gen = gen;
      this.useTerms = useTerms;
    }

    /**
     * A convenience method to make code more readable by calling {@code toJson} instead of {@code
     * get()}
     */
    private void toJson(Supplier<Void> child) {
      child.get();
    }

    @FunctionalInterface
    private interface Task {
      void run() throws IOException;
    }

    private Void generate(Task task) {
      try {
        task.run();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      return null;
    }

    @Override
    public Void alwaysTrue() {
      return generate(() -> gen.writeBoolean(true));
    }

    @Override
    public Void alwaysFalse() {
      return generate(() -> gen.writeBoolean(false));
    }

    @Override
    public Void not(Supplier<Void> child) {
      return generate(
          () -> {
            gen.writeStartObject();
            gen.writeStringField(TYPE, "not");
            gen.writeFieldName(CHILD);
            toJson(child);
            gen.writeEndObject();
          });
    }

    @Override
    public Void and(Supplier<Void> left, Supplier<Void> right) {
      return generate(
          () -> {
            gen.writeStartObject();
            gen.writeStringField(TYPE, "and");
            gen.writeFieldName(LEFT);
            toJson(left);
            gen.writeFieldName(RIGHT);
            toJson(right);
            gen.writeEndObject();
          });
    }

    @Override
    public Void or(Supplier<Void> left, Supplier<Void> right) {
      return generate(
          () -> {
            gen.writeStartObject();
            gen.writeStringField(TYPE, "or");
            gen.writeFieldName(LEFT);
            toJson(left);
            gen.writeFieldName(RIGHT);
            toJson(right);
            gen.writeEndObject();
          });
    }

    @Override
    public <T> Void predicate(BoundPredicate<T> pred) {
      if (!useTerms || pred.term() == null) {
        return generate(() -> boundPredicate(pred));
      }

      return generate(
          () -> {
            gen.writeStartObject();

            gen.writeStringField(TYPE, operationType(pred.op()));
            gen.writeFieldName(TERM);
            term(pred.term());

            if (pred.isLiteralPredicate()) {
              gen.writeFieldName(VALUE);
              SingleValueParser.toJson(
                  pred.term().type(), pred.asLiteralPredicate().literal().value(), gen);
            } else if (pred.isSetPredicate()) {
              gen.writeArrayFieldStart(VALUES);
              for (T value : pred.asSetPredicate().literalSet()) {
                SingleValueParser.toJson(pred.term().type(), value, gen);
              }
              gen.writeEndArray();
            }

            gen.writeEndObject();
          });
    }

    @Override
    public <T> Void predicate(UnboundPredicate<T> pred) {
      if (!useTerms || pred.term() == null) {
        return generate(() -> unboundPredicate(pred));
      }

      return generate(
          () -> {
            gen.writeStartObject();

            gen.writeStringField(TYPE, operationType(pred.op()));
            gen.writeFieldName(TERM);
            term(pred.term());

            if (pred.literals() != null) {
              if (pred.op() == Expression.Operation.IN
                  || pred.op() == Expression.Operation.NOT_IN) {
                gen.writeArrayFieldStart(VALUES);
                for (Literal<T> lit : pred.literals()) {
                  unboundLiteral(lit.value());
                }
                gen.writeEndArray();

              } else {
                gen.writeFieldName(VALUE);
                unboundLiteral(pred.literal().value());
              }
            }

            gen.writeEndObject();
          });
    }

    private void unboundLiteral(Object object) throws IOException {
      // this handles each type supported in Literals.from
      if (object instanceof Integer) {
        SingleValueParser.toJson(Types.IntegerType.get(), object, gen);
      } else if (object instanceof Long) {
        SingleValueParser.toJson(Types.LongType.get(), object, gen);
      } else if (object instanceof String) {
        SingleValueParser.toJson(Types.StringType.get(), object, gen);
      } else if (object instanceof Float) {
        SingleValueParser.toJson(Types.FloatType.get(), object, gen);
      } else if (object instanceof Double) {
        SingleValueParser.toJson(Types.DoubleType.get(), object, gen);
      } else if (object instanceof Boolean) {
        SingleValueParser.toJson(Types.BooleanType.get(), object, gen);
      } else if (object instanceof ByteBuffer) {
        SingleValueParser.toJson(Types.BinaryType.get(), object, gen);
      } else if (object instanceof byte[]) {
        SingleValueParser.toJson(Types.BinaryType.get(), ByteBuffer.wrap((byte[]) object), gen);
      } else if (object instanceof UUID) {
        SingleValueParser.toJson(Types.UUIDType.get(), object, gen);
      } else if (object instanceof BigDecimal) {
        BigDecimal decimal = (BigDecimal) object;
        SingleValueParser.toJson(
            Types.DecimalType.of(decimal.precision(), decimal.scale()), decimal, gen);
      }
    }

    private String operationType(Expression.Operation op) {
      return op.toString().replace('_', '-').toLowerCase(Locale.ROOT);
    }

    private void term(Term term) throws IOException {
      if (term instanceof UnboundTransform) {
        UnboundTransform<?, ?> transform = (UnboundTransform<?, ?>) term;
        transform(transform.transform().toString(), transform.ref().name());
        return;
      } else if (term instanceof BoundTransform) {
        BoundTransform<?, ?> transform = (BoundTransform<?, ?>) term;
        transform(transform.transform().toString(), transform.ref().name());
        return;
      } else if (term instanceof Reference) {
        gen.writeString(((Reference<?>) term).name());
        return;
      }

      throw new UnsupportedOperationException("Cannot write unsupported term: " + term);
    }

    private void transform(String transform, String name) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, TRANSFORM);
      gen.writeStringField(TRANSFORM, transform);
      gen.writeStringField(TERM, name);
      gen.writeEndObject();
    }

    private <T> void unboundPredicate(UnboundPredicate<T> pred) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, operationType(pred.op()));
      switch (pred.op()) {
        case IS_NULL, NOT_NULL, IS_NAN, NOT_NAN -> field(CHILD, pred.child());
        case IN, NOT_IN -> {
          field(CHILD, pred.child());
          gen.writeFieldName(VALUES);
          literals(pred.literals());
        }
        default -> {
          field(LEFT, pred.left());
          field(RIGHT, pred.right());
        }
      }

      gen.writeEndObject();
    }

    private <T> void boundPredicate(BoundPredicate<T> pred) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, operationType(pred.op()));
      if (pred.isUnaryPredicate()) {
        field(CHILD, pred.asUnaryPredicate().child());
      } else if (pred.isLiteralPredicate()) {
        BoundLiteralPredicate<T> literalPred = pred.asLiteralPredicate();
        field(LEFT, literalPred.left());
        field(RIGHT, literalPred.right());
      } else if (pred.isSetPredicate()) {
        BoundSetPredicate<T> setPred = pred.asSetPredicate();
        field(CHILD, setPred.child());
        gen.writeFieldName(VALUES);
        literalSet(valueType(setPred.child()), setPred.literalSet());
      } else {
        throw new UnsupportedOperationException("Cannot write unsupported predicate: " + pred);
      }

      gen.writeEndObject();
    }

    private void field(String name, Expression expr) throws IOException {
      gen.writeFieldName(name);
      valueExpression(expr);
    }

    private void valueExpression(Expression expr) throws IOException {
      if (expr instanceof Literal<?> literal) {
        literal(literal);
      } else if (expr instanceof NamedReference<?> ref) {
        reference(NAME, () -> gen.writeString(ref.name()));
      } else if (expr instanceof IdReference<?> ref) {
        reference(ID, () -> gen.writeNumber(ref.fieldId()));
      } else if (expr instanceof BoundReference<?> ref) {
        reference(ID, () -> gen.writeNumber(ref.fieldId()));
      } else if (expr instanceof UnboundTransform<?, ?> transform) {
        transformFunction(transform.transform(), transform.ref());
      } else if (expr instanceof BoundTransform<?, ?> transform) {
        transformFunction(transform.transform(), transform.ref());
      } else if (expr instanceof UnboundApply<?> apply) {
        apply(apply.function().catalog(), apply.function().identifier(), apply.arguments());
      } else if (expr instanceof BoundApply<?> apply) {
        apply(apply.function().catalog(), apply.function().identifier(), apply.arguments());
      } else {
        throw new UnsupportedOperationException(
            "Cannot write unsupported value expression: " + expr);
      }
    }

    private void reference(String field, Task value) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, REFERENCE);
      gen.writeFieldName(field);
      value.run();
      gen.writeEndObject();
    }

    private void literal(Literal<?> literal) throws IOException {
      Type type = literal.type();
      gen.writeStartObject();
      gen.writeStringField(TYPE, LITERAL);
      gen.writeFieldName(VALUE);
      value(type, literal.value());
      gen.writeStringField(DATA_TYPE, type.toString());
      gen.writeEndObject();
    }

    private <T> void literals(List<Literal<T>> literals) throws IOException {
      Type type = literals.isEmpty() ? null : literals.get(0).type();
      if (type == null || literals.stream().anyMatch(lit -> !type.equals(lit.type()))) {
        gen.writeStartArray();
        for (Literal<T> lit : literals) {
          literal(lit);
        }

        gen.writeEndArray();
      } else {
        literalSet(type, literals.stream().map(Literal::value).toList());
      }
    }

    private void literalSet(Type type, Iterable<?> values) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, LITERALS);
      gen.writeArrayFieldStart(VALUES);
      for (Object value : values) {
        value(type, value);
      }

      gen.writeEndArray();
      gen.writeStringField(DATA_TYPE, type.toString());
      gen.writeEndObject();
    }

    private void value(Type type, Object value) throws IOException {
      // decimal types use non-negative scales, so align values like 1E+3 before writing
      if (value instanceof BigDecimal decimal
          && type instanceof Types.DecimalType decimalType
          && decimal.scale() != decimalType.scale()) {
        SingleValueParser.toJson(type, decimal.setScale(decimalType.scale()), gen);
      } else {
        SingleValueParser.toJson(type, value, gen);
      }
    }

    private Type valueType(Expression expr) {
      if (expr instanceof BoundValueExpression<?> value) {
        return value.type();
      }

      throw new UnsupportedOperationException("Cannot determine type of: " + expr);
    }

    private void transformFunction(Transform<?, ?> transform, Expression reference)
        throws IOException {
      String name = transform.toString();
      Integer width = null;
      Matcher widthMatcher = HAS_WIDTH.matcher(name);
      if (widthMatcher.matches()) {
        name = widthMatcher.group(1);
        width = Integer.parseInt(widthMatcher.group(2));
      }

      if (!TRANSFORM_FUNCTIONS.contains(name)) {
        throw new UnsupportedOperationException(
            "Cannot write transform as a function: " + transform);
      }

      gen.writeStartObject();
      gen.writeStringField(TYPE, APPLY);
      gen.writeFieldName(FUNCTION);
      functionReference(ICEBERG_FUNCTIONS, CatalogObjectIdentifier.of(name));
      gen.writeArrayFieldStart(ARGUMENTS);
      if (width != null) {
        literal(Literal.of(width));
      }

      valueExpression(reference);
      gen.writeEndArray();
      gen.writeEndObject();
    }

    private void apply(
        String catalog, CatalogObjectIdentifier identifier, List<Expression> arguments)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, APPLY);
      gen.writeFieldName(FUNCTION);
      functionReference(catalog, identifier);
      gen.writeArrayFieldStart(ARGUMENTS);
      for (Expression argument : arguments) {
        if (argument instanceof ValueExpression) {
          valueExpression(argument);
        } else {
          ExpressionVisitors.visit(argument, new JsonGeneratorVisitor(gen, false));
        }
      }

      gen.writeEndArray();
      gen.writeEndObject();
    }

    private void functionReference(String catalog, CatalogObjectIdentifier identifier)
        throws IOException {
      gen.writeStartObject();
      if (catalog != null) {
        gen.writeStringField(CATALOG, catalog);
      }

      gen.writeArrayFieldStart(IDENTIFIER);
      for (String level : identifier.levels()) {
        gen.writeString(level);
      }

      gen.writeEndArray();
      gen.writeEndObject();
    }
  }

  public static Expression fromJson(String json) {
    return fromJson(json, null);
  }

  public static Expression fromJson(JsonNode json) {
    return fromJson(json, null);
  }

  public static Expression fromJson(String json, Schema schema) {
    return JsonUtil.parse(json, node -> fromJson(node, schema));
  }

  /**
   * Parses a value expression, such as a function call, a reference, or a literal.
   *
   * <p>A literal without a data type is parsed as a long, double, string, or boolean.
   *
   * @param json a JSON string
   * @return a value expression
   * @throws IllegalArgumentException if the JSON is not a valid value expression
   */
  public static ValueExpression<?> valueFromJson(String json) {
    return JsonUtil.parse(json, ExpressionParser::valueFromJson);
  }

  /**
   * Parses a value expression, such as a function call, a reference, or a literal.
   *
   * <p>A literal without a data type is parsed as a long, double, string, or boolean.
   *
   * @param json a JSON node
   * @return a value expression
   * @throws IllegalArgumentException if the JSON is not a valid value expression
   */
  public static ValueExpression<?> valueFromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse value expression from null object");
    return valueExpression(json, null);
  }

  /**
   * Parses a value expression for a value of the given type, such as a field's write default.
   *
   * <p>A JSON object with a {@code type} field is parsed as a value expression, and a literal in
   * that form without a data type is parsed as the given type. Any other JSON is a constant in the
   * single-value serialization of the given type.
   *
   * @param json a JSON node
   * @param type the type of value the expression produces
   * @return a value expression
   * @throws IllegalArgumentException if the JSON is not a valid value expression or a constant of
   *     the given primitive type
   */
  public static ValueExpression<?> valueFromJson(JsonNode json, Type type) {
    Preconditions.checkArgument(null != json, "Cannot parse value expression from null object");
    Preconditions.checkArgument(null != type, "Invalid type: null");
    if (json.isObject() && json.has(TYPE)) {
      if (LITERAL.equalsIgnoreCase(JsonUtil.getString(TYPE, json))) {
        return valueLiteral(json, type);
      }

      return valueExpression(json, null);
    }

    Preconditions.checkArgument(
        type.isPrimitiveType(), "Cannot parse %s constant as a literal: %s", type, json);
    return typedLiteral(type, json);
  }

  static Expression fromJson(JsonNode json, Schema schema) {
    Preconditions.checkArgument(null != json, "Cannot parse expression from null object");
    // check for constant expressions
    if (json.isBoolean()) {
      if (json.asBoolean()) {
        return Expressions.alwaysTrue();
      } else {
        return Expressions.alwaysFalse();
      }
    }

    Preconditions.checkArgument(
        json.isObject(), "Cannot parse expression from non-object: %s", json);

    String type = JsonUtil.getString(TYPE, json);
    if (type.equalsIgnoreCase(LITERAL)) {
      if (JsonUtil.getBool(VALUE, json)) {
        return Expressions.alwaysTrue();
      } else {
        return Expressions.alwaysFalse();
      }
    }

    Expression.Operation op = fromType(type);
    switch (op) {
      case NOT:
        return Expressions.not(fromJson(JsonUtil.get(CHILD, json), schema));
      case AND:
        return Expressions.and(
            fromJson(JsonUtil.get(LEFT, json), schema),
            fromJson(JsonUtil.get(RIGHT, json), schema));
      case OR:
        return Expressions.or(
            fromJson(JsonUtil.get(LEFT, json), schema),
            fromJson(JsonUtil.get(RIGHT, json), schema));
    }

    if (json.has(TERM)) {
      return predicateFromJson(op, json, schema);
    }

    return valuePredicateFromJson(op, json, schema);
  }

  private static Expression.Operation fromType(String type) {
    return Expression.Operation.fromString(type.replace('-', '_'));
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundPredicate<T> predicateFromJson(
      Expression.Operation op, JsonNode node, Schema schema) {
    UnboundTerm<T> term = term(JsonUtil.get(TERM, node));

    Function<JsonNode, T> convertValue;
    if (schema != null) {
      BoundTerm<?> bound = term.bind(schema.asStruct(), false);
      convertValue = valueNode -> (T) SingleValueParser.fromJson(bound.type(), valueNode);
    } else {
      convertValue = valueNode -> (T) ExpressionParser.asObject(valueNode);
    }

    switch (op) {
      case IS_NULL:
      case NOT_NULL:
      case IS_NAN:
      case NOT_NAN:
        // unary predicates
        Preconditions.checkArgument(
            !node.has(VALUE), "Cannot parse %s predicate: has invalid value field", op);
        Preconditions.checkArgument(
            !node.has(VALUES), "Cannot parse %s predicate: has invalid values field", op);
        return Expressions.predicate(op, term);
      case LT:
      case LT_EQ:
      case GT:
      case GT_EQ:
      case EQ:
      case NOT_EQ:
      case STARTS_WITH:
      case NOT_STARTS_WITH:
        // literal predicates
        Preconditions.checkArgument(
            node.has(VALUE), "Cannot parse %s predicate: missing value", op);
        Preconditions.checkArgument(
            !node.has(VALUES), "Cannot parse %s predicate: has invalid values field", op);
        T value = literal(JsonUtil.get(VALUE, node), convertValue);
        return Expressions.predicate(op, term, ImmutableList.of(value));
      case IN:
      case NOT_IN:
        // literal set predicates
        Preconditions.checkArgument(
            node.has(VALUES), "Cannot parse %s predicate: missing values", op);
        Preconditions.checkArgument(
            !node.has(VALUE), "Cannot parse %s predicate: has invalid value field", op);
        JsonNode valuesNode = JsonUtil.get(VALUES, node);
        Preconditions.checkArgument(
            valuesNode.isArray(), "Cannot parse literals from non-array: %s", valuesNode);
        return Expressions.predicate(
            op,
            term,
            Iterables.transform(
                ((ArrayNode) valuesNode)::elements, valueNode -> literal(valueNode, convertValue)));
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + op);
    }
  }

  private static <T> T literal(JsonNode valueNode, Function<JsonNode, T> toValue) {
    if (valueNode.isObject() && valueNode.has(TYPE)) {
      String type = JsonUtil.getString(TYPE, valueNode);
      Preconditions.checkArgument(
          type.equalsIgnoreCase(LITERAL), "Cannot parse type as a literal: %s", type);
      return toValue.apply(JsonUtil.get(VALUE, valueNode));
    }

    // the node is a directly embedded literal value
    return toValue.apply(valueNode);
  }

  private static Object asObject(JsonNode node) {
    if (node.isIntegralNumber() && node.canConvertToLong()) {
      return node.asLong();
    } else if (node.isTextual()) {
      return node.asText();
    } else if (node.isFloatingPointNumber()) {
      return node.asDouble();
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else {
      throw new IllegalArgumentException("Cannot convert JSON to literal: " + node);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> term(JsonNode node) {
    if (node.isTextual()) {
      return Expressions.ref(node.asText());
    } else if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      switch (type) {
        case REFERENCE:
          return Expressions.ref(JsonUtil.getString(TERM, node));
        case TRANSFORM:
          UnboundTerm<T> child = term(JsonUtil.get(TERM, node));
          String transform = JsonUtil.getString(TRANSFORM, node);
          return (UnboundTerm<T>)
              Expressions.transform(child.ref().name(), Transforms.fromString(transform));
        default:
          throw new IllegalArgumentException("Cannot parse type as a reference: " + type);
      }
    }

    throw new IllegalArgumentException(
        "Cannot parse reference (requires string or object): " + node);
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundPredicate<T> valuePredicateFromJson(
      Expression.Operation op, JsonNode node, Schema schema) {
    switch (op) {
      case IS_NULL:
      case NOT_NULL:
      case IS_NAN:
      case NOT_NAN:
        Preconditions.checkArgument(
            !node.has(VALUES), "Cannot parse %s predicate: has invalid values field", op);
        ValueExpression<T> child = valueExpression(JsonUtil.get(CHILD, node), schema);
        UnboundTerm<T> term = classicTerm(child);
        return Expressions.predicate(op, term != null ? term : child);
      case LT:
      case LT_EQ:
      case GT:
      case GT_EQ:
      case EQ:
      case NOT_EQ:
      case STARTS_WITH:
      case NOT_STARTS_WITH:
        Preconditions.checkArgument(
            !node.has(VALUES), "Cannot parse %s predicate: has invalid values field", op);
        ValueExpression<T> left = valueExpression(JsonUtil.get(LEFT, node), schema);
        JsonNode rightNode = JsonUtil.get(RIGHT, node);
        UnboundTerm<T> leftTerm = classicTerm(left);
        if (leftTerm != null && isLiteral(rightNode)) {
          Literal<T> literal = valueLiteral(rightNode, termType(leftTerm, schema));
          return Expressions.predicate(op, leftTerm, literal);
        }

        return Expressions.predicate(op, left, valueExpression(rightNode, schema));
      case IN:
      case NOT_IN:
        ValueExpression<T> setChild = valueExpression(JsonUtil.get(CHILD, node), schema);
        UnboundTerm<T> setTerm = classicTerm(setChild);
        // literals pass through as values, so they keep their types
        Iterable<T> values =
            (Iterable<T>)
                (Iterable<?>)
                    literals(
                        JsonUtil.get(VALUES, node),
                        setTerm != null ? termType(setTerm, schema) : null);
        return Expressions.predicate(op, setTerm != null ? setTerm : setChild, values);
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + op);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> classicTerm(ValueExpression<?> expr) {
    if (expr instanceof NamedReference<?> ref) {
      return (UnboundTerm<T>) ref;
    } else if (expr instanceof UnboundApply<?> apply) {
      return transformTerm(apply);
    }

    return null;
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> transformTerm(UnboundApply<?> apply) {
    FunctionReference function = apply.function();
    String name = function.name().toLowerCase(Locale.ROOT);
    if (!ICEBERG_FUNCTIONS.equalsIgnoreCase(function.catalog())
        || function.identifier().length() != 1
        || !TRANSFORM_FUNCTIONS.contains(name)) {
      return null;
    }

    boolean hasWidth = name.equals("bucket") || name.equals("truncate");
    int refPos = hasWidth ? 1 : 0;
    List<Expression> args = apply.arguments();
    if (args.size() != refPos + 1 || !(args.get(refPos) instanceof NamedReference<?> ref)) {
      return null;
    }

    String transform = name;
    if (hasWidth) {
      if (!(args.get(0) instanceof Literal<?> lit) || !(lit.value() instanceof Integer width)) {
        return null;
      }

      transform = name + "[" + width + "]";
    }

    return (UnboundTerm<T>) Expressions.transform(ref.name(), Transforms.fromString(transform));
  }

  private static Type termType(UnboundTerm<?> term, Schema schema) {
    return schema != null ? term.bind(schema.asStruct(), false).type() : null;
  }

  @SuppressWarnings("unchecked")
  private static <T> ValueExpression<T> valueExpression(JsonNode node, Schema schema) {
    if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node).toLowerCase(Locale.ROOT);
      switch (type) {
        case LITERAL:
          return valueLiteral(node, null);
        case REFERENCE:
          return (ValueExpression<T>) reference(node);
        case APPLY:
          return (ValueExpression<T>) apply(node, schema);
        default:
          throw new IllegalArgumentException("Cannot parse value expression type: " + type);
      }
    }

    Preconditions.checkArgument(
        !node.isArray(), "Cannot parse value expression from array: %s", node);
    return valueLiteral(node, null);
  }

  private static ValueExpression<?> reference(JsonNode node) {
    if (node.has(ID)) {
      Preconditions.checkArgument(
          !node.has(NAME), "Cannot parse reference with both id and name: %s", node);
      return Expressions.ref(JsonUtil.getInt(ID, node));
    } else if (node.has(NAME)) {
      return Expressions.ref(JsonUtil.getString(NAME, node));
    } else if (node.has(TERM)) {
      return Expressions.ref(JsonUtil.getString(TERM, node));
    }

    throw new IllegalArgumentException("Cannot parse reference without id or name: " + node);
  }

  private static UnboundApply<?> apply(JsonNode node, Schema schema) {
    FunctionReference function = functionReference(JsonUtil.get(FUNCTION, node));
    JsonNode argumentsNode = JsonUtil.get(ARGUMENTS, node);
    Preconditions.checkArgument(
        argumentsNode.isArray(),
        "Cannot parse function arguments from non-array: %s",
        argumentsNode);
    Expression[] arguments = new Expression[argumentsNode.size()];
    for (int i = 0; i < arguments.length; i += 1) {
      arguments[i] = functionArgument(argumentsNode.get(i), schema);
    }

    return Expressions.apply(function, arguments);
  }

  private static Expression functionArgument(JsonNode node, Schema schema) {
    if (node.isBoolean()) {
      // a bare boolean is a predicate; boolean literals are written as literal objects
      return fromJson(node, schema);
    } else if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node).toLowerCase(Locale.ROOT);
      if (type.equals(LITERAL) || type.equals(REFERENCE) || type.equals(APPLY)) {
        return valueExpression(node, schema);
      }

      return fromJson(node, schema);
    }

    return valueExpression(node, schema);
  }

  private static FunctionReference functionReference(JsonNode node) {
    if (node.isTextual()) {
      return Expressions.function(CatalogObjectIdentifier.of(node.asText()));
    } else if (node.isArray()) {
      return Expressions.function(CatalogObjectIdentifier.of(JsonUtil.getStringArray(node)));
    } else if (node.isObject()) {
      return Expressions.function(
          JsonUtil.getStringOrNull(CATALOG, node),
          CatalogObjectIdentifier.of(JsonUtil.getStringArray(IDENTIFIER, node)));
    }

    throw new IllegalArgumentException("Cannot parse function reference: " + node);
  }

  private static boolean isLiteral(JsonNode node) {
    if (node.isObject()) {
      return node.has(TYPE) && LITERAL.equalsIgnoreCase(node.get(TYPE).asText());
    }

    return !node.isArray();
  }

  private static <T> Literal<T> valueLiteral(JsonNode node, Type schemaType) {
    if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      Preconditions.checkArgument(
          type.equalsIgnoreCase(LITERAL), "Cannot parse type as a literal: %s", type);
      JsonNode valueNode = JsonUtil.get(VALUE, node);
      if (node.has(DATA_TYPE)) {
        return typedLiteral(dataType(node), valueNode);
      }

      return untypedLiteral(valueNode, schemaType);
    }

    return untypedLiteral(node, schemaType);
  }

  private static <T> List<Literal<T>> literals(JsonNode node, Type schemaType) {
    if (node.isArray()) {
      return ImmutableList.copyOf(
          Iterables.transform(
              ((ArrayNode) node)::elements, elem -> valueLiteral(elem, schemaType)));
    }

    Preconditions.checkArgument(
        node.isObject() && LITERALS.equalsIgnoreCase(JsonUtil.getString(TYPE, node)),
        "Cannot parse literals: %s",
        node);
    Type type = dataType(node);
    JsonNode valuesNode = JsonUtil.get(VALUES, node);
    Preconditions.checkArgument(
        valuesNode.isArray(), "Cannot parse literals from non-array: %s", valuesNode);
    return ImmutableList.copyOf(
        Iterables.transform(((ArrayNode) valuesNode)::elements, elem -> typedLiteral(type, elem)));
  }

  private static Type dataType(JsonNode node) {
    return Types.fromPrimitiveString(JsonUtil.getString(DATA_TYPE, node));
  }

  @SuppressWarnings("unchecked")
  private static <T> Literal<T> untypedLiteral(JsonNode valueNode, Type schemaType) {
    if (schemaType != null) {
      return typedLiteral(schemaType, valueNode);
    }

    return (Literal<T>) Expressions.lit(asObject(valueNode));
  }

  @SuppressWarnings("unchecked")
  private static <T> Literal<T> typedLiteral(Type type, JsonNode valueNode) {
    Preconditions.checkArgument(!valueNode.isNull(), "Cannot parse %s literal from null", type);
    Object value = SingleValueParser.fromJson(type, valueNode);
    Literal<?> literal =
        type.typeId() == Type.TypeID.TIMESTAMP_NANO
            ? Expressions.nanos((Long) value)
            : Expressions.lit(value);
    Literal<T> converted = literal.to(type);
    Preconditions.checkArgument(converted != null, "Cannot parse %s literal: %s", type, valueNode);
    return converted;
  }
}
