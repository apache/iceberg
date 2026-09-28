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
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
  private static final String APPLY = "apply";
  private static final String FUNCTION = "function";
  private static final String ARGUMENTS = "arguments";
  private static final String NAME = "name";
  private static final String ID = "id";
  private static final String IDENTIFIER = "identifier";
  private static final String CATALOG = "catalog";

  private static final Pattern HAS_WIDTH = Pattern.compile("(\\w+)\\[(\\d+)]");

  private static final String ICEBERG_FUNCTIONS = "iceberg_functions";
  // the expressions spec defines partition transforms as functions, other than void
  private static final Map<String, Supplier<Transform<?, ?>>> TRANSFORMS =
      ImmutableMap.of(
          "identity", Transforms::identity,
          "year", Transforms::year,
          "month", Transforms::month,
          "day", Transforms::day,
          "hour", Transforms::hour);
  // bucket and truncate take the transform parameter as their first argument
  private static final Map<String, IntFunction<Transform<?, ?>>> PARAMETERIZED_TRANSFORMS =
      ImmutableMap.of("bucket", Transforms::bucket, "truncate", Transforms::truncate);

  private ExpressionParser() {}

  public static String toJson(Expression expression) {
    return toJson(expression, false);
  }

  public static String toJson(Expression expression, boolean pretty) {
    Preconditions.checkArgument(expression != null, "Invalid expression: null");
    return JsonUtil.generate(gen -> toJson(expression, gen), pretty);
  }

  public static void toJson(Expression expression, JsonGenerator gen) {
    ExpressionVisitors.visit(expression, new JsonGeneratorVisitor(gen));
  }

  private static class JsonGeneratorVisitor
      extends ExpressionVisitors.CustomOrderExpressionVisitor<Void> {
    private final JsonGenerator gen;

    private JsonGeneratorVisitor(JsonGenerator gen) {
      this.gen = gen;
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
      return generate(
          () -> {
            gen.writeStartObject();
            gen.writeStringField(TYPE, operationType(pred.op()));

            if (pred.isUnaryPredicate()) {
              gen.writeFieldName(CHILD);
              writeExpr(pred.term());
            } else if (pred.isLiteralPredicate()) {
              gen.writeFieldName(LEFT);
              writeExpr(pred.term());
              gen.writeFieldName(RIGHT);
              SingleValueParser.toJson(
                  pred.term().type(), pred.asLiteralPredicate().literal().value(), gen);
            } else if (pred.isSetPredicate()) {
              gen.writeFieldName(CHILD);
              writeExpr(pred.term());
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
      return generate(
          () -> {
            gen.writeStartObject();
            gen.writeStringField(TYPE, operationType(pred.op()));

            if (pred.op() == Expression.Operation.IN || pred.op() == Expression.Operation.NOT_IN) {
              gen.writeFieldName(CHILD);
              writeExpr(pred.term());
              gen.writeArrayFieldStart(VALUES);
              if (pred.literals() != null) {
                for (Literal<T> lit : pred.literals()) {
                  unboundLiteral(lit.value());
                }
              }
              gen.writeEndArray();
            } else if (pred.literals() == null || pred.literals().isEmpty()) {
              gen.writeFieldName(CHILD);
              writeExpr(pred.term());
            } else {
              gen.writeFieldName(LEFT);
              writeExpr(pred.term());
              gen.writeFieldName(RIGHT);
              unboundLiteral(pred.literal().value());
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
      } else {
        throw new UnsupportedOperationException(
            "Cannot write literal of unsupported type: " + object.getClass().getName());
      }
    }

    private String operationType(Expression.Operation op) {
      return op.toString().replace('_', '-').toLowerCase(Locale.ROOT);
    }

    private void writeExpr(Term term) throws IOException {
      if (term instanceof UnboundApply) {
        writeApply((UnboundApply<?>) term);
      } else if (term instanceof UnboundTransform) {
        UnboundTransform<?, ?> transform = (UnboundTransform<?, ?>) term;
        writeTransform(transform.transform(), transform.ref());
      } else if (term instanceof BoundTransform) {
        BoundTransform<?, ?> transform = (BoundTransform<?, ?>) term;
        writeTransform(transform.transform(), transform.ref());
      } else if (term instanceof BoundReference) {
        BoundReference<?> ref = (BoundReference<?>) term;
        gen.writeStartObject();
        gen.writeStringField(TYPE, REFERENCE);
        gen.writeNumberField(ID, ref.fieldId());
        gen.writeEndObject();
      } else if (term instanceof Reference) {
        gen.writeStartObject();
        gen.writeStringField(TYPE, REFERENCE);
        gen.writeStringField(NAME, ((Reference<?>) term).name());
        gen.writeEndObject();
      } else {
        throw new UnsupportedOperationException("Cannot write unsupported term: " + term);
      }
    }

    /**
     * Writes a transform as an apply expression. Parameterized transforms are written as
     * two-argument functions with the parameter first, like {@code bucket(16, ref)}.
     */
    private void writeTransform(Transform<?, ?> transform, Term ref) throws IOException {
      String transformStr = transform.toString();
      gen.writeStartObject();
      gen.writeStringField(TYPE, APPLY);

      Matcher matcher = HAS_WIDTH.matcher(transformStr);
      boolean parameterized = matcher.matches();
      gen.writeStringField(FUNCTION, parameterized ? matcher.group(1) : transformStr);

      gen.writeArrayFieldStart(ARGUMENTS);
      if (parameterized) {
        gen.writeNumber(Integer.parseInt(matcher.group(2)));
      }
      writeExpr(ref);
      gen.writeEndArray();

      gen.writeEndObject();
    }

    private void writeApply(UnboundApply<?> apply) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, APPLY);

      writeFunctionRef(apply.function());

      gen.writeArrayFieldStart(ARGUMENTS);
      for (Object arg : apply.arguments()) {
        if (arg instanceof Term) {
          writeExpr((Term) arg);
        } else if (arg instanceof Expression) {
          ExpressionParser.toJson((Expression) arg, gen);
        } else {
          // remaining arguments are constants, written as bare literal values
          unboundLiteral(((Literal<?>) arg).value());
        }
      }
      gen.writeEndArray();

      gen.writeEndObject();
    }

    private void writeFunctionRef(FunctionReference ref) throws IOException {
      if (ref.catalog() == null && ref.identifier().size() == 1) {
        gen.writeStringField(FUNCTION, ref.name());
      } else if (ref.catalog() == null) {
        JsonUtil.writeStringArray(FUNCTION, ref.identifier(), gen);
      } else {
        gen.writeFieldName(FUNCTION);
        gen.writeStartObject();
        gen.writeStringField(CATALOG, ref.catalog());
        JsonUtil.writeStringArray(IDENTIFIER, ref.identifier(), gen);
        gen.writeEndObject();
      }
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
      case TRUE:
        // deprecated: the constant true predicate is written as a bare boolean
        return Expressions.alwaysTrue();
      case FALSE:
        // deprecated: the constant false predicate is written as a bare boolean
        return Expressions.alwaysFalse();
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
      return termPredicateFromJson(op, json, schema);
    } else {
      return predicateFromJson(op, json, schema);
    }
  }

  private static Expression.Operation fromType(String type) {
    return Expression.Operation.fromString(type.replace('-', '_'));
  }

  private static <T> UnboundPredicate<T> predicateFromJson(
      Expression.Operation op, JsonNode node, Schema schema) {
    return switch (op) {
      case IS_NULL, NOT_NULL, IS_NAN, NOT_NAN -> {
        UnboundTerm<T> child = exprFromJson(JsonUtil.get(CHILD, node), schema);
        yield Expressions.predicate(op, child);
      }
      case LT, LT_EQ, GT, GT_EQ, EQ, NOT_EQ, STARTS_WITH, NOT_STARTS_WITH -> {
        UnboundTerm<T> left = exprFromJson(JsonUtil.get(LEFT, node), schema);
        Function<JsonNode, T> convertValue = valueConverter(left, schema);
        T value = literalFromJson(JsonUtil.get(RIGHT, node), convertValue);
        yield Expressions.predicate(op, left, ImmutableList.of(value));
      }
      case IN, NOT_IN -> {
        UnboundTerm<T> child = exprFromJson(JsonUtil.get(CHILD, node), schema);
        Function<JsonNode, T> convertValue = valueConverter(child, schema);
        Iterable<T> values = literalsFromJson(JsonUtil.get(VALUES, node), convertValue);
        yield Expressions.predicate(op, child, values);
      }
      default -> throw new UnsupportedOperationException("Unsupported operation: " + op);
    };
  }

  private static <T> UnboundPredicate<T> termPredicateFromJson(
      Expression.Operation op, JsonNode node, Schema schema) {
    UnboundTerm<T> term = termFromJson(JsonUtil.get(TERM, node));

    Function<JsonNode, T> convertValue = valueConverter(term, schema);

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
        T value = literalFromJson(JsonUtil.get(VALUE, node), convertValue);
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
        return Expressions.predicate(op, term, literalsFromJson(valuesNode, convertValue));
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + op);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Function<JsonNode, T> valueConverter(UnboundTerm<T> term, Schema schema) {
    if (schema != null) {
      BoundTerm<?> bound = term.bind(schema.asStruct(), false);
      return valueNode -> (T) valueFromJson(bound.type(), valueNode);
    } else {
      return valueNode -> (T) ExpressionParser.asObject(valueNode);
    }
  }

  private static <T> UnboundTerm<T> exprFromJson(JsonNode node, Schema schema) {
    if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      return switch (type) {
        case REFERENCE -> referenceFromJson(node, schema);
        case APPLY -> applyFromJson(node, schema);
        default -> throw new IllegalArgumentException("Unknown value expression type: " + type);
      };
    }

    // a bare string is a literal value, which cannot be a predicate operand
    throw new IllegalArgumentException(
        "Cannot parse value expression, expected a reference or apply: " + node);
  }

  private static <T> UnboundTerm<T> referenceFromJson(JsonNode node, Schema schema) {
    if (node.has(NAME)) {
      return Expressions.ref(JsonUtil.getString(NAME, node));
    } else if (node.has(ID)) {
      int fieldId = JsonUtil.getInt(ID, node);
      Preconditions.checkArgument(
          schema != null, "Cannot parse reference by field ID %s without a schema", fieldId);
      String name = schema.findColumnName(fieldId);
      Preconditions.checkArgument(name != null, "Cannot find field with ID %s in schema", fieldId);
      return Expressions.ref(name);
    } else if (node.has(TERM)) {
      return Expressions.ref(JsonUtil.getString(TERM, node));
    }

    throw new IllegalArgumentException(
        "Cannot parse reference (requires 'name', 'id', or 'term' field): " + node);
  }

  private static <T> UnboundTerm<T> applyFromJson(JsonNode node, Schema schema) {
    FunctionReference function = functionRefFromJson(JsonUtil.get(FUNCTION, node));
    List<Object> arguments = Lists.newArrayList();

    if (node.has(ARGUMENTS)) {
      JsonNode argsNode = JsonUtil.get(ARGUMENTS, node);
      Preconditions.checkArgument(
          argsNode.isArray(), "Apply arguments must be an array: %s", argsNode);
      for (JsonNode argNode : argsNode) {
        arguments.add(argumentFromJson(argNode, schema));
      }
    }

    if (isIcebergFunction(function)) {
      String name = function.name().toLowerCase(Locale.ROOT);
      if (TRANSFORMS.containsKey(name) || PARAMETERIZED_TRANSFORMS.containsKey(name)) {
        return transformFromApply(function, name, arguments);
      }
    }

    return Expressions.apply(function, arguments);
  }

  /**
   * Returns whether a function reference may be a function defined by the expressions spec.
   *
   * <p>The spec defines Iceberg partition transforms as functions in the {@code iceberg_functions}
   * catalog, other than {@code void}.
   */
  private static boolean isIcebergFunction(FunctionReference function) {
    return function.catalog() == null || function.catalog().equalsIgnoreCase(ICEBERG_FUNCTIONS);
  }

  /**
   * Converts a call to an Iceberg partition transform to an {@link UnboundTransform}.
   *
   * <p>Parameterized transforms are called as two-argument functions with the transform parameter
   * first, like {@code bucket(16, ref)}.
   */
  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> transformFromApply(
      FunctionReference function, String name, List<Object> arguments) {
    IntFunction<Transform<?, ?>> parameterized = PARAMETERIZED_TRANSFORMS.get(name);
    int expectedArgs = parameterized != null ? 2 : 1;
    Preconditions.checkArgument(
        arguments.size() == expectedArgs,
        "Cannot convert %s to a transform: expected %s argument(s), got %s",
        function,
        expectedArgs,
        arguments.size());

    Transform<?, ?> transform;
    if (parameterized != null) {
      Object parameter = arguments.get(0);
      Preconditions.checkArgument(
          isInt(parameter),
          "Cannot convert %s to a transform: first argument must be an int, got %s",
          function,
          parameter);
      transform = parameterized.apply(((Number) parameter).intValue());
    } else {
      transform = TRANSFORMS.get(name).get();
    }

    Object valueArg = arguments.get(expectedArgs - 1);
    Preconditions.checkArgument(
        valueArg instanceof NamedReference,
        "Cannot convert %s to a transform: last argument must be a reference, got %s",
        function,
        valueArg);

    return (UnboundTerm<T>)
        Expressions.transform(((NamedReference<?>) valueArg).name(), (Transform<?, T>) transform);
  }

  private static boolean isInt(Object value) {
    return value instanceof Integer || (value instanceof Long l && l == l.intValue());
  }

  private static Object argumentFromJson(JsonNode node, Schema schema) {
    if (node.isIntegralNumber()) {
      return node.canConvertToInt() ? (Object) node.asInt() : (Object) node.asLong();
    } else if (node.isFloatingPointNumber()) {
      return node.asDouble();
    } else if (node.isTextual()) {
      // a bare string is a literal value, not a reference
      return node.asText();
    } else if (node.isBoolean()) {
      return node.asBoolean() ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
    } else if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      return switch (type) {
        case REFERENCE, APPLY -> exprFromJson(node, schema);
        case LITERAL -> literalFromJson(node, ExpressionParser::asObject);
        default -> fromJson(node, schema);
      };
    }

    throw new IllegalArgumentException("Cannot parse apply argument: " + node);
  }

  private static FunctionReference functionRefFromJson(JsonNode node) {
    if (node.isTextual()) {
      return Expressions.function(node.asText());
    } else if (node.isArray()) {
      return Expressions.function(JsonUtil.getStringArray(node));
    } else if (node.isObject()) {
      return Expressions.function(
          JsonUtil.getStringOrNull(CATALOG, node), JsonUtil.getStringList(IDENTIFIER, node));
    }

    throw new IllegalArgumentException("Cannot parse function reference: " + node);
  }

  @SuppressWarnings("unchecked")
  private static <T> T literalFromJson(JsonNode valueNode, Function<JsonNode, T> toValue) {
    if (valueNode.isObject() && valueNode.has(TYPE)) {
      String type = JsonUtil.getString(TYPE, valueNode);
      Preconditions.checkArgument(
          type.equalsIgnoreCase(LITERAL), "Cannot parse type as a literal: %s", type);
      JsonNode value = JsonUtil.get(VALUE, valueNode);
      if (valueNode.hasNonNull(DATA_TYPE)) {
        return (T) requiredValueFromJson(dataTypeFromJson(valueNode), value);
      }

      return toValue.apply(value);
    }

    // the node is a directly embedded literal value
    return toValue.apply(valueNode);
  }

  @SuppressWarnings("unchecked")
  private static <T> Iterable<T> literalsFromJson(
      JsonNode node, Function<JsonNode, T> convertValue) {
    if (node.isArray()) {
      return Iterables.transform(
          ((ArrayNode) node)::elements, valueNode -> literalFromJson(valueNode, convertValue));
    } else if (node.isObject() && node.has(TYPE)) {
      String type = JsonUtil.getString(TYPE, node);
      Preconditions.checkArgument(
          type.equalsIgnoreCase(LITERALS), "Cannot parse type as literals: %s", type);
      JsonNode valuesNode = JsonUtil.get(VALUES, node);
      Preconditions.checkArgument(
          valuesNode.isArray(), "Cannot parse literals values from non-array: %s", valuesNode);
      if (node.hasNonNull(DATA_TYPE)) {
        Type dataType = dataTypeFromJson(node);
        return Iterables.transform(
            ((ArrayNode) valuesNode)::elements,
            valueNode -> (T) requiredValueFromJson(dataType, valueNode));
      }

      return Iterables.transform(((ArrayNode) valuesNode)::elements, convertValue::apply);
    }

    throw new IllegalArgumentException("Cannot parse literals: " + node);
  }

  private static Type dataTypeFromJson(JsonNode node) {
    return Types.fromPrimitiveString(JsonUtil.getString(DATA_TYPE, node));
  }

  private static Object requiredValueFromJson(Type dataType, JsonNode valueNode) {
    Object value = valueFromJson(dataType, valueNode);
    Preconditions.checkArgument(value != null, "Cannot parse %s literal from null value", dataType);
    return value;
  }

  /**
   * Parses a value of the given type into the object used to create an unbound literal.
   *
   * <p>Nanosecond timestamps are returned as their validated ISO-8601 string. Unbound literals are
   * created from values with {@code Literals.from}, which would read a long as microseconds when
   * binding to a nanosecond timestamp; a string literal converts to nanoseconds correctly.
   */
  private static Object valueFromJson(Type type, JsonNode valueNode) {
    Object value = SingleValueParser.fromJson(type, valueNode);
    if (value != null && type.typeId() == Type.TypeID.TIMESTAMP_NANO) {
      return valueNode.asText();
    }

    return value;
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
  private static <T> UnboundTerm<T> termFromJson(JsonNode node) {
    if (node.isTextual()) {
      return Expressions.ref(node.asText());
    } else if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      switch (type) {
        case REFERENCE:
          return referenceFromJson(node, null);
        case TRANSFORM:
          UnboundTerm<T> child = termFromJson(JsonUtil.get(TERM, node));
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
}
