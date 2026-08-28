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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.transforms.UnknownTransform;
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
  private static final String VOID = "void";
  // bucket and truncate take the transform parameter as their first argument, so they cannot be
  // resolved from a name alone
  private static final Set<String> PARAMETERIZED_TRANSFORMS = ImmutableSet.of("bucket", "truncate");

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
              writeValueExpr(pred.term());
            } else if (pred.isLiteralPredicate()) {
              gen.writeFieldName(LEFT);
              writeValueExpr(pred.term());
              gen.writeFieldName(RIGHT);
              SingleValueParser.toJson(
                  pred.term().type(), pred.asLiteralPredicate().literal().value(), gen);
            } else if (pred.isSetPredicate()) {
              gen.writeFieldName(CHILD);
              writeValueExpr(pred.term());
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
              writeValueExpr(pred.term());
              gen.writeArrayFieldStart(VALUES);
              if (pred.literals() != null) {
                for (Literal<T> lit : pred.literals()) {
                  unboundLiteral(lit.value());
                }
              }
              gen.writeEndArray();
            } else if (pred.literals() == null || pred.literals().isEmpty()) {
              gen.writeFieldName(CHILD);
              writeValueExpr(pred.term());
            } else {
              gen.writeFieldName(LEFT);
              writeValueExpr(pred.term());
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
      return op.toString().replaceAll("_", "-").toLowerCase(Locale.ENGLISH);
    }

    private void writeValueExpr(Term term) throws IOException {
      if (term instanceof UnboundApply) {
        writeApply((UnboundApply<?>) term);
      } else if (term instanceof UnboundTransform) {
        UnboundTransform<?, ?> transform = (UnboundTransform<?, ?>) term;
        writeTransformAsApply(transform.transform().toString(), transform.ref());
      } else if (term instanceof BoundTransform) {
        BoundTransform<?, ?> transform = (BoundTransform<?, ?>) term;
        writeTransformAsApply(transform.transform().toString(), transform.ref());
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
    private void writeTransformAsApply(String transformStr, Term ref) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, APPLY);

      Matcher matcher = HAS_WIDTH.matcher(transformStr);
      boolean parameterized = matcher.matches();
      gen.writeStringField(FUNCTION, parameterized ? matcher.group(1) : transformStr);

      gen.writeArrayFieldStart(ARGUMENTS);
      if (parameterized) {
        gen.writeNumber(Integer.parseInt(matcher.group(2)));
      }
      writeValueExpr(ref);
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
          writeValueExpr((Term) arg);
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
        gen.writeFieldName(FUNCTION);
        gen.writeStartArray();
        for (String part : ref.identifier()) {
          gen.writeString(part);
        }
        gen.writeEndArray();
      } else {
        gen.writeFieldName(FUNCTION);
        gen.writeStartObject();
        gen.writeStringField(CATALOG, ref.catalog());
        gen.writeFieldName(IDENTIFIER);
        gen.writeStartArray();
        for (String part : ref.identifier()) {
          gen.writeString(part);
        }
        gen.writeEndArray();
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
      return deprecatedPredicateFromJson(op, json, schema);
    } else {
      return predicateFromJson(op, json, schema);
    }
  }

  private static Expression.Operation fromType(String type) {
    return Expression.Operation.fromString(type.replaceAll("-", "_"));
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundPredicate<T> predicateFromJson(
      Expression.Operation op, JsonNode node, Schema schema) {
    switch (op) {
      case IS_NULL:
      case NOT_NULL:
      case IS_NAN:
      case NOT_NAN:
        {
          UnboundTerm<T> child = valueExprFromJson(JsonUtil.get(CHILD, node), schema);
          return Expressions.predicate(op, child);
        }
      case LT:
      case LT_EQ:
      case GT:
      case GT_EQ:
      case EQ:
      case NOT_EQ:
      case STARTS_WITH:
      case NOT_STARTS_WITH:
        {
          UnboundTerm<T> left = valueExprFromJson(JsonUtil.get(LEFT, node), schema);
          Function<JsonNode, T> convertValue = valueConverter(left, schema);
          T value = literalFromJson(JsonUtil.get(RIGHT, node), convertValue);
          return Expressions.predicate(op, left, ImmutableList.of(value));
        }
      case IN:
      case NOT_IN:
        {
          UnboundTerm<T> child = valueExprFromJson(JsonUtil.get(CHILD, node), schema);
          Function<JsonNode, T> convertValue = valueConverter(child, schema);
          JsonNode valuesNode = JsonUtil.get(VALUES, node);
          Iterable<T> values = literalsFromJson(valuesNode, convertValue);
          return Expressions.predicate(op, child, values);
        }
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + op);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundPredicate<T> deprecatedPredicateFromJson(
      Expression.Operation op, JsonNode node, Schema schema) {
    UnboundTerm<T> term = deprecatedTermFromJson(JsonUtil.get(TERM, node));

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
        return Expressions.predicate(
            op,
            term,
            Iterables.transform(
                ((ArrayNode) valuesNode)::elements,
                valueNode -> literalFromJson(valueNode, convertValue)));
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + op);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Function<JsonNode, T> valueConverter(UnboundTerm<T> term, Schema schema) {
    if (schema != null) {
      BoundTerm<?> bound = term.bind(schema.asStruct(), false);
      return valueNode -> (T) SingleValueParser.fromJson(bound.type(), valueNode);
    } else {
      return valueNode -> (T) ExpressionParser.asObject(valueNode);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> valueExprFromJson(JsonNode node, Schema schema) {
    if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      switch (type) {
        case REFERENCE:
          return referenceFromJson(node, schema);
        case APPLY:
          return applyFromJson(node, schema);
        default:
          throw new IllegalArgumentException("Unknown value expression type: " + type);
      }
    }

    // a bare string is a literal value, which cannot be a predicate operand
    throw new IllegalArgumentException(
        "Cannot parse value expression, expected a reference or apply: " + node);
  }

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> applyFromJson(JsonNode node, Schema schema) {
    FunctionReference funcRef = functionRefFromJson(JsonUtil.get(FUNCTION, node));
    List<Object> arguments = Lists.newArrayList();

    if (node.has(ARGUMENTS)) {
      JsonNode argsNode = JsonUtil.get(ARGUMENTS, node);
      Preconditions.checkArgument(
          argsNode.isArray(), "Apply arguments must be an array: %s", argsNode);
      for (JsonNode argNode : argsNode) {
        arguments.add(parseApplyArgument(argNode, schema));
      }
    }

    UnboundApply<T> apply = Expressions.apply(funcRef, arguments);

    if (isTransformFunction(funcRef)) {
      return transformFromApply(apply);
    }

    return apply;
  }

  /**
   * Returns whether a function reference is an Iceberg partition transform.
   *
   * <p>The expressions spec defines Iceberg partition transforms as functions in the {@code
   * iceberg_functions} catalog, other than {@code void}.
   */
  private static boolean isTransformFunction(FunctionReference function) {
    if (function.catalog() != null && !function.catalog().equalsIgnoreCase(ICEBERG_FUNCTIONS)) {
      return false;
    }

    String name = function.name().toLowerCase(Locale.ROOT);
    if (VOID.equals(name)) {
      return false;
    } else if (PARAMETERIZED_TRANSFORMS.contains(name)) {
      return true;
    }

    return !(Transforms.fromString(name) instanceof UnknownTransform);
  }

  /**
   * Converts an apply expression that calls an Iceberg partition transform to an {@link
   * UnboundTransform}.
   *
   * <p>Parameterized transforms are called as two-argument functions with the transform parameter
   * first, like {@code bucket(16, ref)}.
   */
  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> transformFromApply(UnboundApply<T> apply) {
    FunctionReference function = apply.function();
    String name = function.name().toLowerCase(Locale.ROOT);
    boolean parameterized = PARAMETERIZED_TRANSFORMS.contains(name);

    List<Object> arguments = apply.arguments();
    int expectedArgs = parameterized ? 2 : 1;
    Preconditions.checkArgument(
        arguments.size() == expectedArgs,
        "Cannot convert %s to a transform: expected %s argument(s), got %s",
        function,
        expectedArgs,
        arguments.size());

    String transform = name;
    if (parameterized) {
      Object parameter = arguments.get(0);
      Preconditions.checkArgument(
          parameter instanceof Literal && ((Literal<?>) parameter).value() instanceof Number,
          "Cannot convert %s to a transform: first argument must be a number, got %s",
          function,
          parameter);
      transform = name + "[" + ((Number) ((Literal<?>) parameter).value()).intValue() + "]";
    }

    Object valueArg = arguments.get(expectedArgs - 1);
    Preconditions.checkArgument(
        valueArg instanceof NamedReference,
        "Cannot convert %s to a transform: last argument must be a reference, got %s",
        function,
        valueArg);

    return (UnboundTerm<T>)
        Expressions.transform(
            ((NamedReference<?>) valueArg).name(), Transforms.fromString(transform));
  }

  private static Object parseApplyArgument(JsonNode node, Schema schema) {
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
      switch (type) {
        case REFERENCE:
          return referenceFromJson(node, schema);
        case APPLY:
          return applyFromJson(node, schema);
        case LITERAL:
          return literalFromJson(node, ExpressionParser::asObject);
        default:
          return fromJson(node, schema);
      }
    }

    throw new IllegalArgumentException("Cannot parse apply argument: " + node);
  }

  private static FunctionReference functionRefFromJson(JsonNode node) {
    if (node.isTextual()) {
      return Expressions.function(node.asText());
    } else if (node.isArray()) {
      return Expressions.function(namesFromJson(node));
    } else if (node.isObject()) {
      String catalog = node.hasNonNull(CATALOG) ? JsonUtil.getString(CATALOG, node) : null;
      List<String> identifier = namesFromJson(JsonUtil.get(IDENTIFIER, node));
      return catalog != null
          ? Expressions.function(catalog, identifier)
          : Expressions.function(identifier);
    }

    throw new IllegalArgumentException("Cannot parse function reference: " + node);
  }

  private static List<String> namesFromJson(JsonNode node) {
    Preconditions.checkArgument(
        node.isArray(), "Cannot parse function identifier from non-array: %s", node);
    List<String> names = Lists.newArrayList();
    for (JsonNode name : node) {
      names.add(name.asText());
    }

    return names;
  }

  @SuppressWarnings("unchecked")
  private static <T> T literalFromJson(JsonNode valueNode, Function<JsonNode, T> toValue) {
    if (valueNode.isObject() && valueNode.has(TYPE)) {
      String type = JsonUtil.getString(TYPE, valueNode);
      Preconditions.checkArgument(
          type.equalsIgnoreCase(LITERAL), "Cannot parse type as a literal: %s", type);
      JsonNode value = JsonUtil.get(VALUE, valueNode);
      if (valueNode.hasNonNull(DATA_TYPE)) {
        return (T) typedValueFromJson(dataTypeFromJson(valueNode), value);
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
            valueNode -> (T) typedValueFromJson(dataType, valueNode));
      }

      return Iterables.transform(((ArrayNode) valuesNode)::elements, convertValue::apply);
    }

    throw new IllegalArgumentException("Cannot parse literals: " + node);
  }

  private static Type dataTypeFromJson(JsonNode node) {
    return Types.fromPrimitiveString(JsonUtil.getString(DATA_TYPE, node));
  }

  private static Object typedValueFromJson(Type dataType, JsonNode valueNode) {
    Object value = SingleValueParser.fromJson(dataType, valueNode);
    Preconditions.checkArgument(value != null, "Cannot parse %s literal from null value", dataType);
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
  private static <T> UnboundTerm<T> deprecatedTermFromJson(JsonNode node) {
    if (node.isTextual()) {
      return Expressions.ref(node.asText());
    } else if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      switch (type) {
        case REFERENCE:
          if (node.has(NAME)) {
            return Expressions.ref(JsonUtil.getString(NAME, node));
          }
          return Expressions.ref(JsonUtil.getString(TERM, node));
        case TRANSFORM:
          UnboundTerm<T> child = deprecatedTermFromJson(JsonUtil.get(TERM, node));
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
