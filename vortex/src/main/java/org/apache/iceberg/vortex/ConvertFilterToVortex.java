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
package org.apache.iceberg.vortex;

import dev.vortex.api.Expression;
import dev.vortex.api.Expression.BinaryOp;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

/**
 * Convert an Iceberg filter expression into a valid Vortex pruning predicate that can be pushed
 * into the scan node.
 *
 * <p>A pushed-down filter only has to avoid reading rows that cannot match: {@code
 * ReadBuilder#filter} lets a reader return unfiltered or partially filtered rows and makes the
 * caller re-apply the filter. Every expression produced here is therefore allowed to match more
 * rows than the Iceberg predicate, but must never match fewer, and anything that cannot be
 * translated within that rule becomes {@code ALWAYS_TRUE}.
 */
public final class ConvertFilterToVortex extends ExpressionVisitors.ExpressionVisitor<Expression> {
  static final Expression ALWAYS_TRUE = Expression.literal(true);
  static final Expression ALWAYS_FALSE = Expression.literal(false);
  // Sentinel distinct by reference from ALWAYS_TRUE/FALSE, used to mark
  // sub-expressions that
  // could not be translated to a Vortex expression.
  static final Expression UNCONVERTIBLE = Expression.literal(true);

  private final Schema expectedSchema;
  private final Schema fileSchema;
  private final Map<Integer, Types.NestedField> fileFields;
  private final boolean caseSensitive;

  private ConvertFilterToVortex(
      Schema expectedSchema,
      Schema fileSchema,
      Map<Integer, Types.NestedField> fileFields,
      boolean caseSensitive) {
    this.expectedSchema = expectedSchema;
    this.fileSchema = fileSchema;
    this.fileFields = fileFields;
    this.caseSensitive = caseSensitive;
  }

  public static Expression convert(Schema schema, org.apache.iceberg.expressions.Expression expr) {
    return convert(schema, expr, true);
  }

  /** Converts a filter for a file whose columns carry the same names, ids and types as schema. */
  public static Expression convert(
      Schema schema, org.apache.iceberg.expressions.Expression expr, boolean caseSensitive) {
    Map<Integer, Types.NestedField> fileFields = TypeUtil.indexById(schema.asStruct());
    return convert(new ConvertFilterToVortex(schema, schema, fileFields, caseSensitive), expr);
  }

  /**
   * Converts a filter written against the expected read schema for a file. {@code fileSchema} is
   * the file's schema converted to Iceberg, with synthesized ids, and {@code fileArrowSchema} is
   * the same schema with the file's Iceberg ids attached.
   *
   * <p>The filter binds to the expected schema, and each column then resolves to the file column
   * with the same Iceberg id (or the same name when the file carries no ids), the same way the scan
   * projection does. A column renamed since the file was written therefore filters its physical
   * column, and a predicate on a column the file does not have is left to the engine.
   */
  static Expression convert(
      Schema expectedSchema,
      Schema fileSchema,
      org.apache.arrow.vector.types.pojo.Schema fileArrowSchema,
      org.apache.iceberg.expressions.Expression expr,
      boolean caseSensitive) {
    Map<Integer, Types.NestedField> fileFields = Maps.newHashMap();
    indexFileFields(
        expectedSchema.asStruct(), fileArrowSchema.getFields(), fileSchema.asStruct(), fileFields);

    return convert(
        new ConvertFilterToVortex(expectedSchema, fileSchema, fileFields, caseSensitive), expr);
  }

  /**
   * Maps each expected field id to the matching field of the converted file schema. The converted
   * schema has the file's names and types but synthesized ids, so the match goes through the Arrow
   * fields, which carry the real ids.
   */
  private static void indexFileFields(
      Types.StructType expected,
      List<Field> arrowFields,
      Types.StructType file,
      Map<Integer, Types.NestedField> fileFields) {
    VortexSchemas.FieldBinding binding = VortexSchemas.FieldBinding.of(arrowFields);
    for (Types.NestedField expectedField : expected.fields()) {
      Field arrowField = binding.resolve(expectedField);
      Types.NestedField fileField = arrowField == null ? null : file.field(arrowField.getName());
      if (fileField == null) {
        continue;
      }

      fileFields.put(expectedField.fieldId(), fileField);

      if (expectedField.type().isStructType() && fileField.type().isStructType()) {
        indexFileFields(
            expectedField.type().asStructType(),
            arrowField.getChildren(),
            fileField.type().asStructType(),
            fileFields);
      }
    }
  }

  private static Expression convert(
      ConvertFilterToVortex visitor, org.apache.iceberg.expressions.Expression expr) {
    org.apache.iceberg.expressions.Expression pushedNot = Expressions.rewriteNot(expr);
    Expression converted = ExpressionVisitors.visit(pushedNot, visitor);
    if (converted == UNCONVERTIBLE) {
      return ALWAYS_TRUE;
    } else {
      return converted;
    }
  }

  @Override
  public Expression alwaysFalse() {
    return ALWAYS_FALSE;
  }

  @Override
  public Expression alwaysTrue() {
    return ALWAYS_TRUE;
  }

  @Override
  public Expression not(Expression child) {
    if (child == ALWAYS_TRUE) {
      return ALWAYS_FALSE;
    } else if (child == ALWAYS_FALSE) {
      return ALWAYS_TRUE;
    } else if (child == UNCONVERTIBLE) {
      return UNCONVERTIBLE;
    } else {
      return Expression.not(child);
    }
  }

  @Override
  public Expression and(Expression leftResult, Expression rightResult) {
    if (leftResult == UNCONVERTIBLE && rightResult == UNCONVERTIBLE) {
      return ALWAYS_TRUE;
    } else if (leftResult == UNCONVERTIBLE) {
      return rightResult;
    } else if (rightResult == UNCONVERTIBLE) {
      return leftResult;
    } else {
      return Expression.and(leftResult, rightResult);
    }
  }

  @Override
  public Expression or(Expression leftResult, Expression rightResult) {
    if (leftResult == UNCONVERTIBLE || rightResult == UNCONVERTIBLE) {
      return ALWAYS_TRUE;
    } else {
      return Expression.or(leftResult, rightResult);
    }
  }

  @Override
  public <T> Expression predicate(BoundPredicate<T> pred) {
    if (!(pred.term() instanceof BoundReference<T> term)) {
      // Transform terms (bucket, truncate, ...) have no Vortex equivalent. Drop the predicate
      // rather than failing the scan: the caller re-applies the filter anyway.
      return UNCONVERTIBLE;
    }

    Types.NestedField fileField = fileFields.get(term.fieldId());
    if (fileField == null) {
      // The file has no such column, e.g. one added after the file was written. The reader fills
      // it with a default or null, which the engine filters.
      return UNCONVERTIBLE;
    } else if (fileField.type().equals(term.type())) {
      return filePredicate(pred, fileField);
    } else {
      return promotedPredicate(pred, fileField);
    }
  }

  /**
   * Converts a predicate on a column written before a type promotion, by binding it again to the
   * type the file holds.
   *
   * <p>Only int to long is handled: a long literal converts to int exactly, or to an above-max or
   * below-max bound that folds the predicate to always true or false. Literals of the other
   * promotions round or lose precision when converted to the file type, which can drop matching
   * rows, so those predicates are left to the engine.
   */
  private <T> Expression promotedPredicate(BoundPredicate<T> pred, Types.NestedField fileField) {
    if (pred.term().type().typeId() != Type.TypeID.LONG
        || fileField.type().typeId() != Type.TypeID.INTEGER) {
      return UNCONVERTIBLE;
    }

    String name = fileSchema.findColumnName(fileField.fieldId());
    UnboundPredicate<T> filePred;
    if (pred.isLiteralPredicate()) {
      filePred = Expressions.predicate(pred.op(), name, pred.asLiteralPredicate().literal());
    } else if (pred.isSetPredicate()) {
      filePred = Expressions.predicate(pred.op(), name, pred.asSetPredicate().literalSet());
    } else {
      filePred = Expressions.predicate(pred.op(), name);
    }

    org.apache.iceberg.expressions.Expression bound = filePred.bind(fileSchema.asStruct(), true);
    if (bound instanceof BoundPredicate<?> boundPred) {
      return filePredicate(boundPred, fileField);
    } else if (bound == Expressions.alwaysTrue()) {
      return ALWAYS_TRUE;
    } else if (bound == Expressions.alwaysFalse()) {
      return ALWAYS_FALSE;
    }

    return UNCONVERTIBLE;
  }

  /** Converts a predicate whose term has the type of {@code fileField}. */
  private <T> Expression filePredicate(BoundPredicate<T> pred, Types.NestedField fileField) {
    Type termType = pred.term().type();
    Expression vortexTerm = column(fileField.fieldId());
    if (vortexTerm == UNCONVERTIBLE) {
      return UNCONVERTIBLE;
    }

    if (pred.isLiteralPredicate()) {
      org.apache.iceberg.expressions.Literal<T> icebergLit = pred.asLiteralPredicate().literal();
      if (pred.op() == Operation.STARTS_WITH || pred.op() == Operation.NOT_STARTS_WITH) {
        return fromStartsWith(pred.op(), vortexTerm, icebergLit.value(), termType);
      }

      Expression vortexLit = toVortexLiteral(icebergLit.value(), termType);
      if (vortexLit == UNCONVERTIBLE) {
        return UNCONVERTIBLE;
      }
      return fromBinaryPredicate(pred.op(), vortexTerm, vortexLit);
    } else if (pred.isUnaryPredicate()) {
      return fromUnaryPredicate(pred.op(), vortexTerm);
    } else if (pred.isSetPredicate()) {
      return fromSetPredicate(pred.op(), vortexTerm, pred.asSetPredicate().literalSet(), termType);
    } else {
      return UNCONVERTIBLE;
    }
  }

  @Override
  public <T> Expression predicate(UnboundPredicate<T> pred) {
    org.apache.iceberg.expressions.Expression bound = bind(pred);
    if (bound instanceof BoundPredicate) {
      return predicate((BoundPredicate<?>) bound);
    } else if (bound == Expressions.alwaysTrue()) {
      return ALWAYS_TRUE;
    } else if (bound == Expressions.alwaysFalse()) {
      return ALWAYS_FALSE;
    }
    return UNCONVERTIBLE;
  }

  private org.apache.iceberg.expressions.Expression bind(UnboundPredicate<?> pred) {
    return pred.bind(expectedSchema.asStruct(), caseSensitive);
  }

  private Expression column(int fieldId) {
    List<String> path = Lists.newArrayList();
    if (findFieldPath(fileSchema.asStruct(), fieldId, path)) {
      return Expression.column(path.toArray(String[]::new));
    }

    return UNCONVERTIBLE;
  }

  private static boolean findFieldPath(Types.StructType struct, int fieldId, List<String> path) {
    for (Types.NestedField field : struct.fields()) {
      path.add(field.name());
      if (field.fieldId() == fieldId) {
        return true;
      }

      if (field.type().isStructType()
          && findFieldPath(field.type().asStructType(), fieldId, path)) {
        return true;
      }

      path.remove(path.size() - 1);
    }

    return false;
  }

  /**
   * Convert an Iceberg value to a Vortex literal Expression. Returns {@link #UNCONVERTIBLE} if no
   * matching Vortex literal type exists.
   */
  static Expression toVortexLiteral(Object value, Type termType) {
    if (value == null) {
      return toVortexNullLiteral(termType);
    }
    return switch (termType.typeId()) {
      case BOOLEAN -> Expression.literal((Boolean) value);
      case INTEGER -> Expression.literal((Integer) value);
      case LONG -> Expression.literal((Long) value);
      case FLOAT -> Expression.literal((Float) value);
      case DOUBLE -> Expression.literal((Double) value);
      case STRING -> Expression.literal(((CharSequence) value).toString());
      case BINARY, FIXED -> Expression.literal(ByteBuffers.toByteArray((ByteBuffer) value));
      case DECIMAL -> {
        BigDecimal decimal = (BigDecimal) value;
        Types.DecimalType decimalType = (Types.DecimalType) termType;
        yield Expression.literalDecimal(
            decimal.unscaledValue(), decimalType.precision(), decimalType.scale());
      }
      case DATE -> Expression.literalDate((Integer) value, Expression.TimeUnit.DAYS);
      case TIME -> Expression.literalTime((Long) value, Expression.TimeUnit.MICROSECONDS);
      case TIMESTAMP -> {
        Types.TimestampType timestampType = (Types.TimestampType) termType;
        String timezone = timestampType.shouldAdjustToUTC() ? "UTC" : null;
        yield Expression.literalTimestamp((Long) value, Expression.TimeUnit.MICROSECONDS, timezone);
      }
      case TIMESTAMP_NANO -> {
        Types.TimestampNanoType timestampType = (Types.TimestampNanoType) termType;
        String timezone = timestampType.shouldAdjustToUTC() ? "UTC" : null;
        yield Expression.literalTimestamp((Long) value, Expression.TimeUnit.NANOSECONDS, timezone);
      }
      case UUID -> Expression.literal((UUID) value);
      default -> UNCONVERTIBLE;
    };
  }

  private static Expression toVortexNullLiteral(Type termType) {
    return switch (termType.typeId()) {
      case BOOLEAN -> Expression.nullLiteral(Expression.DType.BOOL);
      case INTEGER -> Expression.nullLiteral(Expression.DType.I32);
      case LONG -> Expression.nullLiteral(Expression.DType.I64);
      case FLOAT -> Expression.nullLiteral(Expression.DType.F32);
      case DOUBLE -> Expression.nullLiteral(Expression.DType.F64);
      case STRING -> Expression.nullLiteral(Expression.DType.UTF8);
      case BINARY, FIXED -> Expression.nullLiteral(Expression.DType.BINARY);
      case DECIMAL -> {
        Types.DecimalType decimalType = (Types.DecimalType) termType;
        yield Expression.nullLiteralDecimal(decimalType.precision(), decimalType.scale());
      }
      case DATE -> Expression.nullLiteralDate(Expression.TimeUnit.DAYS);
      case TIME -> Expression.nullLiteralTime(Expression.TimeUnit.MICROSECONDS);
      case TIMESTAMP -> {
        Types.TimestampType timestampType = (Types.TimestampType) termType;
        String timezone = timestampType.shouldAdjustToUTC() ? "UTC" : null;
        yield Expression.nullLiteralTimestamp(Expression.TimeUnit.MICROSECONDS, timezone);
      }
      case TIMESTAMP_NANO -> {
        Types.TimestampNanoType timestampType = (Types.TimestampNanoType) termType;
        String timezone = timestampType.shouldAdjustToUTC() ? "UTC" : null;
        yield Expression.nullLiteralTimestamp(Expression.TimeUnit.NANOSECONDS, timezone);
      }
      case UUID -> Expression.nullLiteralUuid();
      default -> UNCONVERTIBLE;
    };
  }

  private Expression fromBinaryPredicate(
      org.apache.iceberg.expressions.Expression.Operation op, Expression left, Expression right) {
    return switch (op) {
      case TRUE -> ALWAYS_TRUE;
      case FALSE -> ALWAYS_FALSE;
      case LT -> Expression.binary(BinaryOp.LT, left, right);
      case LT_EQ -> Expression.binary(BinaryOp.LTE, left, right);
      case GT -> Expression.binary(BinaryOp.GT, left, right);
      case GT_EQ -> Expression.binary(BinaryOp.GTE, left, right);
      case EQ -> Expression.binary(BinaryOp.EQ, left, right);
      case NOT_EQ -> Expression.binary(BinaryOp.NOT_EQ, left, right);
      case AND -> Expression.binary(BinaryOp.AND, left, right);
      case OR -> Expression.binary(BinaryOp.OR, left, right);
      default -> UNCONVERTIBLE;
    };
  }

  private Expression fromUnaryPredicate(
      org.apache.iceberg.expressions.Expression.Operation op, Expression child) {
    return switch (op) {
      case TRUE -> ALWAYS_TRUE;
      case FALSE -> ALWAYS_FALSE;
      case IS_NULL -> Expression.isNull(child);
      case NOT_NULL -> Expression.not(Expression.isNull(child));
      case NOT -> {
        if (child == ALWAYS_TRUE) {
          yield ALWAYS_FALSE;
        } else if (child == ALWAYS_FALSE) {
          yield ALWAYS_TRUE;
        } else {
          yield Expression.not(child);
        }
      }
        // IS_NAN / NOT_NAN: Vortex compares NaN as equal to itself, so `f == f` matches NaN rows
        // and `f != f` matches none. Nothing available here isolates NaN, and every near miss
        // drops rows the predicate matches, so the predicate is left to the engine.
      default -> UNCONVERTIBLE;
    };
  }

  /**
   * Translates STARTS_WITH / NOT_STARTS_WITH into a Vortex LIKE over a prefix pattern.
   *
   * <p>Vortex LIKE treats {@code %} and {@code _} as wildcards and {@code \\} as the escape
   * character, and silently drops a backslash that does not introduce a valid escape, so every one
   * of those characters in the prefix has to be escaped.
   *
   * <p>NOT LIKE does not match null, but Iceberg's {@code notStartsWith} does (it is defined as the
   * negation of {@code startsWith}, which is false for null), so nulls are added back explicitly.
   */
  private Expression fromStartsWith(Operation op, Expression term, Object prefix, Type termType) {
    if (termType.typeId() != Type.TypeID.STRING) {
      return UNCONVERTIBLE;
    }

    Expression pattern = Expression.literal(likePrefixPattern(prefix.toString()));
    if (op == Operation.STARTS_WITH) {
      return Expression.like(term, pattern, false, false);
    }

    return Expression.or(Expression.isNull(term), Expression.like(term, pattern, true, false));
  }

  private static String likePrefixPattern(String prefix) {
    StringBuilder pattern = new StringBuilder(prefix.length() + 8);
    for (int index = 0; index < prefix.length(); index++) {
      char character = prefix.charAt(index);
      if (character == '\\' || character == '%' || character == '_') {
        pattern.append('\\');
      }

      pattern.append(character);
    }

    return pattern.append('%').toString();
  }

  /**
   * Translates IN / NOT_IN into a Vortex list membership test against a list literal.
   *
   * <p>Vortex NOT IN uses SQL null rules and does not match null, but Iceberg's {@code notIn} does
   * (it is defined as the negation of {@code in}, which is false for null), so nulls are added back
   * explicitly.
   */
  private <T> Expression fromSetPredicate(
      Operation op, Expression term, Set<T> literalSet, Type termType) {
    Expression elementType = toVortexNullLiteral(termType);
    if (elementType == UNCONVERTIBLE) {
      return UNCONVERTIBLE;
    }

    Expression[] elements = new Expression[literalSet.size()];
    int idx = 0;
    for (T value : literalSet) {
      Expression vortexLit = toVortexLiteral(value, termType);
      if (vortexLit == UNCONVERTIBLE) {
        return UNCONVERTIBLE;
      }

      elements[idx++] = vortexLit;
    }

    Expression list = Expression.literalList(elementType, false, elements);
    return switch (op) {
      case IN -> Expression.in(term, list);
      case NOT_IN -> Expression.or(Expression.isNull(term), Expression.notIn(term, list));
      default -> UNCONVERTIBLE;
    };
  }
}
