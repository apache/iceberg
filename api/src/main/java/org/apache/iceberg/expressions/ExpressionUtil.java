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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

/** Expression utility methods. */
public class ExpressionUtil {
  private static final Function<Object, Integer> HASH_FUNC =
      Transforms.bucket(Integer.MAX_VALUE).bind(Types.StringType.get());
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final long FIVE_MINUTES_IN_MICROS = TimeUnit.MINUTES.toMicros(5);
  private static final long THREE_DAYS_IN_HOURS = TimeUnit.DAYS.toHours(3);
  private static final long NINETY_DAYS_IN_HOURS = TimeUnit.DAYS.toHours(90);
  private static final Pattern DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
  private static final Pattern TIME = Pattern.compile("\\d{2}:\\d{2}(:\\d{2}(.\\d{1,9})?)?");
  private static final Pattern TIMESTAMP =
      Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}(:\\d{2}(.\\d{1,6})?)?");
  private static final Pattern TIMESTAMPNS =
      Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}(:\\d{2}(.\\d{7,9})?)?");
  private static final Pattern TIMESTAMPTZ =
      Pattern.compile(
          "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}(:\\d{2}(.\\d{1,6})?)?([-+]\\d{2}:\\d{2}|Z)");
  private static final Pattern TIMESTAMPTZNS =
      Pattern.compile(
          "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}(:\\d{2}(.\\d{7,9})?)?([-+]\\d{2}:\\d{2}|Z)");
  static final int LONG_IN_PREDICATE_ABBREVIATION_THRESHOLD = 10;
  private static final int LONG_IN_PREDICATE_ABBREVIATION_MIN_GAIN = 5;

  private ExpressionUtil() {}

  /**
   * Produces an unbound {@link Expression} with the same structure, but with data values replaced
   * by descriptions.
   *
   * <p>Numbers are replaced with magnitude and type, string-like values are replaced by hashes, and
   * date/time values are replaced by the type.
   *
   * @param expr an Expression to sanitize
   * @return a sanitized Expression
   */
  public static Expression sanitize(Expression expr) {
    return ExpressionVisitors.visit(expr, new ExpressionSanitizer());
  }

  /**
   * Produces an unbound {@link Expression} with the same structure, but with data values replaced
   * by descriptions.
   *
   * <p>Numbers are replaced with magnitude and type, string-like values are replaced by hashes, and
   * date/time values are replaced by the type.
   *
   * @param struct a StructType to bind the expression
   * @param expr an Expression to sanitize
   * @param caseSensitive whether to bind case sensitively
   * @return a sanitized Expression
   */
  public static Expression sanitize(
      Types.StructType struct, Expression expr, boolean caseSensitive) {
    try {
      Expression bound = Binder.bind(struct, expr, caseSensitive);
      return ExpressionVisitors.visit(bound, new ExpressionSanitizer());
    } catch (RuntimeException e) {
      // if the expression cannot be bound, sanitize the unbound version
      return ExpressionVisitors.visit(expr, new ExpressionSanitizer());
    }
  }

  /**
   * Produces a sanitized expression string with the same structure, but with data values replaced
   * by descriptions.
   *
   * <p>Numbers are replaced with magnitude and type, string-like values are replaced by hashes, and
   * date/time values are replaced by the type.
   *
   * @param expr an Expression to sanitize
   * @return a sanitized expression string
   */
  public static String toSanitizedString(Expression expr) {
    return ExpressionVisitors.visit(expr, new StringSanitizer());
  }

  /**
   * Produces a sanitized expression string with the same structure, but with data values replaced
   * by descriptions.
   *
   * <p>Numbers are replaced with magnitude and type, string-like values are replaced by hashes, and
   * date/time values are replaced by the type.
   *
   * @param struct a StructType to bind the expression
   * @param expr an Expression to sanitize
   * @param caseSensitive whether to bind case sensitively
   * @return a sanitized expression string
   */
  public static String toSanitizedString(
      Types.StructType struct, Expression expr, boolean caseSensitive) {
    try {
      Expression bound = Binder.bind(struct, expr, caseSensitive);
      return ExpressionVisitors.visit(bound, new StringSanitizer());
    } catch (RuntimeException e) {
      // if the expression cannot be bound, sanitize the unbound version
      return ExpressionVisitors.visit(expr, new StringSanitizer());
    }
  }

  /**
   * Extracts an expression that references only the given column IDs from the given expression.
   *
   * <p>The result is inclusive. If a row would match the original filter, it must match the result
   * filter.
   *
   * @param expression a filter Expression
   * @param schema a Schema
   * @param caseSensitive whether binding is case sensitive
   * @param ids field IDs used to match predicates to extract from the expression
   * @return an Expression that selects at least the same rows as the original using only the IDs
   */
  public static Expression extractByIdInclusive(
      Expression expression, Schema schema, boolean caseSensitive, int... ids) {
    PartitionSpec spec = identitySpec(schema, ids);
    return Projections.inclusive(spec, caseSensitive).project(Expressions.rewriteNot(expression));
  }

  /**
   * Returns whether two unbound expressions will accept the same inputs.
   *
   * <p>If this returns true, the expressions are guaranteed to return the same evaluation for the
   * same input. However, if this returns false the expressions may return the same evaluation for
   * the same input. That is, expressions may be equivalent even if this returns false.
   *
   * @param left an unbound expression
   * @param right an unbound expression
   * @param struct a struct type for binding
   * @param caseSensitive whether to bind expressions using case-sensitive matching
   * @return true if the expressions are equivalent
   */
  public static boolean equivalent(
      Expression left, Expression right, Types.StructType struct, boolean caseSensitive) {
    return Binder.bind(struct, Expressions.rewriteNot(left), caseSensitive)
        .isEquivalentTo(Binder.bind(struct, Expressions.rewriteNot(right), caseSensitive));
  }

  /**
   * Returns whether an expression selects whole partitions for all partition specs in a table.
   *
   * <p>For example, ts &lt; '2021-03-09T10:00:00.000' selects whole partitions in an hourly spec,
   * [hours(ts)], but does not select whole partitions in a daily spec, [days(ts)].
   *
   * @param expr an unbound expression
   * @param table a table
   * @param caseSensitive whether expression binding should be case sensitive
   * @return true if the expression will select whole partitions in all table specs
   */
  public static boolean selectsPartitions(Expression expr, Table table, boolean caseSensitive) {
    return table.specs().values().stream()
        .allMatch(spec -> selectsPartitions(expr, spec, caseSensitive));
  }

  /**
   * Returns whether an expression selects whole partitions for a partition spec.
   *
   * <p>For example, ts &lt; '2021-03-09T10:00:00.000' selects whole partitions in an hourly spec,
   * [hours(ts)], but does not select whole partitions in a daily spec, [days(ts)].
   *
   * @param expr an unbound expression
   * @param spec a partition spec
   * @return true if the expression will select whole partitions in the given spec
   */
  public static boolean selectsPartitions(
      Expression expr, PartitionSpec spec, boolean caseSensitive) {
    return equivalent(
        Projections.inclusive(spec, caseSensitive).project(expr),
        Projections.strict(spec, caseSensitive).project(expr),
        spec.partitionType(),
        caseSensitive);
  }

  public static String describe(Term term) {
    if (term instanceof UnboundTransform) {
      return ((UnboundTransform<?, ?>) term).transform()
          + "("
          + describe(((UnboundTransform<?, ?>) term).ref())
          + ")";
    } else if (term instanceof BoundTransform) {
      return ((BoundTransform<?, ?>) term).transform()
          + "("
          + describe(((BoundTransform<?, ?>) term).ref())
          + ")";
    } else if (term instanceof NamedReference) {
      return ((NamedReference<?>) term).name();
    } else if (term instanceof BoundReference) {
      return ((BoundReference<?>) term).name();
    } else {
      throw new UnsupportedOperationException("Unsupported term: " + term);
    }
  }

  public static <T> UnboundTerm<T> unbind(BoundTerm<T> term) {
    if (term instanceof BoundTransform) {
      BoundTransform<?, T> bound = (BoundTransform<?, T>) term;
      return Expressions.transform(bound.ref().name(), bound.transform());
    } else if (term instanceof BoundReference) {
      return Expressions.ref(((BoundReference<T>) term).name());
    }

    throw new UnsupportedOperationException("Cannot unbind unsupported term: " + term);
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> unbind(Term term) {
    if (term instanceof UnboundTerm) {
      return (UnboundTerm<T>) term;
    } else if (term instanceof BoundTerm) {
      return unbind((BoundTerm<T>) term);
    }

    throw new UnsupportedOperationException("Cannot unbind unsupported term: " + term);
  }

  private static class ExpressionSanitizer
      extends ExpressionVisitors.ExpressionVisitor<Expression> {
    private final long now;
    private final int today;

    private ExpressionSanitizer() {
      long nowMillis = System.currentTimeMillis();
      OffsetDateTime nowDateTime = Instant.ofEpochMilli(nowMillis).atOffset(ZoneOffset.UTC);
      this.now = nowMillis * 1000;
      this.today = (int) ChronoUnit.DAYS.between(EPOCH, nowDateTime);
    }

    @Override
    public Expression alwaysTrue() {
      return Expressions.alwaysTrue();
    }

    @Override
    public Expression alwaysFalse() {
      return Expressions.alwaysFalse();
    }

    @Override
    public Expression not(Expression result) {
      return Expressions.not(result);
    }

    @Override
    public Expression and(Expression leftResult, Expression rightResult) {
      return Expressions.and(leftResult, rightResult);
    }

    @Override
    public Expression or(Expression leftResult, Expression rightResult) {
      return Expressions.or(leftResult, rightResult);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Expression predicate(BoundPredicate<T> pred) {
      if (pred.isUnaryPredicate()) {
        // unary predicates don't need to be sanitized
        return new UnboundPredicate<>(pred.op(), unbind(pred.term()));
      } else if (pred.isLiteralPredicate()) {
        BoundLiteralPredicate<T> bound = (BoundLiteralPredicate<T>) pred;
        return new UnboundPredicate<>(
            pred.op(),
            unbind(pred.term()),
            (T) sanitize(bound.term().type(), bound.literal(), now, today));
      } else if (pred.isSetPredicate()) {
        BoundSetPredicate<T> bound = (BoundSetPredicate<T>) pred;
        Iterable<T> iter =
            () ->
                bound.literalSet().stream()
                    .map(lit -> (T) sanitize(bound.term().type(), lit, now, today))
                    .iterator();
        return new UnboundPredicate<>(pred.op(), unbind(pred.term()), iter);
      }

      throw new UnsupportedOperationException("Cannot sanitize bound predicate type: " + pred.op());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Expression predicate(UnboundPredicate<T> pred) {
      switch (pred.op()) {
        case IS_NULL:
        case NOT_NULL:
        case IS_NAN:
        case NOT_NAN:
          // unary predicates don't need to be sanitized
          return pred;
        case LT:
        case LT_EQ:
        case GT:
        case GT_EQ:
        case EQ:
        case NOT_EQ:
        case STARTS_WITH:
        case NOT_STARTS_WITH:
          return new UnboundPredicate<>(
              pred.op(), pred.term(), (T) sanitize(pred.literal(), now, today));
        case IN:
        case NOT_IN:
          Iterable<String> iter =
              () -> pred.literals().stream().map(lit -> sanitize(lit, now, today)).iterator();
          return new UnboundPredicate<>(pred.op(), pred.term(), (Iterable<T>) iter);
        default:
          throw new UnsupportedOperationException(
              "Cannot sanitize unsupported predicate type: " + pred.op());
      }
    }
  }

  private static class StringSanitizer extends ExpressionVisitors.ExpressionVisitor<String> {
    private final long nowMicros;
    private final int today;

    private StringSanitizer() {
      long nowMillis = System.currentTimeMillis();
      OffsetDateTime nowDateTime = Instant.ofEpochMilli(nowMillis).atOffset(ZoneOffset.UTC);
      this.nowMicros = nowMillis * 1000;
      this.today = (int) ChronoUnit.DAYS.between(EPOCH, nowDateTime);
    }

    @Override
    public String alwaysTrue() {
      return "true";
    }

    @Override
    public String alwaysFalse() {
      return "false";
    }

    @Override
    public String not(String result) {
      return "NOT (" + result + ")";
    }

    @Override
    public String and(String leftResult, String rightResult) {
      return "(" + leftResult + " AND " + rightResult + ")";
    }

    @Override
    public String or(String leftResult, String rightResult) {
      return "(" + leftResult + " OR " + rightResult + ")";
    }

    private String value(BoundLiteralPredicate<?> pred) {
      return sanitize(pred.term().type(), pred.literal().value(), nowMicros, today);
    }

    @Override
    public <T> String predicate(BoundPredicate<T> pred) {
      String term = describe(pred.term());
      switch (pred.op()) {
        case IS_NULL:
          return term + " IS NULL";
        case NOT_NULL:
          return term + " IS NOT NULL";
        case IS_NAN:
          return "is_nan(" + term + ")";
        case NOT_NAN:
          return "not_nan(" + term + ")";
        case LT:
          return term + " < " + value((BoundLiteralPredicate<?>) pred);
        case LT_EQ:
          return term + " <= " + value((BoundLiteralPredicate<?>) pred);
        case GT:
          return term + " > " + value((BoundLiteralPredicate<?>) pred);
        case GT_EQ:
          return term + " >= " + value((BoundLiteralPredicate<?>) pred);
        case EQ:
          return term + " = " + value((BoundLiteralPredicate<?>) pred);
        case NOT_EQ:
          return term + " != " + value((BoundLiteralPredicate<?>) pred);
        case IN:
          return term
              + " IN "
              + abbreviateValues(
                      pred.asSetPredicate().literalSet().stream()
                          .map(lit -> sanitize(pred.term().type(), lit, nowMicros, today))
                          .collect(Collectors.toList()))
                  .stream()
                  .collect(Collectors.joining(", ", "(", ")"));
        case NOT_IN:
          return term
              + " NOT IN "
              + abbreviateValues(
                      pred.asSetPredicate().literalSet().stream()
                          .map(lit -> sanitize(pred.term().type(), lit, nowMicros, today))
                          .collect(Collectors.toList()))
                  .stream()
                  .collect(Collectors.joining(", ", "(", ")"));
        case STARTS_WITH:
          return term + " STARTS WITH " + value((BoundLiteralPredicate<?>) pred);
        case NOT_STARTS_WITH:
          return term + " NOT STARTS WITH " + value((BoundLiteralPredicate<?>) pred);
        default:
          throw new UnsupportedOperationException(
              "Cannot sanitize unsupported predicate type: " + pred.op());
      }
    }

    @Override
    public <T> String predicate(UnboundPredicate<T> pred) {
      String term = describe(pred.term());
      switch (pred.op()) {
        case IS_NULL:
          return term + " IS NULL";
        case NOT_NULL:
          return term + " IS NOT NULL";
        case IS_NAN:
          return "is_nan(" + term + ")";
        case NOT_NAN:
          return "not_nan(" + term + ")";
        case LT:
          return term + " < " + sanitize(pred.literal(), nowMicros, today);
        case LT_EQ:
          return term + " <= " + sanitize(pred.literal(), nowMicros, today);
        case GT:
          return term + " > " + sanitize(pred.literal(), nowMicros, today);
        case GT_EQ:
          return term + " >= " + sanitize(pred.literal(), nowMicros, today);
        case EQ:
          return term + " = " + sanitize(pred.literal(), nowMicros, today);
        case NOT_EQ:
          return term + " != " + sanitize(pred.literal(), nowMicros, today);
        case IN:
          return term
              + " IN "
              + abbreviateValues(
                      pred.literals().stream()
                          .map(lit -> sanitize(lit, nowMicros, today))
                          .collect(Collectors.toList()))
                  .stream()
                  .collect(Collectors.joining(", ", "(", ")"));
        case NOT_IN:
          return term
              + " NOT IN "
              + abbreviateValues(
                      pred.literals().stream()
                          .map(lit -> sanitize(lit, nowMicros, today))
                          .collect(Collectors.toList()))
                  .stream()
                  .collect(Collectors.joining(", ", "(", ")"));
        case STARTS_WITH:
          return term + " STARTS WITH " + sanitize(pred.literal(), nowMicros, today);
        case NOT_STARTS_WITH:
          return term + " NOT STARTS WITH " + sanitize(pred.literal(), nowMicros, today);
        default:
          throw new UnsupportedOperationException(
              "Cannot sanitize unsupported predicate type: " + pred.op());
      }
    }
  }

  private static <T> List<String> abbreviateValues(List<String> sanitizedValues) {
    if (sanitizedValues.size() >= LONG_IN_PREDICATE_ABBREVIATION_THRESHOLD) {
      Set<String> distinctValues = ImmutableSet.copyOf(sanitizedValues);
      if (distinctValues.size()
          <= sanitizedValues.size() - LONG_IN_PREDICATE_ABBREVIATION_MIN_GAIN) {
        List<String> abbreviatedList = Lists.newArrayListWithCapacity(distinctValues.size() + 1);
        abbreviatedList.addAll(distinctValues);
        abbreviatedList.add(
            String.format(
                "... (%d values hidden, %d in total)",
                sanitizedValues.size() - distinctValues.size(), sanitizedValues.size()));
        return abbreviatedList;
      }
    }
    return sanitizedValues;
  }

  private static String sanitize(Type type, Object value, long now, int today) {
    switch (type.typeId()) {
      case INTEGER:
      case LONG:
        return sanitizeNumber((Number) value, "int");
      case FLOAT:
      case DOUBLE:
        return sanitizeNumber((Number) value, "float");
      case DATE:
        return sanitizeDate((int) value, today);
      case TIME:
        return "(time)";
      case TIMESTAMP:
        return sanitizeTimestamp(((Types.TimestampType) type).unit(), (long) value, now);
      case STRING:
        return sanitizeString((CharSequence) value, now, today);
      case BOOLEAN:
      case UUID:
      case DECIMAL:
      case FIXED:
      case BINARY:
        // for boolean, uuid, decimal, fixed, and binary, match the string result
        return sanitizeSimpleString(value.toString());
    }
    throw new UnsupportedOperationException(
        String.format("Cannot sanitize value for unsupported type %s: %s", type, value));
  }

  private static String sanitize(Literal<?> literal, long now, int today) {
    if (literal instanceof Literals.StringLiteral) {
      return sanitizeString(((Literals.StringLiteral) literal).value(), now, today);
    } else if (literal instanceof Literals.DateLiteral) {
      return sanitizeDate(((Literals.DateLiteral) literal).value(), today);
    } else if (literal instanceof Literals.TimestampLiteral) {
      Literals.TimestampLiteral tsLiteral = ((Literals.TimestampLiteral) literal);
      return sanitizeTimestamp(tsLiteral.unit(), tsLiteral.value(), now);
    } else if (literal instanceof Literals.TimeLiteral) {
      return "(time)";
    } else if (literal instanceof Literals.IntegerLiteral) {
      return sanitizeNumber(((Literals.IntegerLiteral) literal).value(), "int");
    } else if (literal instanceof Literals.LongLiteral) {
      return sanitizeNumber(((Literals.LongLiteral) literal).value(), "int");
    } else if (literal instanceof Literals.FloatLiteral) {
      return sanitizeNumber(((Literals.FloatLiteral) literal).value(), "float");
    } else if (literal instanceof Literals.DoubleLiteral) {
      return sanitizeNumber(((Literals.DoubleLiteral) literal).value(), "float");
    } else {
      // for uuid, decimal, fixed, and binary, match the string result
      return sanitizeSimpleString(literal.value().toString());
    }
  }

  private static String sanitizeDate(int days, int today) {
    String isPast = today > days ? "ago" : "from-now";
    int diff = Math.abs(today - days);
    if (diff == 0) {
      return "(date-today)";
    } else if (diff < 90) {
      return "(date-" + diff + "-days-" + isPast + ")";
    }

    return "(date)";
  }

  private static String sanitizeTimestamp(Types.TimestampType.Unit unit, long timeUnits, long now) {
    final long micros;
    switch (unit) {
      case MICROS:
        micros = timeUnits;
        break;
      case NANOS:
        micros = DateTimeUtil.nanosToMicros(timeUnits);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported timestamp unit: " + unit);
    }
    String isPast = now > micros ? "ago" : "from-now";
    long diff = Math.abs(now - micros);
    if (diff < FIVE_MINUTES_IN_MICROS) {
      return "(timestamp-about-now)";
    }

    long hours = TimeUnit.MICROSECONDS.toHours(diff);
    if (hours <= THREE_DAYS_IN_HOURS) {
      return "(timestamp-" + hours + "-hours-" + isPast + ")";
    } else if (hours < NINETY_DAYS_IN_HOURS) {
      long days = hours / 24;
      return "(timestamp-" + days + "-days-" + isPast + ")";
    }

    return "(timestamp)";
  }

  private static String sanitizeNumber(Number value, String type) {
    // log10 of zero isn't defined and will result in negative infinity
    int numDigits =
        0.0d == value.doubleValue() ? 1 : (int) Math.log10(Math.abs(value.doubleValue())) + 1;
    return "(" + numDigits + "-digit-" + type + ")";
  }

  private static String sanitizeString(CharSequence value, long now, int today) {
    try {
      if (DATE.matcher(value).matches()) {
        Literal<Integer> date = Literal.of(value).to(Types.DateType.get());
        return sanitizeDate(date.value(), today);
      } else if (TIMESTAMP.matcher(value).matches()) {
        Literal<Long> ts = Literal.of(value).to(Types.TimestampType.microsWithoutZone());
        return sanitizeTimestamp(Types.TimestampType.Unit.MICROS, ts.value(), now);
      } else if (TIMESTAMPNS.matcher(value).matches()) {
        Literal<Long> ts = Literal.of(value).to(Types.TimestampType.nanosWithoutZone());
        return sanitizeTimestamp(Types.TimestampType.Unit.NANOS, ts.value(), now);
      } else if (TIMESTAMPTZ.matcher(value).matches()) {
        Literal<Long> ts = Literal.of(value).to(Types.TimestampType.microsWithZone());
        return sanitizeTimestamp(Types.TimestampType.Unit.MICROS, ts.value(), now);
      } else if (TIMESTAMPTZNS.matcher(value).matches()) {
        Literal<Long> ts = Literal.of(value).to(Types.TimestampType.nanosWithZone());
        return sanitizeTimestamp(Types.TimestampType.Unit.NANOS, ts.value(), now);
      } else if (TIME.matcher(value).matches()) {
        return "(time)";
      } else {
        return sanitizeSimpleString(value);
      }
    } catch (Exception ex) {
      // Don't throw when parsing failed in sanitizeString
      // because user could provide an invalid integer/date/timestamp string
      // and expect them to be treated as a string instead of specific type
      return sanitizeSimpleString(value);
    }
  }

  private static String sanitizeSimpleString(CharSequence value) {
    // hash the value and return the hash as hex
    return String.format("(hash-%08x)", HASH_FUNC.apply(value));
  }

  private static PartitionSpec identitySpec(Schema schema, int... ids) {
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);

    for (int id : ids) {
      specBuilder.identity(schema.findColumnName(id));
    }

    return specBuilder.build();
  }
}
