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

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

/** Expression utility methods. */
public class ExpressionUtil {
  private static final Function<Object, Integer> HASH_FUNC =
      Transforms.bucket(Integer.MAX_VALUE).bind(Types.StringType.get());
  private static final Pattern DATE = Pattern.compile("\\d\\d\\d\\d-\\d\\d-\\d\\d");
  private static final Pattern TIME = Pattern.compile("\\d\\d:\\d\\d(:\\d\\d(.\\d{1,6})?)?");
  private static final Pattern TIMESTAMP =
      Pattern.compile(
          "\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d(:\\d\\d(.\\d{1,6})?)?([-+]\\d\\d:\\d\\d)?");
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
    return ExpressionVisitors.visit(expr, ExpressionSanitizer.INSTANCE);
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
    return ExpressionVisitors.visit(expr, StringSanitizer.INSTANCE);
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

  private static class ExpressionSanitizer
      extends ExpressionVisitors.ExpressionVisitor<Expression> {
    private static final ExpressionSanitizer INSTANCE = new ExpressionSanitizer();

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
    public <T> Expression predicate(BoundPredicate<T> pred) {
      throw new UnsupportedOperationException("Cannot sanitize bound predicate: " + pred);
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
          return new UnboundPredicate<>(pred.op(), pred.term(), (T) sanitize(pred.literal()));
        case IN:
        case NOT_IN:
          Iterable<String> iter =
              () -> pred.literals().stream().map(ExpressionUtil::sanitize).iterator();
          return new UnboundPredicate<>(pred.op(), pred.term(), (Iterable<T>) iter);
        default:
          throw new UnsupportedOperationException(
              "Cannot sanitize unsupported predicate type: " + pred.op());
      }
    }
  }

  private static class StringSanitizer extends ExpressionVisitors.ExpressionVisitor<String> {
    private static final StringSanitizer INSTANCE = new StringSanitizer();

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

    @Override
    public <T> String predicate(BoundPredicate<T> pred) {
      throw new UnsupportedOperationException("Cannot sanitize bound predicate: " + pred);
    }

    public String termToString(UnboundTerm<?> term) {
      if (term instanceof UnboundTransform) {
        return ((UnboundTransform<?, ?>) term).transform() + "(" + termToString(term.ref()) + ")";
      } else if (term instanceof NamedReference) {
        return ((NamedReference<?>) term).name();
      } else {
        throw new UnsupportedOperationException("Unsupported term: " + term);
      }
    }

    @Override
    public <T> String predicate(UnboundPredicate<T> pred) {
      String term = termToString(pred.term());
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
          return term + " < " + sanitize(pred.literal());
        case LT_EQ:
          return term + " <= " + sanitize(pred.literal());
        case GT:
          return term + " > " + sanitize(pred.literal());
        case GT_EQ:
          return term + " >= " + sanitize(pred.literal());
        case EQ:
          return term + " = " + sanitize(pred.literal());
        case NOT_EQ:
          return term + " != " + sanitize(pred.literal());
        case IN:
          return term
              + " IN "
              + abbreviateValues(
                      pred.literals().stream()
                          .map(ExpressionUtil::sanitize)
                          .collect(Collectors.toList()))
                  .stream()
                  .collect(Collectors.joining(", ", "(", ")"));
        case NOT_IN:
          return term
              + " NOT IN "
              + abbreviateValues(
                      pred.literals().stream()
                          .map(ExpressionUtil::sanitize)
                          .collect(Collectors.toList()))
                  .stream()
                  .collect(Collectors.joining(", ", "(", ")"));
        case STARTS_WITH:
          return term + " STARTS WITH " + sanitize(pred.literal());
        case NOT_STARTS_WITH:
          return term + " NOT STARTS WITH " + sanitize(pred.literal());
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
                "... (%d values hidden, %d in total) ...",
                sanitizedValues.size() - distinctValues.size(), sanitizedValues.size()));
        return abbreviatedList;
      }
    }
    return sanitizedValues;
  }

  private static String sanitize(Literal<?> literal) {
    if (literal instanceof Literals.StringLiteral) {
      CharSequence value = ((Literals.StringLiteral) literal).value();
      if (DATE.matcher(value).matches()) {
        return "(date)";
      } else if (TIME.matcher(value).matches()) {
        return "(time)";
      } else if (TIMESTAMP.matcher(value).matches()) {
        return "(timestamp)";
      } else {
        return sanitizeString(value);
      }
    } else if (literal instanceof Literals.DateLiteral) {
      return "(date)";
    } else if (literal instanceof Literals.TimeLiteral) {
      return "(time)";
    } else if (literal instanceof Literals.TimestampLiteral) {
      return "(timestamp)";
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
      return sanitizeString(literal.value().toString());
    }
  }

  private static String sanitizeNumber(Number value, String type) {
    // log10 of zero isn't defined and will result in negative infinity
    int numDigits =
        0.0d == value.doubleValue() ? 1 : (int) Math.log10(Math.abs(value.doubleValue())) + 1;
    return "(" + numDigits + "-digit-" + type + ")";
  }

  private static String sanitizeString(CharSequence value) {
    // hash the value and return the hash as hex
    return String.format("(hash-%08x)", HASH_FUNC.apply(value));
  }
}
