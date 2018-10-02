/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.spark;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.expressions.Binder;
import com.netflix.iceberg.expressions.BoundReference;
import com.netflix.iceberg.expressions.Expression.Operation;
import com.netflix.iceberg.expressions.ExpressionVisitors;
import com.netflix.iceberg.types.Types.TimestampType;
import com.netflix.iceberg.util.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.And$;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.BinaryExpression;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.catalyst.expressions.InSet;
import org.apache.spark.sql.catalyst.expressions.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.IsNull;
import org.apache.spark.sql.catalyst.expressions.LessThan;
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Not;
import org.apache.spark.sql.catalyst.expressions.Not$;
import org.apache.spark.sql.catalyst.expressions.Or;
import org.apache.spark.sql.catalyst.expressions.Or$;
import org.apache.spark.sql.catalyst.expressions.ParseToDate;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;
import org.apache.spark.sql.catalyst.expressions.Year;
import org.apache.spark.sql.functions$;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.netflix.iceberg.expressions.ExpressionVisitors.visit;
import static com.netflix.iceberg.expressions.Expressions.alwaysFalse;
import static com.netflix.iceberg.expressions.Expressions.and;
import static com.netflix.iceberg.expressions.Expressions.equal;
import static com.netflix.iceberg.expressions.Expressions.not;
import static com.netflix.iceberg.expressions.Expressions.or;
import static com.netflix.iceberg.expressions.Expressions.predicate;
import static scala.collection.JavaConverters.seqAsJavaListConverter;
import static scala.collection.JavaConverters.setAsJavaSetConverter;


public class SparkExpressions {
  private SparkExpressions() {
  }

  private static final Map<Class<? extends Expression>, Operation> FILTERS = ImmutableMap
      .<Class<? extends Expression>, Operation>builder()
      .put(EqualTo.class, Operation.EQ)
      .put(EqualNullSafe.class, Operation.EQ)
      .put(GreaterThan.class, Operation.GT)
      .put(GreaterThanOrEqual.class, Operation.GT_EQ)
      .put(LessThan.class, Operation.LT)
      .put(LessThanOrEqual.class, Operation.LT_EQ)
      .put(In.class, Operation.IN)
      .put(InSet.class, Operation.IN)
      .put(IsNull.class, Operation.IS_NULL)
      .put(IsNotNull.class, Operation.NOT_NULL)
      .put(And.class, Operation.AND)
      .put(Or.class, Operation.OR)
      .put(Not.class, Operation.NOT)
      .build();

  public static com.netflix.iceberg.expressions.Expression convert(Expression expr) {
    Class<? extends Expression> exprClass = expr.getClass();
    Operation op = FILTERS.get(exprClass);
    if (op != null) {
      switch (op) {
        case IS_NULL:
        case NOT_NULL:
          UnaryExpression unary = (UnaryExpression) expr;
          if (unary.child() instanceof Attribute) {
            Attribute attr = (Attribute) unary.child();
            return predicate(op, attr.name());
          }
          return null;
        case LT:
        case LT_EQ:
        case GT:
        case GT_EQ:
        case EQ:
        case NOT_EQ:
          BinaryExpression binary = (BinaryExpression) expr;
          return convert(op, binary.left(), binary.right());
        case NOT:
          com.netflix.iceberg.expressions.Expression child = convert(((Not) expr).child());
          if (child != null) {
            return not(child);
          }
          return null;
        case AND:
          And andExpr = (And) expr;
          com.netflix.iceberg.expressions.Expression andLeft = convert(andExpr.left());
          com.netflix.iceberg.expressions.Expression andRight = convert(andExpr.right());
          if (andLeft != null && andRight != null) {
            return and(convert(andExpr.left()), convert(andExpr.right()));
          }
          return null;
        case OR:
          Or orExpr = (Or) expr;
          com.netflix.iceberg.expressions.Expression orLeft = convert(orExpr.left());
          com.netflix.iceberg.expressions.Expression orRight = convert(orExpr.right());
          if (orLeft != null && orRight != null) {
            return or(orLeft, orRight);
          }
          return null;
        case IN:
          if (expr instanceof In) {
            In inExpr = (In) expr;
            List<Object> literals = convertLiterals(seqAsJavaListConverter(inExpr.list()).asJava());
            if (literals != null) {
              return convertIn(inExpr.value(), literals);
            } else {
              // if the list contained a non-literal, it can't be converted
              return null;
            }
          } else if (expr instanceof InSet) {
            InSet inExpr = (InSet) expr;
            // expressions are already converted to Java objects
            Set<Object> literals = setAsJavaSetConverter(inExpr.hset()).asJava();
            return convertIn(inExpr.child(), literals);
          }
        default:
      }
    }

    return null; // can't convert
  }

  private enum Transform {
    IDENTITY,
    YEAR, // literal is an integer year, like 2018
    DAY,  // literal is an integer date
  }

  private static final Map<Class<? extends Expression>, Transform> TRANSFORMS = ImmutableMap
      .<Class<? extends Expression>, Transform>builder()
      .put(UnresolvedAttribute.class, Transform.IDENTITY)
      .put(AttributeReference.class, Transform.IDENTITY)
      .put(Year.class, Transform.YEAR)
      .put(ParseToDate.class, Transform.DAY)
      .put(Cast.class, Transform.DAY)
      .build();

  private static com.netflix.iceberg.expressions.Expression convertIn(Expression expr,
                                                                      Collection<Object> values) {
    if (expr instanceof Attribute) {
      Attribute attr = (Attribute) expr;
      com.netflix.iceberg.expressions.Expression converted = alwaysFalse();
      for (Object item : values) {
        converted = or(converted, equal(attr.name(), item));
      }
      return converted;
    }

    return null;
  }

  private static List<Object> convertLiterals(List<Expression> values) {
    List<Object> converted = Lists.newArrayListWithExpectedSize(values.size());

    for (Expression value : values) {
      if (value instanceof Literal) {
        Literal lit = (Literal) value;
        converted.add(valueFromSpark(lit));
      } else {
        return null;
      }
    }

    return converted;
  }

  private static com.netflix.iceberg.expressions.Expression convert(Operation op,
                                                                    Expression left,
                                                                    Expression right) {
    Pair<Transform, String> attrPair = null;
    Operation leftOperation = null;
    Literal lit = null;

    if (right instanceof Literal) {
      lit = (Literal) right;
      attrPair = convertAttr(left);
      leftOperation = op;
    } else if (left instanceof Literal) {
      lit = (Literal) left;
      attrPair = convertAttr(right);
      leftOperation = op.flipLR();
    }

    if (attrPair != null) {
      switch (attrPair.first()) {
        case IDENTITY:
          return predicate(leftOperation, attrPair.second(), valueFromSpark(lit));
        case YEAR:
          return filter(leftOperation, attrPair.second(), (int) lit.value(),
              SparkExpressions::yearToTimestampMicros);
        case DAY:
          return filter(leftOperation, attrPair.second(), (int) lit.value(),
              SparkExpressions::dayToTimestampMicros);
        default:
      }
    }

    return null;
  }

  private static Object valueFromSpark(Literal lit) {
    if (lit.value() instanceof UTF8String) {
      return lit.value().toString();
    } else if (lit.value() instanceof Decimal) {
      return ((Decimal) lit.value()).toJavaBigDecimal();
    }
    return lit.value();
  }

  private static Pair<Transform, String> convertAttr(Expression expr) {
    Transform type = TRANSFORMS.get(expr.getClass());
    if (type == Transform.IDENTITY) {
      Attribute attr = (Attribute) expr;
      return Pair.of(type, attr.name());

    } else if (expr instanceof Cast) {
      Cast cast = (Cast) expr;
      if (DateType$.MODULE$.sameType(cast.dataType()) && cast.child() instanceof Attribute) {
        Attribute attr = (Attribute) cast.child();
        return Pair.of(Transform.DAY, attr.name());
      }

    } else if (expr instanceof ParseToDate) {
      ParseToDate toDate = (ParseToDate) expr;
      if (toDate.left() instanceof Attribute) {
        Attribute attr = (Attribute) toDate.left();
        return Pair.of(Transform.DAY, attr.name());
      }

    } else if (expr instanceof UnaryExpression) {
      UnaryExpression func = (UnaryExpression) expr;
      if (func.child() instanceof Attribute) {
        Attribute attr = (Attribute) func.child();
        return Pair.of(type, attr.name());
      }
    }

    return null;
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static long yearToTimestampMicros(int year) {
    return ChronoUnit.MICROS.between(EPOCH,
        LocalDateTime.of(year, 1, 1, 0, 0).atOffset(ZoneOffset.UTC));
  }

  private static long dayToTimestampMicros(int daysFromEpoch) {
    return ChronoUnit.MICROS.between(EPOCH,
        EPOCH_DAY.plusDays(daysFromEpoch).atStartOfDay().atOffset(ZoneOffset.UTC));
  }

  private static com.netflix.iceberg.expressions.Literal<Long> tsLiteral(long timestampMicros) {
    return com.netflix.iceberg.expressions.Literal
        .of(timestampMicros)
        .to(TimestampType.withoutZone());
  }

  private static com.netflix.iceberg.expressions.Expression filter(
      Operation op, String name, int value, Function<Integer, Long> startTsMicros) {
    switch (op) {
      case LT:
        return predicate(Operation.LT, name, tsLiteral(startTsMicros.apply(value)));
      case LT_EQ:
        return predicate(Operation.LT, name, tsLiteral(startTsMicros.apply(value + 1)));
      case GT:
        return predicate(Operation.GT_EQ, name, tsLiteral(startTsMicros.apply(value + 1)));
      case GT_EQ:
        return predicate(Operation.GT_EQ, name, tsLiteral(startTsMicros.apply(value)));
      case EQ:
        return and(
            predicate(Operation.GT_EQ, name, tsLiteral(startTsMicros.apply(value))),
            predicate(Operation.LT, name, tsLiteral(startTsMicros.apply(value + 1)))
        );
      case NOT_EQ:
        return or(
            predicate(Operation.GT_EQ, name, tsLiteral(startTsMicros.apply(value + 1))),
            predicate(Operation.LT, name, tsLiteral(startTsMicros.apply(value)))
        );
      case IN:
      case NOT_IN:
      default:
        throw new IllegalArgumentException("Cannot convert operation to year filter: " + op);
    }
  }

  public static Expression convert(com.netflix.iceberg.expressions.Expression filter,
                                   Schema schema) {
    return visit(Binder.bind(schema.asStruct(), filter), new ExpressionToSpark(schema));
  }

  private static class ExpressionToSpark extends ExpressionVisitors.
      BoundExpressionVisitor<Expression> {
    private final Schema schema;

    public ExpressionToSpark(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Expression alwaysTrue() {
      return functions$.MODULE$.lit(true).expr();
    }

    @Override
    public Expression alwaysFalse() {
      return functions$.MODULE$.lit(false).expr();
    }

    @Override
    public Expression not(Expression child) {
      return Not$.MODULE$.apply(child);
    }

    @Override
    public Expression and(Expression left, Expression right) {
      return And$.MODULE$.apply(left, right);
    }

    @Override
    public Expression or(Expression left, Expression right) {
      return Or$.MODULE$.apply(left, right);
    }

    @Override
    public <T> Expression isNull(BoundReference<T> ref) {
      return column(ref).isNull().expr();
    }

    @Override
    public <T> Expression notNull(BoundReference<T> ref) {
      return column(ref).isNotNull().expr();
    }

    @Override
    public <T> Expression lt(BoundReference<T> ref,
                             com.netflix.iceberg.expressions.Literal<T> lit) {
      return column(ref).lt(lit.value()).expr();
    }

    @Override
    public <T> Expression ltEq(BoundReference<T> ref,
                               com.netflix.iceberg.expressions.Literal<T> lit) {
      return column(ref).leq(lit.value()).expr();
    }

    @Override
    public <T> Expression gt(BoundReference<T> ref,
                             com.netflix.iceberg.expressions.Literal<T> lit) {
      return column(ref).gt(lit.value()).expr();
    }

    @Override
    public <T> Expression gtEq(BoundReference<T> ref,
                               com.netflix.iceberg.expressions.Literal<T> lit) {
      return column(ref).geq(lit.value()).expr();
    }

    @Override
    public <T> Expression eq(BoundReference<T> ref,
                             com.netflix.iceberg.expressions.Literal<T> lit) {
      return column(ref).equalTo(lit.value()).expr();
    }

    @Override
    public <T> Expression notEq(BoundReference<T> ref,
                                com.netflix.iceberg.expressions.Literal<T> lit) {
      return column(ref).notEqual(lit.value()).expr();
    }

    @Override
    public <T> Expression in(BoundReference<T> ref,
                             com.netflix.iceberg.expressions.Literal<T> lit) {
      throw new UnsupportedOperationException("Not implemented: in");
    }

    @Override
    public <T> Expression notIn(BoundReference<T> ref,
                                com.netflix.iceberg.expressions.Literal<T> lit) {
      throw new UnsupportedOperationException("Not implemented: notIn");
    }

    private Column column(BoundReference ref) {
      return functions$.MODULE$.column(schema.findColumnName(ref.fieldId()));
    }
  }
}
