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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.RangeInPredUtil;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.iceberg.spark.source.broadcastvar.BoundBroadcastRangeInPredicate;
import org.apache.iceberg.spark.source.broadcastvar.BroadcastHRUnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestRangeInPredicate extends SparkTestBaseWithCatalog {

  public TestRangeInPredicate() {}

  private static <T, R> R testRangeInEvaluatorBoundPred(
      Function<BoundBroadcastRangeInPredicate<T>, R> evaluator,
      BoundBroadcastRangeInPredicate<T> pred) {
    return evaluator.apply(pred);
  }

  private static <T, R> R testRangeInEvaluatorUnBound(
      Function<UnboundPredicate<T>, R> evaluator, UnboundPredicate<T> pred) {
    return evaluator.apply(pred);
  }

  private static void projectEvaluator(
      Projections.ProjectionEvaluator pe, UnboundPredicate<Long> rangeInPred, Schema schema) {
    Expression retVal1 = testRangeInEvaluatorUnBound(pred -> pe.project(pred), rangeInPred);
    Assert.assertNotNull(retVal1);
    Assert.assertTrue(retVal1 instanceof UnboundPredicate);
    BoundBroadcastRangeInPredicate<Long> bound =
        (BoundBroadcastRangeInPredicate<Long>) rangeInPred.bind(schema.asStruct(), true);
    Expression retVal2 = testRangeInEvaluatorBoundPred(pred -> pe.project(pred), bound);
    Assert.assertNotNull(retVal2);
    Assert.assertTrue(retVal2 instanceof UnboundPredicate);
  }

  private static Tuple<Set<?>, DataType> getInSet(Type.TypeID typeId) {
    switch (typeId) {
      case DATE:
        return new Tuple<>(
            Sets.newHashSet(1, 1, 7, 8, 3, 11, 4, 34, 4847, 35, 42, 73), DataTypes.DateType);
      case INTEGER:
        return new Tuple<>(
            Sets.newHashSet(1, 1, 7, 8, 3, 11, 4, 34, 4847, 35, 42, 73), DataTypes.IntegerType);
      case TIME:
      case TIMESTAMP:
        return new Tuple<>(
            Sets.newHashSet(1L, 1L, 7L, 8L, 3L, 11L, 4L, 8373L, 43L, 2210L, 63L),
            DataTypes.TimestampType);
      case LONG:
        return new Tuple<>(
            Sets.newHashSet(1L, 1L, 7L, 8L, 3L, 11L, 4L, 8373L, 43L, 2210L, 63L),
            DataTypes.LongType);
      case FLOAT:
        return new Tuple<>(
            Sets.newHashSet(
                1.1f,
                3.5f,
                7.0f,
                8.44f,
                3.1424f,
                11.585f,
                4.0f,
                33766.372653f,
                653.532f,
                24348.6253905f),
            DataTypes.FloatType);
      case DOUBLE:
        return new Tuple<>(
            Sets.newHashSet(
                1.1d,
                3.5d,
                7.0736455456d,
                8.44d,
                3.1424d,
                11.585d,
                4.0d,
                64554.62523554d,
                6723.63254534d,
                142.2736535739d),
            DataTypes.DoubleType);
      case DECIMAL:
        return new Tuple<>(
            Sets.newHashSet(
                BigDecimal.valueOf(1L),
                BigDecimal.valueOf(1L),
                BigDecimal.valueOf(7.725353d),
                BigDecimal.valueOf(87263534.73653453d),
                BigDecimal.valueOf(3L),
                BigDecimal.valueOf(1167363465.63463d),
                BigDecimal.valueOf(4L),
                BigDecimal.valueOf(635654.63553d),
                BigDecimal.valueOf(6525222.676354533d),
                BigDecimal.valueOf(1424.783736353d)),
            new DecimalType());
      case STRING:
        return new Tuple<>(
            Sets.newHashSet(
                "abc",
                "defkf",
                "bdghuw3",
                "hjdwh7265g3bkd",
                "jdj2w99823j",
                "747640mdhhe",
                "uudj37762",
                "jr7874hhy6dh2",
                "83hdbv2779q2",
                "87hdsy267"),
            new DecimalType());

      default:
        throw new UnsupportedOperationException("for " + typeId);
    }
  }

  @SuppressWarnings("CyclomaticComplexity")
  private static Object getElementNotContained(
      Type.TypeID typeId, SortedSet check, Element typeOfEle) {
    switch (typeId) {
      case DATE:
      case INTEGER:
        switch (typeOfEle) {
          case NOT_CONTAINED_HIGHER_THAN_LAST:
            return (Integer) check.last() + 6736;
          case NOT_CONTAINED_LOWER_THAN_FIRST:
            return (Integer) check.first() - 6736;
          case NOT_CONTAINED_BETWEEN_FIRST_AND_LAST:
            for (int i = (Integer) check.first(); i < (Integer) check.last(); ++i) {
              if (!check.contains(i)) {
                return i;
              }
            }
            throw new RuntimeException(
                "Not able to find an element not contained in SortedSet and lying"
                    + "between bounds");
        }
      case TIME:
      case TIMESTAMP:
      case LONG:
        switch (typeOfEle) {
          case NOT_CONTAINED_HIGHER_THAN_LAST:
            return (Long) check.last() + 6736L;
          case NOT_CONTAINED_LOWER_THAN_FIRST:
            return (Long) check.first() - 6736L;
          case NOT_CONTAINED_BETWEEN_FIRST_AND_LAST:
            for (long j = (Long) check.first(); j < (Long) check.last(); ++j) {
              if (!check.contains(j)) {
                return j;
              }
            }
            throw new RuntimeException(
                "Not able to find an element not contained in SortedSet and lying"
                    + "between bounds");
        }
      case FLOAT:
        switch (typeOfEle) {
          case NOT_CONTAINED_HIGHER_THAN_LAST:
            return (Float) check.last() + 6736.8772f;
          case NOT_CONTAINED_LOWER_THAN_FIRST:
            return (Float) check.first() - 6736.74284954f;
          case NOT_CONTAINED_BETWEEN_FIRST_AND_LAST:
            for (float k = (Float) check.first(); k < (Float) check.last(); ++k) {
              if (!check.contains(k)) {
                return k;
              }
            }
            throw new RuntimeException(
                "Not able to find an element not contained in SortedSet and lying"
                    + "between bounds");
        }
      case DOUBLE:
        switch (typeOfEle) {
          case NOT_CONTAINED_HIGHER_THAN_LAST:
            return (Double) check.last() + 6736.8772d;
          case NOT_CONTAINED_LOWER_THAN_FIRST:
            return (Double) check.first() - 6736.74284954d;
          case NOT_CONTAINED_BETWEEN_FIRST_AND_LAST:
            for (double l = (Double) check.first(); l < (Double) check.last(); ++l) {
              if (!check.contains(l)) {
                return l;
              }
            }
            throw new RuntimeException(
                "Not able to find an element not contained in SortedSet and lying"
                    + "between bounds");
        }
      case DECIMAL:
        switch (typeOfEle) {
          case NOT_CONTAINED_HIGHER_THAN_LAST:
            return ((BigDecimal) check.last()).add(BigDecimal.valueOf(7847466.73635355d));
          case NOT_CONTAINED_LOWER_THAN_FIRST:
            return ((BigDecimal) check.first()).subtract(BigDecimal.valueOf(7847466.73636535d));
          case NOT_CONTAINED_BETWEEN_FIRST_AND_LAST:
            for (BigDecimal m = (BigDecimal) check.first();
                m.compareTo((BigDecimal) check.last()) < 0; ) {
              if (!check.contains(m)) {
                return m;
              }
              m = m.add(BigDecimal.valueOf(1.7363d));
            }
            throw new RuntimeException(
                "Not able to find an element not contained in SortedSet and lying"
                    + "between bounds");
        }
      case STRING:
        switch (typeOfEle) {
          case NOT_CONTAINED_HIGHER_THAN_LAST:
            return (String) check.last() + "zzZZZ";
          case NOT_CONTAINED_LOWER_THAN_FIRST:
            return ((String) check.first()).substring(0, ((String) check.first()).length() - 2);
          case NOT_CONTAINED_BETWEEN_FIRST_AND_LAST:
            for (String m = (String) check.first(); m.compareTo((String) check.last()) < 0; ) {
              if (!check.contains(m)) {
                return m;
              }
              m = m + "z";
            }
            throw new RuntimeException(
                "Not able to find an element not contained in SortedSet and lying"
                    + "between bounds");
        }
      default:
        throw new UnsupportedOperationException("for " + typeId);
    }
  }

  private static List<?> get2ElementInList(Type.TypeID typeId) {
    switch (typeId) {
      case INTEGER:
        return Arrays.asList(1, 73);
      case LONG:
        return Arrays.asList(1L, 63L);
      case DATE:
        return Arrays.asList(8, 13437);
      case FLOAT:
        return Arrays.asList(8.44f, 33766.372653f);
      case DOUBLE:
        return Arrays.asList(7.0736455456d, 64554.62523554d);
      case TIME:
      case TIMESTAMP:
        return Arrays.asList(11L, 6356534554L);
      case DECIMAL:
        return Arrays.asList(BigDecimal.valueOf(7.725353d), BigDecimal.valueOf(1424.783736353d));
      default:
        throw new UnsupportedOperationException("for " + typeId);
    }
  }

  private static Stream<? extends Type> rangeSupportingColumnnTypes() {
    return Arrays.stream(Type.TypeID.values())
        .filter(
            typeId ->
                typeId != Type.TypeID.LIST
                    && typeId != Type.TypeID.MAP
                    && typeId != Type.TypeID.STRUCT)
        .map(
            typeID -> {
              String typeIdName = typeID.name().toLowerCase();
              if (typeIdName.equals("integer")) {
                typeIdName = "int";
              }
              if (typeIdName.equals("fixed")) {
                return Types.FixedType.ofLength(8);
              } else if (typeIdName.equals("decimal")) {
                return Types.DecimalType.of(15, 15);
              }
              return Types.fromPrimitiveString(typeIdName);
            })
        .filter(Type::supportsRangePrunable);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS dim");
  }

  @Test
  public void testProjectionEvaluator() {
    Schema schema = new Schema(optional(16, "id", Types.LongType.get()));
    Tuple<Set<?>, DataType> dataTuple = getInSet(Type.TypeID.LONG);
    UnboundPredicate<Long> rangeInPred =
        new BroadcastHRUnboundPredicate(
            "id",
            new DummyBroadcastedJoinKeysWrapper(
                dataTuple.getElement2(), dataTuple.getElement1().toArray(), 1L));

    Projections.ProjectionEvaluator pe1 =
        Projections.inclusive(PartitionSpec.builderFor(schema).identity("id").build());
    projectEvaluator(pe1, rangeInPred, schema);

    Projections.ProjectionEvaluator pe2 =
        Projections.strict(PartitionSpec.builderFor(schema).identity("id").build());
    projectEvaluator(pe2, rangeInPred, schema);
  }

  @Test
  public void testBind() {
    Schema schema = new Schema(optional(16, "id", Types.LongType.get()));
    Tuple<Set<?>, DataType> dataTuple = getInSet(Type.TypeID.LONG);

    UnboundPredicate<Long> unbound =
        new BroadcastHRUnboundPredicate(
            "id",
            new DummyBroadcastedJoinKeysWrapper(
                dataTuple.getElement2(), dataTuple.getElement1().toArray(), 1L));
    BoundPredicate<Long> bound =
        (BoundPredicate<Long>) Binder.bind(schema.asStruct(), unbound, true);
    Assert.assertEquals(bound.op(), Expression.Operation.RANGE_IN);
    BoundBroadcastRangeInPredicate<Long> brip = (BoundBroadcastRangeInPredicate<Long>) bound;
    Assert.assertTrue(brip.isSetPredicate());
    Assert.assertTrue(brip.literalSet() instanceof NavigableSet);
    Assert.assertTrue(dataTuple.getElement1().containsAll(brip.literalSet()));
  }

  @Test
  public void testRangeFilterableDataTypes() {
    rangeSupportingColumnnTypes()
        .forEach(
            type -> {
              Tuple<Set<?>, DataType> dataTuple = getInSet(type.typeId());
              Schema schema = new Schema(optional(16, "id", type));
              UnboundPredicate<?> unboundPredicate =
                  new BroadcastHRUnboundPredicate(
                      "id",
                      new DummyBroadcastedJoinKeysWrapper(
                          dataTuple.getElement2(), dataTuple.getElement1().toArray(), 1L));

              BoundPredicate bound =
                  (BoundPredicate) Binder.bind(schema.asStruct(), unboundPredicate, true);
              Assert.assertEquals(bound.op(), Expression.Operation.RANGE_IN);
              BoundBroadcastRangeInPredicate brip = (BoundBroadcastRangeInPredicate) bound;
              Assert.assertTrue(brip.isSetPredicate());
              Assert.assertTrue(brip.literalSet() instanceof NavigableSet);
              Assert.assertTrue(dataTuple.getElement1().containsAll(brip.literalSet()));
            });
  }

  @SuppressWarnings("checkstyle:methodlength")
  @Test
  public void tesRangePredicateUtil() {
    rangeSupportingColumnnTypes()
        .forEach(
            type -> {
              Schema schema = new Schema(optional(16, "id", type));
              Tuple<Set<?>, DataType> dataTuple = getInSet(type.typeId());
              UnboundPredicate unbound =
                  new BroadcastHRUnboundPredicate(
                      "id",
                      new DummyBroadcastedJoinKeysWrapper(
                          dataTuple.getElement2(), dataTuple.getElement1().toArray(), 1L));
              BoundBroadcastRangeInPredicate bound =
                  (BoundBroadcastRangeInPredicate) Binder.bind(schema.asStruct(), unbound, true);
              NavigableSet sortedSet = (NavigableSet) bound.literalSet();
              Comparator comparator = bound.ref().comparator();
              Assert.assertTrue(
                  RangeInPredUtil.isInRange(
                      () -> sortedSet.first(), () -> sortedSet.last(), sortedSet, true, comparator));

              // min/ max same and contained
              Assert.assertTrue(
                  RangeInPredUtil.isInRange(
                      () -> sortedSet.first(), () -> sortedSet.first(), sortedSet, true, comparator));

              // min or max or both null
              Assert.assertTrue(RangeInPredUtil.isInRange(() -> null, () -> null, sortedSet, true,
                      comparator));
              Assert.assertTrue(
                  RangeInPredUtil.isInRange(() -> sortedSet.first(), () -> null, sortedSet, true,
                          comparator));
              Assert.assertTrue(
                  RangeInPredUtil.isInRange(() -> null, () -> sortedSet.last(), sortedSet, true,
                          comparator));

              Assert.assertFalse(
                  RangeInPredUtil.isInRange(() -> null, () -> null, sortedSet, false, comparator));
              // if either of the bound is contained & other being unbounded, it should be retained
              // irrespective
              Assert.assertTrue(
                  RangeInPredUtil.isInRange(() -> null, () -> sortedSet.last(), sortedSet, false,
                          comparator));
              Assert.assertTrue(
                  RangeInPredUtil.isInRange(() -> sortedSet.first(), () -> null, sortedSet, false,
                          comparator));

              // lower bound null, but upper bound less than the first
              Assert.assertFalse(
                  RangeInPredUtil.isInRange(
                      () -> null,
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_LOWER_THAN_FIRST),
                      sortedSet,
                      true, comparator));
              Assert.assertFalse(
                  RangeInPredUtil.isInRange(
                      () -> null,
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_LOWER_THAN_FIRST),
                      sortedSet,
                      false, comparator));

              // upper bound null, but lower bound > than the last
              Assert.assertFalse(
                  RangeInPredUtil.isInRange(
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_HIGHER_THAN_LAST),
                      () -> null,
                      sortedSet,
                      true, comparator));
              Assert.assertFalse(
                  RangeInPredUtil.isInRange(
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_HIGHER_THAN_LAST),
                      () -> null,
                      sortedSet,
                      true, comparator));

              Assert.assertTrue(
                  RangeInPredUtil.isInRange(
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_LOWER_THAN_FIRST),
                      () ->
                          getElementNotContained(
                              type.typeId(),
                              sortedSet,
                              Element.NOT_CONTAINED_BETWEEN_FIRST_AND_LAST),
                      sortedSet,
                      true, comparator));

              Assert.assertTrue(
                  RangeInPredUtil.isInRange(
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_LOWER_THAN_FIRST),
                      () ->
                          getElementNotContained(
                              type.typeId(),
                              sortedSet,
                              Element.NOT_CONTAINED_BETWEEN_FIRST_AND_LAST),
                      sortedSet,
                      false, comparator));

              Assert.assertTrue(
                  RangeInPredUtil.isInRange(
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_LOWER_THAN_FIRST),
                      () ->
                          getElementNotContained(
                              type.typeId(),
                              sortedSet,
                              Element.NOT_CONTAINED_BETWEEN_FIRST_AND_LAST),
                      sortedSet,
                      true, comparator));

              Assert.assertTrue(
                  RangeInPredUtil.isInRange(
                      () ->
                          getElementNotContained(
                              type.typeId(),
                              sortedSet,
                              Element.NOT_CONTAINED_BETWEEN_FIRST_AND_LAST),
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_HIGHER_THAN_LAST),
                      sortedSet,
                      false, comparator));

              Assert.assertTrue(
                  RangeInPredUtil.isInRange(
                      () ->
                          getElementNotContained(
                              type.typeId(),
                              sortedSet,
                              Element.NOT_CONTAINED_BETWEEN_FIRST_AND_LAST),
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_HIGHER_THAN_LAST),
                      sortedSet,
                      false, comparator));

              Assert.assertTrue(
                  RangeInPredUtil.isInRange(
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_LOWER_THAN_FIRST),
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_HIGHER_THAN_LAST),
                      sortedSet,
                      true, comparator));

              Assert.assertTrue(
                  RangeInPredUtil.isInRange(
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_LOWER_THAN_FIRST),
                      () ->
                          getElementNotContained(
                              type.typeId(), sortedSet, Element.NOT_CONTAINED_HIGHER_THAN_LAST),
                      sortedSet,
                      false, comparator));
            });
  }

  private enum Element {
    NOT_CONTAINED_LOWER_THAN_FIRST,
    NOT_CONTAINED_HIGHER_THAN_LAST,
    NOT_CONTAINED_BETWEEN_FIRST_AND_LAST,
  }
}
