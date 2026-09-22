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
package org.apache.iceberg.spark;

import java.util.Arrays;
import java.util.Objects;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Map;
import scala.collection.Seq;

/**
 * Compares Spark values by value rather than by reference.
 *
 * <p>Spark represents a binary column as {@code byte[]}, whose {@link Object#equals} is reference
 * equality. Such values must be compared with {@link Arrays#equals}, including when nested inside
 * arrays, structs, and maps.
 */
final class SparkValueEquality {
  private SparkValueEquality() {}

  @FunctionalInterface
  interface ValueEquality {
    boolean test(Object left, Object right);
  }

  /** Equality for types whose own {@link Object#equals} already compares by value. */
  private static final ValueEquality DEFAULT_EQUALITY = Objects::equals;

  static ValueEquality[] forFields(StructType type) {
    int size = type.size();
    ValueEquality[] equalities = new ValueEquality[size];
    for (int index = 0; index < size; index++) {
      equalities[index] = forType(type.fields()[index].dataType());
    }

    return equalities;
  }

  private static ValueEquality forType(DataType type) {
    if (type.equals(DataTypes.BinaryType)) {
      return nullSafe((left, right) -> Arrays.equals((byte[]) left, (byte[]) right));
    } else if (type instanceof ArrayType array) {
      ValueEquality elementEquality = forType(array.elementType());
      if (elementEquality == DEFAULT_EQUALITY) {
        return DEFAULT_EQUALITY;
      }

      return nullSafe((left, right) -> arraysEqual((Seq<?>) left, (Seq<?>) right, elementEquality));
    } else if (type instanceof StructType struct) {
      ValueEquality[] fieldEqualities = forFields(struct);
      if (Arrays.stream(fieldEqualities).allMatch(equality -> equality == DEFAULT_EQUALITY)) {
        return DEFAULT_EQUALITY;
      }

      return nullSafe(
          (left, right) ->
              left.equals(right) || structsEqual((Row) left, (Row) right, fieldEqualities));
    } else if (type instanceof MapType map) {
      ValueEquality keyEquality = forType(map.keyType());
      ValueEquality valueEquality = forType(map.valueType());
      if (keyEquality == DEFAULT_EQUALITY && valueEquality == DEFAULT_EQUALITY) {
        return DEFAULT_EQUALITY;
      }

      // keys can only be looked up by hash if their own equals agrees with the key equality
      boolean hashLookup = keyEquality == DEFAULT_EQUALITY;
      return nullSafe(
          (left, right) ->
              mapsEqual(
                  (Map<?, ?>) left, (Map<?, ?>) right, keyEquality, valueEquality, hashLookup));
    } else {
      return DEFAULT_EQUALITY;
    }
  }

  private static ValueEquality nullSafe(ValueEquality equality) {
    return (left, right) ->
        left == right || (left != null && right != null && equality.test(left, right));
  }

  private static boolean arraysEqual(Seq<?> left, Seq<?> right, ValueEquality elementEquality) {
    if (left.size() != right.size()) {
      return false;
    }

    Iterator<?> leftElements = left.iterator();
    Iterator<?> rightElements = right.iterator();
    while (leftElements.hasNext()) {
      if (!elementEquality.test(leftElements.next(), rightElements.next())) {
        return false;
      }
    }

    return true;
  }

  private static boolean structsEqual(Row left, Row right, ValueEquality[] fieldEqualities) {
    for (int index = 0; index < fieldEqualities.length; index++) {
      if (!fieldEqualities[index].test(left.get(index), right.get(index))) {
        return false;
      }
    }

    return true;
  }

  private static boolean mapsEqual(
      Map<?, ?> left,
      Map<?, ?> right,
      ValueEquality keyEquality,
      ValueEquality valueEquality,
      boolean hashLookup) {
    if (left.size() != right.size()) {
      return false;
    }

    Iterator<? extends Tuple2<?, ?>> entries = left.iterator();
    while (entries.hasNext()) {
      Tuple2<?, ?> entry = entries.next();
      boolean matched =
          hashLookup
              ? hasEntryByHash(right, entry, valueEquality)
              : hasEntryByScan(right, entry, keyEquality, valueEquality);
      if (!matched) {
        return false;
      }
    }

    return true;
  }

  @SuppressWarnings("unchecked")
  private static boolean hasEntryByHash(
      Map<?, ?> map, Tuple2<?, ?> entry, ValueEquality valueEquality) {
    Map<Object, Object> keyedMap = (Map<Object, Object>) map;
    return keyedMap.contains(entry._1())
        && valueEquality.test(entry._2(), keyedMap.apply(entry._1()));
  }

  private static boolean hasEntryByScan(
      Map<?, ?> map, Tuple2<?, ?> entry, ValueEquality keyEquality, ValueEquality valueEquality) {
    Iterator<? extends Tuple2<?, ?>> candidates = map.iterator();
    while (candidates.hasNext()) {
      Tuple2<?, ?> candidate = candidates.next();
      if (keyEquality.test(entry._1(), candidate._1())) {
        return valueEquality.test(entry._2(), candidate._2());
      }
    }

    return false;
  }
}
