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
package org.apache.iceberg.spark.source.broadcastvar;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Literals;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.transforms.Transform;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;

public final class BroadcastUtil {

  private BroadcastUtil() {
    throw new UnsupportedOperationException("Utility class instance cannot be constructed");
  }

  public static <T> Stream<Literal<T>> evaluateLiteral(BroadcastedJoinKeysWrapper bcVar) {
    // TODO: Verify if any of the element of broadcast varaibl keys can be null.
    // logicaly if it is null, it should not be present as key at all
    return Stream.of(bcVar.getKeysArray().getBaseArray())
        // .filter(Objects::nonNull)
        .map(
            ele ->
                (Literal<T>)
                    (ele != null ? Literals.from(SparkFilters.convertLiteral(ele)) : null));
  }

  public static List<Literal[]> evaluateLiteralFor2D(BroadcastedJoinKeysWrapper bcVar) {
    return Stream.of(bcVar.getKeysArray().getBaseArray())
        .map(
            eleArrArg -> {
              Object[] eleArr = (Object[]) eleArrArg;
              return Arrays.stream(eleArr)
                  .map(ele -> ele != null ? Literals.from(SparkFilters.convertLiteral(ele)) : null)
                  .toArray(len -> new Literal[len]);
            })
        .collect(Collectors.<Literal[]>toList());
  }

  public static <S, T> Stream<Literal<T>> evaluateLiteralWithTransform(
      BroadcastedJoinKeysWrapper bcVar, Function<S, T> transform, boolean fixDate) {
    if (fixDate) {
      List<Object> temp =
          Stream.of(bcVar.getKeysArray().getBaseArray()) // .filter(Objects::nonNull).
              .map(x -> x != null ? transform.apply((S) SparkFilters.convertLiteral(x)) : null)
              .collect(Collectors.toList());
      return Transform.dateFixer.apply(temp).stream()
          .map(x -> x != null ? Literals.from((T) x) : null);
    } else {
      return Stream.of(bcVar.getKeysArray().getBaseArray())
          // .filter(Objects::nonNull).
          .map(
              x ->
                  x != null
                      ? Literals.from(transform.apply((S) SparkFilters.convertLiteral(x)))
                      : null);
    }
  }

  // TODO optimize this if possible as this is going to create a new array
  public static <S, T> Stream<Literal<T>> evaluateLiteralWithTransformFrom2D(
      BroadcastedJoinKeysWrapper bcVar,
      Function<S, T> transform,
      int relativeKeyIndex,
      boolean fixDate) {
    if (fixDate) {
      List<Object> temp =
          Stream.of(bcVar.getKeysArray().getBaseArray())
              .map(
                  eleArrArg -> {
                    Object[] eleArr = (Object[]) eleArrArg;
                    return eleArr[relativeKeyIndex] != null
                        ? transform.apply((S) SparkFilters.convertLiteral(eleArr[relativeKeyIndex]))
                        : null;
                  })
              .collect(Collectors.toList());
      return Transform.dateFixer.apply(temp).stream()
          .map(x -> x != null ? Literals.from((T) x) : null);
    } else {
      return Stream.of(bcVar.getKeysArray().getBaseArray())
          .map(
              eleArrArg -> {
                Object[] eleArr = (Object[]) eleArrArg;
                return eleArr[relativeKeyIndex] != null
                    ? Literals.from(
                        transform.apply((S) SparkFilters.convertLiteral(eleArr[relativeKeyIndex])))
                    : null;
              });
    }
  }
}
