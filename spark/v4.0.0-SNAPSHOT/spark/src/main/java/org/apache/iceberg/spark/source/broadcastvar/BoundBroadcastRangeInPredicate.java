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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.BoundTerm;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.UnBoundCreator;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.source.Tuple;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.catalyst.bcvar.ArrayWrapper;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;

public class BoundBroadcastRangeInPredicate<T> extends BoundSetPredicate<T>
    implements UnBoundCreator<T>, BroadcastVarPredicate {
  private final BroadcastedJoinKeysWrapper bcVar;
  private static final LoadingCache<Tuple<BroadcastedJoinKeysWrapper, Type>, NavigableSet>
      idempotentializer =
          Caffeine.newBuilder()
              .expireAfterWrite(Duration.ofSeconds(BroadcastedJoinKeysWrapper.CACHE_EXPIRY))
              .maximumSize(BroadcastedJoinKeysWrapper.CACHE_SIZE)
           //   .weakValues()
              .build(
                  tuple -> {
                    // lets check the initialization here.
                    // TODO: figure out a better way to initialize
                    BroadcastVarReaper.checkInitialized();
                    BroadcastedJoinKeysWrapper bcj = tuple.getElement1();
                    Type type = tuple.getElement2();
                    ArrayWrapper temp = bcj.getKeysArray();
                    return createNavigableSet(temp, type);
                  });

  static <T> NavigableSet<T> createNavigableSet(ArrayWrapper<T> array, Type type) {
      NavigableSet<T> tempSet = Sets.newTreeSet(Comparators.forType(type.asPrimitiveType()));
      for(int i = 0; i < array.getLength(); ++i) {
        tempSet.add((T)array.get(i));
      }
      return tempSet;
  }

  static <T> NavigableSet<T> createNavigableSet(List<T> list, Type type) {
    NavigableSet<T> tempSet = Sets.newTreeSet(Comparators.forType(type.asPrimitiveType()));
    Iterator<T> iter = list.iterator();
    while(iter.hasNext()) {
      tempSet.add(iter.next());
    }
    return tempSet;
  }


  public BoundBroadcastRangeInPredicate(
      Operation op,
      BoundTerm<T> term,
      BroadcastedJoinKeysWrapper bcVar) {
    super(op, term, Collections.emptySet());
    Preconditions.checkArgument(
        op == Operation.RANGE_IN, "%s predicate does not support a literal set", op);
    this.bcVar = bcVar;

  }

  @Override
  public Expression negate() {
    throw new UnsupportedOperationException("not suppported");
  }

  @Override
  public boolean test(T value) {
    throw new UnsupportedOperationException("Invalid operation for BoundRangeInPredicate: " + op());
  }

  @Override
  public Set<T> literalSet() {
   return (NavigableSet<T>)idempotentializer.get(new Tuple<>(this.bcVar, this.term().type()));
  }

  @Override
  public boolean isEquivalentTo(Expression other) {
    if (op() == other.op()) {
      BoundBroadcastRangeInPredicate<?> pred = (BoundBroadcastRangeInPredicate<?>) other;
      return literalSet().equals(pred.literalSet());
    }
    return false;
  }

  @Override
  public <S> UnboundPredicate<S> createTransformAppliedUnboundPred(
      Function<T, S> transform, String name) {
    return new BroadcastHRUnboundPredWithTransform(name, this.bcVar, transform);
  }

  public <S> UnboundPredicate<S> createTransformAppliedUnboundPred(
      Function<T, S> transform, String name, boolean fixDate) {
    return new BroadcastHRUnboundPredWithTransform(name, this.bcVar, transform, fixDate);
  }

  @Override
  public UnboundPredicate createUnboundPred(String name) {
    return new BroadcastHRUnboundPredicate(name, this.bcVar);
  }

  @Override
  public UnboundPredicate createTransformRemovedUnboundPred() {
    throw new IllegalStateException("not expected to be called");
  }

  @Override
  public String toString() {
    return term() + " range in (" + this.bcVar.toString() + ")";
  }

  static void removeBroadcast(long id) {
    Set<Tuple<BroadcastedJoinKeysWrapper, Type>> keySetBefore =
        idempotentializer.asMap().keySet();
    int totalSizeBefore = keySetBefore.size();
    keySetBefore.stream()
        .filter(tuple -> tuple.getElement1().getBroadcastVarId() == id)
        .forEach(idempotentializer::invalidate);
    Set<Tuple<BroadcastedJoinKeysWrapper, Type>> keySetAfter =
        idempotentializer.asMap().keySet();
    int totalSizeAfter = keySetAfter.size();
    if (totalSizeBefore > totalSizeAfter) {
      // removed element
      // System.out.println("removed element");
    }

  }

  static void invalidateBroadcastCache() {
    idempotentializer.invalidateAll();
  }

  @Override
  public BroadcastedJoinKeysWrapper getBroadcastVar() {
    return this.bcVar;
  }
}
