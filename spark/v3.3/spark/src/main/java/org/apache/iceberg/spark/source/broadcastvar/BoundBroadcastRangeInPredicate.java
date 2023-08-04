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
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.BoundTerm;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnBoundCreator;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.source.Tuple;
import org.apache.iceberg.spark.source.broadcastvar.broadcastutils.LiteralListWrapper;
import org.apache.iceberg.spark.source.broadcastvar.broadcastutils.NavigableSetWrapper;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;

public class BoundBroadcastRangeInPredicate<T> extends BoundSetPredicate<T>
    implements UnBoundCreator<T>, BroadcastVarPredicate {
  private final BroadcastedJoinKeysWrapper bcVar;
  private transient volatile WeakReference<NavigableSet<T>> transientLitSet;

  static final LoadingCache<BroadcastedJoinKeysWrapper, List<Literal[]>>
      idempotentializer2DLiterals =
          Caffeine.newBuilder()
              .expireAfterWrite(Duration.ofSeconds(BroadcastedJoinKeysWrapper.CACHE_EXPIRY))
              .maximumSize(BroadcastedJoinKeysWrapper.CACHE_SIZE)
              .weakValues()
              .build(
                  bcj -> {
                    // lets check the initialization here.
                    // TODO: figure out a better way to initialize
                    BroadcastVarReaper.checkInitialized();
                    List<Literal[]> temp = BroadcastUtil.evaluateLiteralFor2D(bcj);
                    return temp;
                  });

  private static final LoadingCache<Tuple<BroadcastedJoinKeysWrapper, Integer>, NavigableSet>
      idempotentializer =
          Caffeine.newBuilder()
              .expireAfterWrite(Duration.ofSeconds(BroadcastedJoinKeysWrapper.CACHE_EXPIRY))
              .maximumSize(BroadcastedJoinKeysWrapper.CACHE_SIZE)
              .weakValues()
              .build(
                  tuple -> {
                    // lets check the initialization here.
                    // TODO: figure out a better way to initialize
                    BroadcastVarReaper.checkInitialized();
                    BroadcastedJoinKeysWrapper bcj = tuple.getElement1();
                    int relativeIndx = tuple.getElement2();
                    if (bcj.getTupleLength() == 1) {
                      Stream<Literal<Object>> temp = BroadcastUtil.evaluateLiteral(bcj);
                      Iterator<Literal<Object>> iter = temp.iterator();
                      return createNavigableSet(iter);
                    } else {
                      List<Literal[]> temp = idempotentializer2DLiterals.get(bcj);
                      Iterator<Literal[]> iter = temp.iterator();
                      return new NavigableSetWrapper(relativeIndx, iter);
                    }
                  });

  static <T> NavigableSet<T> createNavigableSet(Iterator<Literal<T>> transientLiterals) {
    Iterator<Literal<T>> iter = transientLiterals;
    Literal<T> firstNotNull = null;
    while (firstNotNull == null && iter.hasNext()) {
      firstNotNull = iter.next();
    }
    if (firstNotNull != null) {
      NavigableSet<T> tempSet = Sets.newTreeSet(firstNotNull.comparator());
      tempSet.add(firstNotNull.value());
      iter.forEachRemaining(
          x -> {
            if (x != null) {
              tempSet.add(x.value());
            }
          });
      return tempSet;
    } else {
      return Collections.emptyNavigableSet();
    }
  }

  public BoundBroadcastRangeInPredicate(
      Operation op,
      BoundTerm<T> term,
      BroadcastedJoinKeysWrapper bcVar,
      final List<Literal<T>> transientLiterals) {
    super(op, term, Collections.emptySet());
    Preconditions.checkArgument(
        op == Operation.RANGE_IN, "%s predicate does not support a literal set", op);
    this.bcVar = bcVar;
    if (transientLiterals != null) {
      NavigableSet<T> actualLitset =
          idempotentializer.get(
              new Tuple<>(bcVar, bcVar.getRelativeKeyIndex()),
              tuple -> {
                // lets check the initialization here.
                // TODO: figure out a better way to initialize
                BroadcastVarReaper.checkInitialized();
                BroadcastedJoinKeysWrapper bcj = tuple.getElement1();
                int relativeKeyIndex = tuple.getElement2();
                if (bcj.getTupleLength() == 1) {
                  return createNavigableSet(transientLiterals.iterator());
                } else {
                  LiteralListWrapper<Literal<T>> castedList =
                      (LiteralListWrapper<Literal<T>>) transientLiterals;
                  return new NavigableSetWrapper(
                      relativeKeyIndex, castedList.getUnderlyingList().iterator());
                }
              });
      this.transientLitSet = new WeakReference<>(actualLitset);
    } else {
      this.transientLitSet = null;
    }
  }

  public BoundBroadcastRangeInPredicate(
      Operation op, BoundTerm<T> term, BroadcastedJoinKeysWrapper bcVar) {
    super(op, term, Collections.emptySet());
    Preconditions.checkArgument(
        op == Operation.RANGE_IN, "%s predicate does not support a literal set", op);
    this.bcVar = bcVar;
    this.transientLitSet = null;
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

    NavigableSet<T> actualLitset = this.transientLitSet != null ? this.transientLitSet.get() : null;
    if (actualLitset == null) {
      actualLitset =
          (NavigableSet<T>)
              idempotentializer.get(new Tuple<>(this.bcVar, this.bcVar.getRelativeKeyIndex()));
      this.transientLitSet = new WeakReference<>(actualLitset);
    }
    return actualLitset;
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
    return new BroadcastHRUnboundPredWithTransform<T, S>(name, this.bcVar, transform);
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
    Set<Tuple<BroadcastedJoinKeysWrapper, Integer>> keySetBefore =
        idempotentializer.asMap().keySet();
    int totalSizeBefore = keySetBefore.size();
    keySetBefore.stream()
        .filter(tuple -> tuple.getElement1().getBroadcastVarId() == id)
        .forEach(idempotentializer::invalidate);
    Set<Tuple<BroadcastedJoinKeysWrapper, Integer>> keySetAfter =
        idempotentializer.asMap().keySet();
    int totalSizeAfter = keySetAfter.size();
    if (totalSizeBefore > totalSizeAfter) {
      // removed element
      // System.out.println("removed element");
    }
    idempotentializer2DLiterals.asMap().keySet().stream()
        .filter(bcj -> bcj.getBroadcastVarId() == id)
        .forEach(idempotentializer2DLiterals::invalidate);
  }

  static void invalidateBroadcastCache() {
    idempotentializer.invalidateAll();
    idempotentializer2DLiterals.invalidateAll();
  }

  @Override
  public BroadcastedJoinKeysWrapper getBroadcastVar() {
    return this.bcVar;
  }
}
