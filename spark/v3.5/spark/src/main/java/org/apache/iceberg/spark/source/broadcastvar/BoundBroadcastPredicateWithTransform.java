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
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.BoundTerm;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnBoundCreator;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.source.Tuple;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;

public class BoundBroadcastPredicateWithTransform<S, T> extends BoundSetPredicate<T>
    implements UnBoundCreator, BroadcastVarPredicate {
  private final BroadcastedJoinKeysWrapper bcVar;
  private transient volatile WeakReference<NavigableSet<T>> transientLitSet;

  private final Function<S, T> transform;

  private final boolean fixDate;

  private static final LoadingCache<
          Tuple<BroadcastedJoinKeysWrapper, Function>, NavigableSet>
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
                    Function tf = tuple.getElement2();
                    boolean fixDate = BroadcastHRUnboundPredWithTransform.fixDateFlag.get();
                    int relativeKeyIndex = bcj.getRelativeKeyIndex();
                    Stream<Literal<Object>> temp;
                    if (bcj.getTupleLength() == 1) {
                      temp = BroadcastUtil.evaluateLiteralWithTransform(bcj, tf, fixDate);
                    } else {
                      temp =
                          BroadcastUtil.evaluateLiteralWithTransformFrom2D(
                              bcj, tf, relativeKeyIndex, fixDate);
                    }
                    Iterator<Literal<Object>> iter = temp.iterator();
                    return BoundBroadcastRangeInPredicate.createNavigableSet(iter);
                  });

  public BoundBroadcastPredicateWithTransform(
      Operation op,
      BoundTerm<T> term,
      BroadcastedJoinKeysWrapper bcVar,
      Function<S, T> transform,
      List<Literal<T>> transientLitSet,
      boolean fixDateX) {
    super(op, term, Collections.emptySet());
    Preconditions.checkArgument(
        op == Operation.RANGE_IN, "%s predicate does not support a literal set", op);
    this.bcVar = bcVar;
    if (transientLitSet != null) {
      this.transientLitSet =
          new WeakReference<>(
              BoundBroadcastRangeInPredicate.createNavigableSet(transientLitSet.iterator()));
      // BroadcastUtil.convertLiteralToValue(transientLitSet).collect(Collectors.toSet());
    }
    this.transform = transform;
    this.fixDate = fixDateX;
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
      BroadcastHRUnboundPredWithTransform.fixDateFlag.set(this.fixDate);
      try {
        actualLitset =
            idempotentializer.get(
                new Tuple<>(this.bcVar, this.transform));
        this.transientLitSet = new WeakReference<>(actualLitset);
        return actualLitset;
      } finally {
        BroadcastHRUnboundPredWithTransform.fixDateFlag.set(false);
      }
    }
    return actualLitset;
  }

  @Override
  public boolean isEquivalentTo(Expression other) {
    if (op() == other.op()) {
      BoundBroadcastPredicateWithTransform<?, ?> pred =
          (BoundBroadcastPredicateWithTransform<?, ?>) other;
      return literalSet().equals(pred.literalSet());
    }
    return false;
  }

  @Override
  public UnboundPredicate createTransformAppliedUnboundPred(Function transformm, String colName) {
    throw new IllegalStateException("not expected to be called");
  }

  @Override
  public UnboundPredicate createTransformAppliedUnboundPred(
      Function transformm, String colName, boolean fixDateX) {
    throw new IllegalStateException("not expected to be called");
  }

  @Override
  public UnboundPredicate<T> createUnboundPred(String name) {
    return new BroadcastHRUnboundPredWithTransform(name, this.bcVar, this.transform);
  }

  @Override
  public UnboundPredicate<S> createTransformRemovedUnboundPred() {
    return new BroadcastHRUnboundPredicate<>(
        ((BoundReference) this.term()).field().name(), this.bcVar);
  }

  @Override
  public String toString() {
    return term() + " range in (" + this.bcVar.toString() + "," + this.transform.toString() + ")";
  }

  static void removeBroadcast(long id) {
    idempotentializer.asMap().keySet().stream()
        .filter(tup -> tup.getElement1().getBroadcastVarId() == id)
        .forEach(idempotentializer::invalidate);
  }

  static void invalidateBroadcastCache() {
    idempotentializer.invalidateAll();
  }

  @Override
  public BroadcastedJoinKeysWrapper getBroadcastVar() {
    return this.bcVar;
  }
}
