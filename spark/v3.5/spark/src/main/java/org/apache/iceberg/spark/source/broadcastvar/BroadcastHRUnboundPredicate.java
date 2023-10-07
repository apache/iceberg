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
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.BoundTerm;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.spark.source.Tuple;
import org.apache.iceberg.spark.source.UncomparableLiteralException;
import org.apache.iceberg.spark.source.broadcastvar.broadcastutils.LiteralListWrapper;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;

public class BroadcastHRUnboundPredicate<T> extends UnboundPredicate<T>
    implements BroadcastVarPredicate {
  private static final LoadingCache<Tuple<BroadcastedJoinKeysWrapper, Integer>, List>
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
                    try {
                      int relativeKeyIndex = tuple.getElement2();
                      if (bcj.getTupleLength() == 1) {
                        List<Literal<Object>> transientLiterals = new LinkedList<>();
                        BroadcastUtil.evaluateLiteral(bcj)
                            .forEach(
                                x -> {
                                  if (x != null) {
                                    transientLiterals.add(x);
                                  }
                                });
                        return transientLiterals;
                      } else {
                        List<Literal[]> underlying2DList =
                            BoundBroadcastRangeInPredicate.idempotentializer2DLiterals.get(bcj);
                        return new LiteralListWrapper(
                            relativeKeyIndex, underlying2DList, bcj.getTupleLength());
                      }
                    } finally {
                      // We are not invalidating the cached data on driver as the cached array may
                      // be
                      // used multiple times in Transform
                      if (!BroadcastVarReaper.isCreatedInDriver()) {
                        bcj.invalidateSelf();
                      }
                    }
                  });
  private final BroadcastedJoinKeysWrapper bcVar;
  private transient WeakReference<List<Literal<T>>> transientLiterals;

  public BroadcastHRUnboundPredicate(
      String termName, BroadcastedJoinKeysWrapper bcVar, Type internalType)
      throws UncomparableLiteralException {
    super(Operation.RANGE_IN, ref(termName));
    this.bcVar = bcVar;
    if (!internalType.supportsRangePrunable()) {
      throw new UncomparableLiteralException(internalType);
    }
  }

  public BroadcastHRUnboundPredicate(String termName, BroadcastedJoinKeysWrapper bcVar) {
    super(Operation.RANGE_IN, ref(termName));
    this.bcVar = bcVar;
  }

  public static <T> NamedReference<T> ref(String name) {
    return new NamedReference<>(name);
  }

  static void removeBroadcast(final long id) {
    idempotentializer.asMap().keySet().stream()
        .filter(tuple -> tuple.getElement1().getBroadcastVarId() == id)
        .forEach(idempotentializer::invalidate);
  }

  static void invalidateBroadcastCache() {
    idempotentializer.invalidateAll();
  }

  @Override
  public Expression bind(Types.StructType struct, boolean caseSensitive) {
    BoundTerm<T> bound = (BoundTerm) ((UnboundTerm) this.term()).bind(struct, caseSensitive);
    return this.bindRangeInOperation(bound);
  }

  private List<Literal<T>> initLiteral() {
    List<Literal<T>> actualList =
        this.transientLiterals != null ? this.transientLiterals.get() : null;
    if (actualList == null) {
      actualList =
          (List<Literal<T>>)
              idempotentializer.get(new Tuple<>(this.bcVar, this.bcVar.getRelativeKeyIndex()));
      this.transientLiterals = new WeakReference<>(actualList);
    }
    return actualList;
  }

  private Expression bindRangeInOperation(BoundTerm<T> boundTerm) {

    // In case of RangeIn operation, the data types of the columns is guaranteed to be correct
    // so below conversion check is not needed
    /* List<Literal<T>> convertedLiterals = Lists.newArrayList(Iterables.filter(
        Lists.transform(literals(), lit -> {
          Literal<T> converted = lit.to(boundTerm.type());
          ValidationException.check(converted != null,
              "Invalid value for conversion to type %s: %s (%s)",
              boundTerm.type(),
              lit,
              lit.getClass().getName());
          return converted;
        }),
        lit -> lit != Literals.aboveMax() && lit != Literals.belowMin()));

    if (convertedLiterals.isEmpty()) {
      return Expressions.alwaysFalse();
    } */
    List<Literal<T>> actualLit =
        this.transientLiterals != null ? this.transientLiterals.get() : null;
    if (actualLit != null) {
      return new BoundBroadcastRangeInPredicate<>(op(), boundTerm, this.bcVar, actualLit);
    } else {
      return new BoundBroadcastRangeInPredicate<>(op(), boundTerm, this.bcVar);
    }
  }

  @Override
  public List<Literal<T>> literals() {
    return this.initLiteral();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o != null && o instanceof BroadcastHRUnboundPredicate<?>) {
      BroadcastHRUnboundPredicate<?> that = (BroadcastHRUnboundPredicate<?>) o;
      return this.term().ref().name().equals(that.term().ref().name())
          && this.bcVar.equals(that.bcVar);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.term().ref().name(), this.bcVar);
  }

  @Override
  public String toString() {
    return term() + " RANGE IN (BroadcastVar ID =" + this.bcVar.getBroadcastVarId() + ")";
  }

  public String toStringWithData() {
    return term()
        + " RANGE IN (BroadcastVar ID ="
        + this.literals().stream().map(x -> x.toString()).collect(Collectors.joining())
        + ")";
  }

  @Override
  public BroadcastedJoinKeysWrapper getBroadcastVar() {
    return this.bcVar;
  }
}
