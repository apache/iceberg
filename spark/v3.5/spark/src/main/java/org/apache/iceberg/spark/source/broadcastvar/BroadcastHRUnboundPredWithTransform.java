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
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.expressions.BoundTerm;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.spark.source.Tuple;
import org.apache.iceberg.spark.source.broadcastvar.broadcastutils.LiteralListWrapper;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.bcvar.ArrayWrapper;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;

// TODO: Asif. Review this class for optimization and correctness
public class BroadcastHRUnboundPredWithTransform<S, T> extends UnboundPredicate<T>
    implements BroadcastVarPredicate {

  static final ThreadLocal<Boolean> fixDateFlag = ThreadLocal.withInitial(() -> false);
  static final LoadingCache<Tuple<BroadcastedJoinKeysWrapper, Function>, ArrayWrapper>
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
                    int keyIndex = bcj.getKeyIndex();

                    boolean fixDate = fixDateFlag.get();

                    return BroadcastUtil.evaluateLiteralWithTransform(bcj, tf, fixDate);
                  });
  private final BroadcastedJoinKeysWrapper bcVar;
  private final Function<S, T> transform;

  private final boolean fixDate;

  public BroadcastHRUnboundPredWithTransform(
      String termName, BroadcastedJoinKeysWrapper bcVar, Function<S, T> transformm) {
    this(termName, bcVar, transformm, false);
  }

  public BroadcastHRUnboundPredWithTransform(
      String termName,
      BroadcastedJoinKeysWrapper bcVar,
      Function<S, T> transformm,
      boolean fixDateX) {
    super(Operation.RANGE_IN, BroadcastHRUnboundPredicate.ref(termName));
    this.bcVar = bcVar;
    this.transform = transformm;
    this.fixDate = fixDateX;
  }

  @Override
  public List<Literal<T>> literals() {
      return new LiteralListWrapper<T>(idempotentializer.get(new Tuple<>(bcVar, this.transform)));
  }

  @Override
  public Expression bind(Types.StructType struct, boolean caseSensitive) {
    BoundTerm<T> bound = (BoundTerm) ((UnboundTerm) this.term()).bind(struct, caseSensitive);
    return this.bindRangeInOperation(bound);
  }

  private Expression bindRangeInOperation(BoundTerm<T> boundTerm) {
    return new BoundBroadcastPredicateWithTransform<>(
        op(),
        boundTerm,
        this.bcVar,
        this.transform,
        this.fixDate);
  }

  @Override
  public String toString() {
    return term() + " range in (" + this.bcVar.toString() + "," + this.transform.toString() + ")";
  }

  @Override
  public BroadcastedJoinKeysWrapper getBroadcastVar() {
    return this.bcVar;
  }
}
