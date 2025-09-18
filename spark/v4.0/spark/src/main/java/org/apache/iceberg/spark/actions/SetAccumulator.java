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
package org.apache.iceberg.spark.actions;

import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.util.AccumulatorV2;

public class SetAccumulator<T> extends AccumulatorV2<T, java.util.Set<T>> {

  private final Set<T> set = Collections.synchronizedSet(Sets.newHashSet());

  @Override
  public boolean isZero() {
    return set.isEmpty();
  }

  @Override
  public AccumulatorV2<T, Set<T>> copy() {
    SetAccumulator<T> newAccumulator = new SetAccumulator<>();
    newAccumulator.set.addAll(set);
    return newAccumulator;
  }

  @Override
  public void reset() {
    set.clear();
  }

  @Override
  public void add(T v) {
    set.add(v);
  }

  @Override
  public void merge(AccumulatorV2<T, Set<T>> other) {
    set.addAll(other.value());
  }

  @Override
  public Set<T> value() {
    return set;
  }
}
