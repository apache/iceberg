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
package org.apache.iceberg.expressions;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class CountAggregate<T> extends BoundAggregate<T, Long> {
  protected CountAggregate(Operation op, BoundTerm<T> term) {
    super(op, term);
  }

  @Override
  public Long eval(StructLike struct) {
    return countFor(struct);
  }

  @Override
  public Long eval(DataFile file) {
    return countFor(file);
  }

  protected Long countFor(StructLike row) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement countFor(StructLike)");
  }

  protected Long countFor(DataFile file) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement countFor(DataFile)");
  }

  @Override
  public Aggregator<Long> newAggregator(List<BoundGroupBy<?, ?>> groupBys) {
    return new CountAggregator<>(this, groupBys);
  }

  private static class CountAggregator<T> extends NullSafeAggregator<T, Long> {
    private Long count = 0L;
    private Map<StructLike, Long> countP = Maps.newHashMap();

    CountAggregator(BoundAggregate<T, Long> aggregate, List<BoundGroupBy<?, ?>> groupBys) {
      super(aggregate, groupBys);
    }

    @Override
    protected void update(Long value) {
      count += value;
    }

    @Override
    protected void update(Long value, StructLike partitionKey) {
      if (countP.get(partitionKey) == null) {
        countP.put(partitionKey, value);
      } else {
        countP.put(partitionKey, countP.get(partitionKey) + value);
      }
    }

    @Override
    protected Long current() {
      return count;
    }

    @Override
    protected Map<StructLike, Long> currentPartition() {
      return countP;
    }
  }
}
