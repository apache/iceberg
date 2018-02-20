/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class BinPacking {
  public static class ListPacker<T> extends Packer<T> {
    private final List<List<T>> bins = Lists.newArrayList();
    private Function<T, Long> weightFunc = null;
    private boolean reverseBins = false;

    public ListPacker(long targetWeight, int lookback) {
      super(targetWeight, lookback);
    }

    @Override
    protected long weigh(T item) {
      return weightFunc.apply(item);
    }

    @Override
    protected void output(List<T> bin) {
      if (reverseBins) {
        bins.add(Lists.reverse(bin));
      } else {
        bins.add(bin);
      }
    }

    public List<List<T>> packEnd(List<T> items, Function<T, Long> weightFunc) {
      bins.clear();
      this.weightFunc = weightFunc;

      try {
        reverseBins = true;
        pack(Lists.reverse(items));
        return Lists.reverse(ImmutableList.copyOf(bins));

      } finally {
        reverseBins = false;
        bins.clear();
        this.weightFunc = null;
      }
    }

    public List<List<T>> pack(Iterable<T> items, Function<T, Long> weightFunc) {
      bins.clear();
      this.weightFunc = weightFunc;

      try {
        reverseBins = false;
        pack(items);
        return ImmutableList.copyOf(bins);

      } finally {
        bins.clear();
        this.weightFunc = null;
      }
    }
  }

  private abstract static class Packer<T> {
    private final long targetWeight;
    private final int lookback;

    protected Packer(long targetWeight, int lookback) {
      Preconditions.checkArgument(lookback > 0,
          "Bin look-back size must be greater than 0: %s", lookback);
      this.targetWeight = targetWeight;
      this.lookback = lookback;
    }

    protected abstract long weigh(T item);

    protected abstract void output(List<T> bin);

    @SuppressWarnings("unchecked")
    protected void pack(Iterable<T> items) {
      LinkedList<Bin> bins = Lists.newLinkedList();

      for (T item : items) {
        long weight = weigh(item);
        Bin bin = find(bins, weight);

        if (bin != null) {
          bin.add(item, weight);

        } else {
          bin = new Bin();
          bin.add(item, weight);
          bins.addLast(bin);

          if (bins.size() > lookback) {
            output(ImmutableList.copyOf(bins.removeFirst().items()));
          }
        }
      }

      for (Bin bin : bins) {
        output(ImmutableList.copyOf(bin.items()));
      }
    }

    private Bin find(List<Bin> bins, long weight) {
      for (Bin bin : bins) {
        if (bin.canAdd(weight)) {
          return bin;
        }
      }
      return null;
    }

    private class Bin {
      private long binWeight = 0L;
      private List<T> items = Lists.newArrayList();

      public List<T> items() {
        return items;
      }

      public boolean canAdd(long weight) {
        return (binWeight + weight <= targetWeight);
      }

      public void add(T item, long weight) {
        this.binWeight += weight;
        items.add(item);
      }
    }
  }
}
