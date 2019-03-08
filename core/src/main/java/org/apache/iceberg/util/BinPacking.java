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

package org.apache.iceberg.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class BinPacking {
  public static class ListPacker<T> {
    private final long targetWeight;
    private final int lookback;
    private final boolean expensiveTaskFirst;

    public ListPacker(long targetWeight, int lookback, boolean expensiveTaskFirst) {
      this.targetWeight = targetWeight;
      this.lookback = lookback;
      this.expensiveTaskFirst = expensiveTaskFirst;
    }

    public List<List<T>> packEnd(List<T> items, Function<T, Long> weightFunc) {
      return Lists.reverse(ImmutableList.copyOf(Iterables.transform(
          new PackingIterable<>(Lists.reverse(items), targetWeight, lookback, weightFunc, expensiveTaskFirst),
          Lists::reverse)));
    }

    public List<List<T>> pack(Iterable<T> items, Function<T, Long> weightFunc) {
      return ImmutableList.copyOf(new PackingIterable<>(items, targetWeight, lookback, weightFunc, expensiveTaskFirst));
    }
  }

  public static class PackingIterable<T> implements Iterable<List<T>> {
    private final Iterable<T> iterable;
    private final long targetWeight;
    private final int lookback;
    private final Function<T, Long> weightFunc;
    private final boolean expensiveTaskFirst;

    public PackingIterable(Iterable<T> iterable, long targetWeight, int lookback,
                           Function<T, Long> weightFunc, boolean expensiveTaskFirst) {
      Preconditions.checkArgument(lookback > 0,
          "Bin look-back size must be greater than 0: %s", lookback);
      this.iterable = iterable;
      this.targetWeight = targetWeight;
      this.lookback = lookback;
      this.weightFunc = weightFunc;
      this.expensiveTaskFirst = expensiveTaskFirst;
    }

    @Override
    public Iterator<List<T>> iterator() {
      return new PackingIterator<>(iterable.iterator(), targetWeight, lookback, weightFunc, expensiveTaskFirst);
    }
  }

  private static class PackingIterator<T> implements Iterator<List<T>> {
    private final LinkedList<Bin> bins = Lists.newLinkedList();
    private final Iterator<T> items;
    private final long targetWeight;
    private final int lookback;
    private final Function<T, Long> weightFunc;
    private final boolean expensiveTaskFirst;

    private PackingIterator(Iterator<T> items, long targetWeight, int lookback,
                            Function<T, Long> weightFunc, boolean expensiveTaskFirst) {
      this.items = items;
      this.targetWeight = targetWeight;
      this.lookback = lookback;
      this.weightFunc = weightFunc;
      this.expensiveTaskFirst = expensiveTaskFirst;
    }

    public boolean hasNext() {
      return items.hasNext() || !bins.isEmpty();
    }

    public List<T> next() {
      while (items.hasNext()) {
        T item = items.next();

        long weight = weightFunc.apply(item);
        Bin bin = find(bins, weight);

        if (bin != null) {
          bin.add(item, weight);

        } else {
          bin = new Bin();
          bin.add(item, weight);
          bins.addLast(bin);

          if (bins.size() > lookback) {
            if (expensiveTaskFirst) {
              Bin maxBin = null;

              // Iterate through all bins looking for one with maximum weight, taking O(n) time.
              for (Bin currBin : bins) {
                maxBin = maxBin == null ? currBin : (maxBin.weight() >= currBin.weight() ? maxBin : currBin);
              }
              // Sanity checks: we have resolved maxBin and removed it from list of bins.
              if (maxBin != null && bins.remove(maxBin)) {
                return ImmutableList.copyOf(maxBin.items());
              } else {
                throw new NoSuchElementException();
              }
            } else {
              return ImmutableList.copyOf(bins.removeFirst().items());
            }
          }
        }
      }

      if (bins.isEmpty()) {
        throw new NoSuchElementException();
      }

      return ImmutableList.copyOf(bins.removeFirst().items());
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

      public long weight() {
        return binWeight;
      }
    }
  }
}
