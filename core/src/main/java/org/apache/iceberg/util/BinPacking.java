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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class BinPacking {

  /** Computes the weight an item adds to a bin, given the items already in it. */
  interface BinWeighter<T> {
    /**
     * Returns the weight this item would add to the bin given its current contents. Must not modify
     * internal state — called speculatively during bin selection.
     */
    long tentativeWeight(T item);

    /**
     * Called after the item is confirmed to be placed in this bin. Implementations should update
     * any internal state (e.g. record which shared resources are now present in the bin).
     */
    void commit(T item);
  }

  static class StatelessBinWeighter<T> implements BinWeighter<T>, Supplier<BinWeighter<T>> {
    private final Function<T, Long> weightFunc;

    StatelessBinWeighter(Function<T, Long> weightFunc) {
      this.weightFunc = weightFunc;
    }

    @Override
    public BinWeighter<T> get() {
      return this;
    }

    @Override
    public long tentativeWeight(T item) {
      return weightFunc.apply(item);
    }

    @Override
    public void commit(T item) {}
  }

  public static class ListPacker<T> {
    private final long targetWeight;
    private final int lookback;
    private final boolean largestBinFirst;
    private final long maxItemsPerBin;

    public ListPacker(long targetWeight, int lookback, boolean largestBinFirst) {
      this(targetWeight, lookback, largestBinFirst, Long.MAX_VALUE);
    }

    public ListPacker(
        long targetWeight, int lookback, boolean largestBinFirst, long maxItemsPerBin) {
      this.targetWeight = targetWeight;
      this.lookback = lookback;
      this.largestBinFirst = largestBinFirst;
      this.maxItemsPerBin = maxItemsPerBin;
    }

    public List<List<T>> packEnd(List<T> items, Function<T, Long> weightFunc) {
      return Lists.reverse(
          ImmutableList.copyOf(
              Iterables.transform(
                  new PackingIterable<>(
                      Lists.reverse(items),
                      targetWeight,
                      lookback,
                      weightFunc,
                      largestBinFirst,
                      maxItemsPerBin),
                  Lists::reverse)));
    }

    public List<List<T>> pack(Iterable<T> items, Function<T, Long> weightFunc) {
      return ImmutableList.copyOf(
          new PackingIterable<>(
              items, targetWeight, lookback, weightFunc, largestBinFirst, maxItemsPerBin));
    }
  }

  public static class PackingIterable<T> implements Iterable<List<T>> {
    private final Iterable<T> iterable;
    private final long targetWeight;
    private final int lookback;
    private final long maxSize;
    private final Supplier<BinWeighter<T>> weighterSupplier;
    private final boolean largestBinFirst;

    public PackingIterable(
        Iterable<T> iterable, long targetWeight, int lookback, Function<T, Long> weightFunc) {
      this(iterable, targetWeight, lookback, weightFunc, false, Long.MAX_VALUE);
    }

    public PackingIterable(
        Iterable<T> iterable,
        long targetWeight,
        int lookback,
        Function<T, Long> weightFunc,
        boolean largestBinFirst) {
      this(iterable, targetWeight, lookback, weightFunc, largestBinFirst, Long.MAX_VALUE);
    }

    public PackingIterable(
        Iterable<T> iterable,
        long targetWeight,
        int lookback,
        Function<T, Long> weightFunc,
        boolean largestBinFirst,
        long maxSize) {
      this(
          iterable,
          targetWeight,
          lookback,
          new StatelessBinWeighter<>(weightFunc),
          largestBinFirst,
          maxSize);
    }

    PackingIterable(
        Iterable<T> iterable,
        long targetWeight,
        int lookback,
        Supplier<BinWeighter<T>> weighterSupplier,
        boolean largestBinFirst) {
      this(iterable, targetWeight, lookback, weighterSupplier, largestBinFirst, Long.MAX_VALUE);
    }

    PackingIterable(
        Iterable<T> iterable,
        long targetWeight,
        int lookback,
        Supplier<BinWeighter<T>> weighterSupplier,
        boolean largestBinFirst,
        long maxSize) {
      Preconditions.checkArgument(
          lookback > 0, "Bin look-back size must be greater than 0: %s", lookback);
      this.iterable = iterable;
      this.targetWeight = targetWeight;
      this.lookback = lookback;
      this.maxSize = maxSize;
      this.weighterSupplier = weighterSupplier;
      this.largestBinFirst = largestBinFirst;
    }

    @Override
    public Iterator<List<T>> iterator() {
      return new PackingIterator<>(
          iterable.iterator(), targetWeight, lookback, maxSize, weighterSupplier, largestBinFirst);
    }
  }

  private static class PackingIterator<T> implements Iterator<List<T>> {
    private final Deque<Bin<T>> bins = Lists.newLinkedList();
    private final Iterator<T> items;
    private final long targetWeight;
    private final int lookback;
    private final long maxSize;
    private final Supplier<BinWeighter<T>> weighterSupplier;
    private final StatelessBinWeighter<T> statelessWeighter;
    private final boolean largestBinFirst;

    private PackingIterator(
        Iterator<T> items,
        long targetWeight,
        int lookback,
        long maxSize,
        Supplier<BinWeighter<T>> weighterSupplier,
        boolean largestBinFirst) {
      this.items = items;
      this.targetWeight = targetWeight;
      this.lookback = lookback;
      this.maxSize = maxSize;
      this.weighterSupplier = weighterSupplier;
      this.statelessWeighter =
          weighterSupplier instanceof StatelessBinWeighter<T> weighter ? weighter : null;
      this.largestBinFirst = largestBinFirst;
    }

    @Override
    public boolean hasNext() {
      return items.hasNext() || !bins.isEmpty();
    }

    @Override
    public List<T> next() {
      while (items.hasNext()) {
        T item = items.next();

        long weight = 0L;
        Bin<T> bin = null;
        if (statelessWeighter != null) {
          weight = statelessWeighter.tentativeWeight(item);
          bin = findBin(weight);
        } else {
          for (Bin<T> candidate : bins) {
            weight = candidate.tentativeWeight(item);
            if (candidate.canAdd(weight)) {
              bin = candidate;
              break;
            }
          }
        }

        if (bin != null) {
          bin.add(item, weight);

        } else {
          bin = newBin();
          bin.add(item, statelessWeighter != null ? weight : bin.tentativeWeight(item));
          bins.addLast(bin);

          if (bins.size() > lookback) {
            Bin<T> binToRemove;
            if (largestBinFirst) {
              binToRemove = removeLargestBin(bins);
            } else {
              binToRemove = bins.removeFirst();
            }
            return ImmutableList.copyOf(binToRemove.items());
          }
        }
      }

      if (bins.isEmpty()) {
        throw new NoSuchElementException();
      }

      return ImmutableList.copyOf(bins.removeFirst().items());
    }

    private Bin<T> findBin(long weight) {
      for (Bin<T> bin : bins) {
        if (bin.canAdd(weight)) {
          return bin;
        }
      }
      return null;
    }

    private Bin<T> newBin() {
      return new Bin<>(weighterSupplier.get(), targetWeight, maxSize);
    }

    private static <T> Bin<T> removeLargestBin(Collection<Bin<T>> bins) {
      // Iterate through all bins looking for one with maximum weight, taking O(n) time.
      Bin<T> maxBin = Collections.max(bins, Comparator.comparingLong(Bin::weight));

      // Sanity check: we have removed maxBin from list of bins.
      if (bins.remove(maxBin)) {
        return maxBin;
      } else {
        throw new NoSuchElementException();
      }
    }
  }

  private static class Bin<T> {
    private final long targetWeight;
    private final long maxSize;
    private final List<T> items = Lists.newArrayList();
    private final BinWeighter<T> weighter;
    private long binWeight = 0L;
    private int binSize = 0;

    Bin(BinWeighter<T> weighter, long targetWeight, long maxSize) {
      this.weighter = weighter;
      this.targetWeight = targetWeight;
      this.maxSize = maxSize;
    }

    List<T> items() {
      return items;
    }

    long tentativeWeight(T item) {
      return weighter.tentativeWeight(item);
    }

    boolean canAdd(long weight) {
      return binWeight + weight <= targetWeight && binSize < maxSize;
    }

    void add(T item, long weight) {
      this.binWeight += weight;
      this.binSize++;
      items.add(item);
      weighter.commit(item);
    }

    long weight() {
      return binWeight;
    }
  }
}
