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
package org.apache.iceberg.formats;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Combines the row fragments read from a data file and its column files into the rows of the
 * requested projection.
 *
 * <p>Fragments are matched by their position in the file, so only rows read by every part are
 * produced. This keeps the parts aligned when a part reads only a range of the file or skips rows
 * that cannot match a filter.
 *
 * @param <D> the type of the data records that are combined
 */
class RowAlignedStitchingIterable<D> extends CloseableGroup implements CloseableIterable<D> {
  private final List<CloseableIterable<D>> parts;
  private final Stitcher<D> stitcher;

  RowAlignedStitchingIterable(List<CloseableIterable<D>> parts, Stitcher<D> stitcher) {
    Preconditions.checkArgument(parts != null, "Invalid parts: null");
    Preconditions.checkArgument(parts.size() > 1, "Invalid parts: %s (must be > 1)", parts.size());
    Preconditions.checkArgument(stitcher != null, "Invalid stitcher: null");

    this.parts = ImmutableList.copyOf(parts);
    this.stitcher = stitcher;

    this.parts.forEach(this::addCloseable);
  }

  @Override
  public CloseableIterator<D> iterator() {
    List<SkippingCloseableIterator<D>> iterators = Lists.newArrayListWithCapacity(parts.size());
    for (int split = 0; split < parts.size(); split += 1) {
      CloseableIterator<D> iterator = parts.get(split).iterator();
      if (!(iterator instanceof SkippingCloseableIterator<D> skipping)) {
        throw new UnsupportedOperationException(
            String.format("Cannot stitch vertical split %s: row positions are not tracked", split));
      }

      iterators.add(skipping);
    }

    return new StitchingIterator<>(iterators, stitcher);
  }

  private static class StitchingIterator<D> extends CloseableGroup implements CloseableIterator<D> {
    private final List<SkippingCloseableIterator<D>> iterators;
    private final Stitcher<D> stitcher;
    private final List<D> rows;
    private boolean aligned = false;

    private StitchingIterator(List<SkippingCloseableIterator<D>> iterators, Stitcher<D> stitcher) {
      this.iterators = iterators;
      this.stitcher = stitcher;
      this.rows = Lists.newArrayList(Collections.nCopies(iterators.size(), null));
      iterators.forEach(this::addCloseable);
    }

    @Override
    public boolean hasNext() {
      // skip ahead until every split is at the same row
      long target = 0;
      int split = 0;
      int alignedSplits = 0;
      while (!aligned) {
        SkippingCloseableIterator<D> iterator = iterators.get(split);
        iterator.advanceTo(target);
        if (!iterator.hasNext()) {
          return false;
        }

        long position = iterator.position();
        if (position == target) {
          alignedSplits += 1;
        } else {
          target = position;
          alignedSplits = 1;
        }

        this.aligned = alignedSplits == iterators.size();
        split = (split + 1) % iterators.size();
      }

      return true;
    }

    @Override
    public D next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      for (int split = 0; split < iterators.size(); split += 1) {
        rows.set(split, iterators.get(split).next());
      }

      this.aligned = false;
      return stitcher.stitch(rows, 1);
    }
  }
}
