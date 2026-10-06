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
 * Combines the batches read from a data file and its column files into batches of the requested
 * projection.
 *
 * <p>Rows are matched by their position in the file, so only rows read by every part are produced.
 * The parts may be read in batches with different boundaries, so each batch produced holds the rows
 * that the current batches of all parts have in common.
 *
 * @param <D> the type of the batches that are combined
 */
class VectorizedStitchingIterable<D> extends CloseableGroup implements CloseableIterable<D> {
  private final List<CloseableIterable<D>> parts;
  private final VectorizedStitcher<D> stitcher;

  VectorizedStitchingIterable(List<CloseableIterable<D>> parts, VectorizedStitcher<D> stitcher) {
    Preconditions.checkArgument(parts != null, "Invalid parts: null");
    Preconditions.checkArgument(parts.size() > 1, "Invalid parts: %s (must be > 1)", parts.size());
    Preconditions.checkArgument(stitcher != null, "Invalid stitcher: null");

    this.parts = ImmutableList.copyOf(parts);
    this.stitcher = stitcher;

    this.parts.forEach(this::addCloseable);
  }

  @Override
  public CloseableIterator<D> iterator() {
    List<PartBatch<D>> partBatches = Lists.newArrayListWithCapacity(parts.size());
    for (int split = 0; split < parts.size(); split += 1) {
      CloseableIterator<D> iterator = parts.get(split).iterator();
      if (!(iterator instanceof SkippingCloseableIterator<D> skipping)) {
        throw new UnsupportedOperationException(
            String.format("Cannot stitch vertical split %s: row positions are not tracked", split));
      }

      partBatches.add(new PartBatch<>(split, skipping, stitcher));
    }

    StitchingIterator<D> iterator = new StitchingIterator<>(partBatches, stitcher);
    addCloseable(iterator);
    return iterator;
  }

  /** The current batch of a part and the range of row positions it holds. */
  private static class PartBatch<D> {
    private final int split;
    private final SkippingCloseableIterator<D> iterator;
    private final VectorizedStitcher<D> stitcher;
    private D batch = null;
    private long start = 0;
    private long end = 0;

    private PartBatch(
        int split, SkippingCloseableIterator<D> iterator, VectorizedStitcher<D> stitcher) {
      this.split = split;
      this.iterator = iterator;
      this.stitcher = stitcher;
    }

    /**
     * Returns the first position at or after a position that this part has a row for, or -1 if it
     * has no more rows, without reading the batch that holds it.
     */
    private long seek(long position) {
      if (position < end) {
        return position;
      }

      iterator.advanceTo(position);
      if (!iterator.hasNext()) {
        return -1;
      }

      // advanceTo does not split the batch that holds the position, so it may start before it
      return Math.max(iterator.position(), position);
    }

    /** Reads the batch that holds a position, unless the current batch does. */
    private void load(long position) {
      if (position < end) {
        return;
      }

      this.start = iterator.position();
      this.batch = iterator.next();
      this.end = start + stitcher.numRows(batch);
      Preconditions.checkState(
          start <= position && position < end,
          "Cannot stitch vertical split %s: batch of rows [%s, %s) does not hold row %s",
          split,
          start,
          end,
          position);
    }
  }

  private static class StitchingIterator<D> extends CloseableGroup implements CloseableIterator<D> {
    private final List<PartBatch<D>> parts;
    private final VectorizedStitcher<D> stitcher;
    private final List<D> window;
    private long nextPosition = 0;
    private boolean aligned = false;

    private StitchingIterator(List<PartBatch<D>> parts, VectorizedStitcher<D> stitcher) {
      this.parts = parts;
      this.stitcher = stitcher;
      this.window = Lists.newArrayList(Collections.nCopies(parts.size(), null));
      parts.forEach(part -> addCloseable(part.iterator));
    }

    @Override
    public boolean hasNext() {
      // skip ahead until every part has a row at the same position
      int split = 0;
      int alignedSplits = 0;
      while (!aligned) {
        long position = parts.get(split).seek(nextPosition);
        if (position < 0) {
          return false;
        }

        if (position == nextPosition) {
          alignedSplits += 1;
        } else {
          this.nextPosition = position;
          alignedSplits = 1;
        }

        this.aligned = alignedSplits == parts.size();
        split = (split + 1) % parts.size();
      }

      return true;
    }

    @Override
    public D next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      long end = Long.MAX_VALUE;
      for (PartBatch<D> part : parts) {
        part.load(nextPosition);
        end = Math.min(end, part.end);
      }

      int count = (int) (end - nextPosition);
      for (int split = 0; split < parts.size(); split += 1) {
        window.set(split, slice(parts.get(split), count));
      }

      this.nextPosition = end;
      this.aligned = false;
      return stitcher.stitch(window, count);
    }

    private D slice(PartBatch<D> part, int count) {
      int offset = (int) (nextPosition - part.start);
      if (offset == 0 && count == part.end - part.start) {
        return part.batch;
      }

      return stitcher.slice(part.batch, offset, count);
    }
  }
}
