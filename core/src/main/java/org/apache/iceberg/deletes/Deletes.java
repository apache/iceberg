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
package org.apache.iceberg.deletes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FilterIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.iceberg.util.Filter;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.SortedMerge;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Deletes {

  private static final Logger LOG = LoggerFactory.getLogger(Deletes.class);

  private static final Schema POSITION_DELETE_SCHEMA =
      new Schema(MetadataColumns.DELETE_FILE_PATH, MetadataColumns.DELETE_FILE_POS);

  private static final Accessor<StructLike> FILENAME_ACCESSOR =
      POSITION_DELETE_SCHEMA.accessorForField(MetadataColumns.DELETE_FILE_PATH.fieldId());
  private static final Accessor<StructLike> POSITION_ACCESSOR =
      POSITION_DELETE_SCHEMA.accessorForField(MetadataColumns.DELETE_FILE_POS.fieldId());

  private Deletes() {}

  public static <T> CloseableIterable<T> filter(
      CloseableIterable<T> rows, Function<T, StructLike> rowToDeleteKey, StructLikeSet deleteSet) {
    if (deleteSet.isEmpty()) {
      return rows;
    }

    EqualitySetDeleteFilter<T> equalityFilter =
        new EqualitySetDeleteFilter<>(rowToDeleteKey, deleteSet);
    return equalityFilter.filter(rows);
  }

  /**
   * Returns the same rows that are input, while marking the deleted ones.
   *
   * @param rows the rows to process
   * @param isDeleted a predicate that determines if a row is deleted
   * @param deleteMarker a function that marks a row as deleted
   * @return the processed rows
   */
  public static <T> CloseableIterable<T> markDeleted(
      CloseableIterable<T> rows, Predicate<T> isDeleted, Consumer<T> deleteMarker) {
    return CloseableIterable.transform(
        rows,
        row -> {
          if (isDeleted.test(row)) {
            deleteMarker.accept(row);
          }

          return row;
        });
  }

  /**
   * Returns the remaining rows (the ones that are not deleted), while counting the deleted ones.
   *
   * @param rows the rows to process
   * @param isDeleted a predicate that determines if a row is deleted
   * @param counter a counter that counts deleted rows
   * @return the processed rows
   */
  public static <T> CloseableIterable<T> filterDeleted(
      CloseableIterable<T> rows, Predicate<T> isDeleted, DeleteCounter counter) {
    Filter<T> remainingRowsFilter =
        new Filter<T>() {
          @Override
          protected boolean shouldKeep(T item) {
            boolean deleted = isDeleted.test(item);
            if (deleted) {
              counter.increment();
            }

            return !deleted;
          }
        };

    return remainingRowsFilter.filter(rows);
  }

  public static StructLikeSet toEqualitySet(
      CloseableIterable<StructLike> eqDeletes, Types.StructType eqType) {
    try (CloseableIterable<StructLike> deletes = eqDeletes) {
      StructLikeSet deleteSet = StructLikeSet.create(eqType);
      Iterables.addAll(deleteSet, deletes);
      return deleteSet;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close equality delete source", e);
    }
  }

  /**
   * Builds a map of position delete indexes by path.
   *
   * <p>Unlike {@link #toPositionIndex(CharSequence, List)}, this method builds a position delete
   * index for each referenced data file and does not filter deletes. This can be useful when the
   * entire delete file content is needed (e.g. caching).
   *
   * @param posDeletes position deletes
   * @return the map of position delete indexes by path
   */
  public static <T extends StructLike> CharSequenceMap<PositionDeleteIndex> toPositionIndexes(
      CloseableIterable<T> posDeletes) {
    CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();

    try (CloseableIterable<T> deletes = posDeletes) {
      for (T delete : deletes) {
        CharSequence filePath = (CharSequence) FILENAME_ACCESSOR.get(delete);
        long position = (long) POSITION_ACCESSOR.get(delete);
        PositionDeleteIndex index =
            indexes.computeIfAbsent(filePath, key -> new BitmapPositionDeleteIndex());
        index.delete(position);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close position delete source", e);
    }

    return indexes;
  }

  public static <T extends StructLike> PositionDeleteIndex toPositionIndex(
      CharSequence dataLocation, List<CloseableIterable<T>> deleteFiles) {
    return toPositionIndex(dataLocation, deleteFiles, ThreadPools.getDeleteWorkerPool());
  }

  public static <T extends StructLike> PositionDeleteIndex toPositionIndex(
      CharSequence dataLocation,
      List<CloseableIterable<T>> deleteFiles,
      ExecutorService deleteWorkerPool) {
    DataFileFilter<T> locationFilter = new DataFileFilter<>(dataLocation);
    List<CloseableIterable<Long>> positions =
        Lists.transform(
            deleteFiles,
            deletes ->
                CloseableIterable.transform(
                    locationFilter.filter(deletes), row -> (Long) POSITION_ACCESSOR.get(row)));
    if (positions.size() > 1 && deleteWorkerPool != null) {
      return toPositionIndex(new ParallelIterable<>(positions, deleteWorkerPool));
    } else {
      return toPositionIndex(CloseableIterable.concat(positions));
    }
  }

  public static PositionDeleteIndex toPositionIndex(CloseableIterable<Long> posDeletes) {
    try (CloseableIterable<Long> deletes = posDeletes) {
      PositionDeleteIndex positionDeleteIndex = new BitmapPositionDeleteIndex();
      deletes.forEach(positionDeleteIndex::delete);
      return positionDeleteIndex;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close position delete source", e);
    }
  }

  public static <T> CloseableIterable<T> streamingFilter(
      CloseableIterable<T> rows,
      Function<T, Long> rowToPosition,
      CloseableIterable<Long> posDeletes) {
    return streamingFilter(rows, rowToPosition, posDeletes, new DeleteCounter());
  }

  public static <T> CloseableIterable<T> streamingFilter(
      CloseableIterable<T> rows,
      Function<T, Long> rowToPosition,
      CloseableIterable<Long> posDeletes,
      DeleteCounter counter) {
    return new PositionStreamDeleteFilter<>(rows, rowToPosition, posDeletes, counter);
  }

  public static <T> CloseableIterable<T> streamingMarker(
      CloseableIterable<T> rows,
      Function<T, Long> rowToPosition,
      CloseableIterable<Long> posDeletes,
      Consumer<T> markDeleted) {
    return new PositionStreamDeleteMarker<>(rows, rowToPosition, posDeletes, markDeleted);
  }

  public static CloseableIterable<Long> deletePositions(
      CharSequence dataLocation, CloseableIterable<StructLike> deleteFile) {
    return deletePositions(dataLocation, ImmutableList.of(deleteFile));
  }

  public static <T extends StructLike> CloseableIterable<Long> deletePositions(
      CharSequence dataLocation, List<CloseableIterable<T>> deleteFiles) {
    DataFileFilter<T> locationFilter = new DataFileFilter<>(dataLocation);
    List<CloseableIterable<Long>> positions =
        Lists.transform(
            deleteFiles,
            deletes ->
                CloseableIterable.transform(
                    locationFilter.filter(deletes), row -> (Long) POSITION_ACCESSOR.get(row)));

    return new SortedMerge<>(Long::compare, positions);
  }

  private static class EqualitySetDeleteFilter<T> extends Filter<T> {
    private final StructLikeSet deletes;
    private final Function<T, StructLike> extractEqStruct;

    protected EqualitySetDeleteFilter(Function<T, StructLike> extractEq, StructLikeSet deletes) {
      this.extractEqStruct = extractEq;
      this.deletes = deletes;
    }

    @Override
    protected boolean shouldKeep(T row) {
      return !deletes.contains(extractEqStruct.apply(row));
    }
  }

  private abstract static class PositionStreamDeleteIterable<T> extends CloseableGroup
      implements CloseableIterable<T> {
    private final CloseableIterable<T> rows;
    private final CloseableIterator<Long> deletePosIterator;
    private final Function<T, Long> rowToPosition;
    private long nextDeletePos;

    PositionStreamDeleteIterable(
        CloseableIterable<T> rows,
        Function<T, Long> rowToPosition,
        CloseableIterable<Long> deletePositions) {
      this.rows = rows;
      this.rowToPosition = rowToPosition;
      this.deletePosIterator = deletePositions.iterator();
    }

    @Override
    public CloseableIterator<T> iterator() {
      CloseableIterator<T> iter;
      if (deletePosIterator.hasNext()) {
        nextDeletePos = deletePosIterator.next();
        iter = applyDelete(rows.iterator(), deletePosIterator);
      } else {
        iter = rows.iterator();
      }

      addCloseable(iter);
      addCloseable(deletePosIterator);

      return iter;
    }

    boolean isDeleted(T row) {
      long currentPos = rowToPosition.apply(row);
      if (currentPos < nextDeletePos) {
        return false;
      }

      // consume delete positions until the next is past the current position
      boolean isDeleted = currentPos == nextDeletePos;
      while (deletePosIterator.hasNext() && nextDeletePos <= currentPos) {
        this.nextDeletePos = deletePosIterator.next();
        if (!isDeleted && currentPos == nextDeletePos) {
          // if any delete position matches the current position
          isDeleted = true;
        }
      }

      return isDeleted;
    }

    protected abstract CloseableIterator<T> applyDelete(
        CloseableIterator<T> items, CloseableIterator<Long> deletePositions);
  }

  private static class PositionStreamDeleteFilter<T> extends PositionStreamDeleteIterable<T> {
    private final DeleteCounter counter;

    PositionStreamDeleteFilter(
        CloseableIterable<T> rows,
        Function<T, Long> rowToPosition,
        CloseableIterable<Long> deletePositions,
        DeleteCounter counter) {
      super(rows, rowToPosition, deletePositions);
      this.counter = counter;
    }

    @Override
    protected CloseableIterator<T> applyDelete(
        CloseableIterator<T> items, CloseableIterator<Long> deletePositions) {
      return new FilterIterator<T>(items) {
        @Override
        protected boolean shouldKeep(T item) {
          boolean deleted = isDeleted(item);
          if (deleted) {
            counter.increment();
          }

          return !deleted;
        }

        @Override
        public void close() {
          try {
            deletePositions.close();
          } catch (IOException e) {
            LOG.warn("Error closing delete file", e);
          }
          super.close();
        }
      };
    }
  }

  private static class PositionStreamDeleteMarker<T> extends PositionStreamDeleteIterable<T> {
    private final Consumer<T> markDeleted;

    PositionStreamDeleteMarker(
        CloseableIterable<T> rows,
        Function<T, Long> rowToPosition,
        CloseableIterable<Long> deletePositions,
        Consumer<T> markDeleted) {
      super(rows, rowToPosition, deletePositions);
      this.markDeleted = markDeleted;
    }

    @Override
    protected CloseableIterator<T> applyDelete(
        CloseableIterator<T> items, CloseableIterator<Long> deletePositions) {

      return new CloseableIterator<T>() {
        @Override
        public void close() {
          try {
            deletePositions.close();
          } catch (IOException e) {
            LOG.warn("Error closing delete file", e);
          }
          try {
            items.close();
          } catch (IOException e) {
            LOG.warn("Error closing data file", e);
          }
        }

        @Override
        public boolean hasNext() {
          return items.hasNext();
        }

        @Override
        public T next() {
          T row = items.next();
          if (isDeleted(row)) {
            markDeleted.accept(row);
          }
          return row;
        }
      };
    }
  }

  private static class DataFileFilter<T extends StructLike> extends Filter<T> {
    private final CharSequence dataLocation;

    DataFileFilter(CharSequence dataLocation) {
      this.dataLocation = dataLocation;
    }

    @Override
    protected boolean shouldKeep(T posDelete) {
      return charSeqEquals(dataLocation, (CharSequence) FILENAME_ACCESSOR.get(posDelete));
    }

    private boolean charSeqEquals(CharSequence s1, CharSequence s2) {
      if (s1 == s2) {
        return true;
      }

      int count = s1.length();
      if (count != s2.length()) {
        return false;
      }

      if (s1 instanceof String && s2 instanceof String && s1.hashCode() != s2.hashCode()) {
        return false;
      }

      // File paths inside a delete file normally have more identical chars at the beginning. For
      // example, a typical
      // path is like "s3:/bucket/db/table/data/partition/00000-0-[uuid]-00001.parquet".
      // The uuid is where the difference starts. So it's faster to find the first diff backward.
      for (int i = count - 1; i >= 0; i--) {
        if (s1.charAt(i) != s2.charAt(i)) {
          return false;
        }
      }
      return true;
    }
  }
}
