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

import java.io.EOFException;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils class for vectoredReads, to help with things like range validation. Most of the code in
 * this class is written by @mukundthakur, and taken from
 * /hadoop-common/src/main/java/org/apache/hadoop/fs/VectoredReadUtils.java (thank you!).
 */
public final class VectoredReadUtils {
  private VectoredReadUtils() {}

  private static final Logger LOG = LoggerFactory.getLogger(VectoredReadUtils.class);

  /**
   * Validate a list of ranges (including overlapping checks) and return the sorted list.
   *
   * <p>Two ranges overlap when the start offset of second is less than the end offset of first. End
   * offset is calculated as start offset + length.
   *
   * @param input input list
   * @return a new sorted list.
   * @throws IllegalArgumentException if there are overlapping ranges or a range element is invalid
   *     (other than with negative offset)
   * @throws EOFException if the last range extends beyond the end of the file supplied or a range
   *     offset is negative
   */
  public static List<FileRange> validateAndSortRanges(final List<FileRange> input)
      throws EOFException {

    Preconditions.checkNotNull(input, "Null input list");

    if (input.isEmpty()) {
      // this may seem a pathological case, but it was valid
      // before and somehow Spark can call it through parquet.
      LOG.debug("Empty input list");
      return input;
    }

    final List<FileRange> sortedRanges;

    if (input.size() == 1) {
      validateRangeRequest(input.get(0));
      sortedRanges = input;
    } else {
      sortedRanges = sortRangeList(input);
      FileRange prev = null;
      for (final FileRange current : sortedRanges) {
        validateRangeRequest(current);
        if (prev != null) {
          Preconditions.checkArgument(
              current.offset() >= prev.offset() + prev.length(),
              "Overlapping ranges %s and %s",
              prev,
              current);
        }
        prev = current;
      }
    }

    return sortedRanges;
  }

  /**
   * Validate a single range.
   *
   * @param range range to validate.
   * @return the range.
   * @throws IllegalArgumentException the range length is negative or other invalid condition is met
   *     other than the those which raise EOFException or NullPointerException.
   * @throws EOFException the range offset is negative
   * @throws NullPointerException if the range is null.
   */
  public static FileRange validateRangeRequest(FileRange range) throws EOFException {
    Preconditions.checkNotNull(range, "range is null");

    Preconditions.checkArgument(range.length() >= 0, "length is negative in %s", range);
    if (range.offset() < 0) {
      throw new EOFException("position is negative in range " + range);
    }
    return range;
  }

  /**
   * Sort the input ranges by offset; no validation is done.
   *
   * @param input input ranges.
   * @return a new list of the ranges, sorted by offset.
   */
  public static List<FileRange> sortRangeList(List<FileRange> input) {
    final List<FileRange> l = Lists.newArrayList(input);
    l.sort(Comparator.comparingLong(FileRange::offset));
    return l;
  }
}
