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
package org.apache.iceberg;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Accumulates circular longitude intervals, where west greater than east crosses the antimeridian.
 *
 * <p>Bounds may conservatively widen to keep retained state bounded. Callers must supply finite
 * longitudes in [-180, 180]. Empty input produces no bounds.
 */
class LongitudeBoundsBuilder {
  private static final double MIN_LONGITUDE = -180.0;
  private static final double MAX_LONGITUDE = 180.0;
  private static final double LONGITUDE_SPAN = MAX_LONGITUDE - MIN_LONGITUDE;
  private static final int MAX_INTERVALS = 64;

  private final List<Interval> intervals = Lists.newArrayList();
  private boolean fullLongitude = false;

  void add(double west, double east) {
    if (west <= east) {
      insertSegment(west, east);
    } else {
      insertSegment(west, MAX_LONGITUDE);
      insertSegment(MIN_LONGITUDE, east);
    }
  }

  Interval build() {
    if (fullLongitude) {
      return new Interval(MIN_LONGITUDE, MAX_LONGITUDE);
    } else if (intervals.isEmpty()) {
      return null;
    }

    // The covering circular interval is the complement of the largest uncovered gap.
    Interval first = intervals.get(0);
    Interval last = intervals.get(intervals.size() - 1);
    double largestGap = LONGITUDE_SPAN + first.west - last.east;
    Interval bounds = new Interval(first.west, last.east);
    for (int index = 1; index < intervals.size(); index += 1) {
      Interval previous = intervals.get(index - 1);
      Interval next = intervals.get(index);
      double gap = next.west - previous.east;
      if (gap > largestGap) {
        largestGap = gap;
        bounds = new Interval(next.west, previous.east);
      }
    }

    return bounds;
  }

  private void insertSegment(double west, double east) {
    if (fullLongitude) {
      return;
    }

    int index = 0;
    while (index < intervals.size() && intervals.get(index).east < west) {
      index += 1;
    }

    double mergedWest = west;
    double mergedEast = east;
    int mergeFrom = index;
    while (index < intervals.size() && intervals.get(index).west <= mergedEast) {
      Interval existing = intervals.get(index);
      mergedWest = Math.min(mergedWest, existing.west);
      mergedEast = Math.max(mergedEast, existing.east);
      index += 1;
    }

    intervals.subList(mergeFrom, index).clear();
    if (mergedWest <= MIN_LONGITUDE && mergedEast >= MAX_LONGITUDE) {
      fullLongitude = true;
      intervals.clear();
      return;
    }

    intervals.add(mergeFrom, new Interval(mergedWest, mergedEast));
    compactIntervals();
  }

  private void compactIntervals() {
    while (intervals.size() > MAX_INTERVALS) {
      int mergeIndex = 1;
      double smallestGap = Double.POSITIVE_INFINITY;
      for (int index = 1; index < intervals.size(); index += 1) {
        double gap = intervals.get(index).west - intervals.get(index - 1).east;
        if (gap < smallestGap) {
          mergeIndex = index;
          smallestGap = gap;
        }
      }

      Interval previous = intervals.get(mergeIndex - 1);
      Interval next = intervals.remove(mergeIndex);
      intervals.set(mergeIndex - 1, new Interval(previous.west, next.east));
    }
  }

  static class Interval {
    private final double west;
    private final double east;

    private Interval(double west, double east) {
      this.west = west;
      this.east = east;
    }

    double west() {
      return west;
    }

    double east() {
      return east;
    }
  }
}
