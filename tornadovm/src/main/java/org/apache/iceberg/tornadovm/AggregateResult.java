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
package org.apache.iceberg.tornadovm;

import java.util.Map;
import java.util.TreeMap;

/**
 * The result of an aggregate scan: for each group, the live row count and one sum a value column.
 * An ungrouped aggregate has one group, keyed 0.
 */
public final class AggregateResult {

  /** One group's partial or final totals. */
  public static final class Group {
    private long count;
    private final double[] sums;

    Group(int columns) {
      this.sums = new double[columns];
    }

    public long count() {
      return count;
    }

    public double[] sums() {
      return sums;
    }

    void add(long rows, double[] values) {
      count += rows;
      for (int c = 0; c < sums.length; c++) {
        sums[c] += values[c];
      }
    }
  }

  private final int columns;
  private final Map<Integer, Group> groups = new TreeMap<>();

  AggregateResult(int columns) {
    this.columns = columns;
  }

  /** Merges one file's partial totals for a group, the way a global aggregate merges partials. */
  synchronized void add(int key, long rows, double[] sums) {
    groups.computeIfAbsent(key, ignored -> new Group(columns)).add(rows, sums);
  }

  public Map<Integer, Group> groups() {
    return groups;
  }

  public long rows() {
    return groups.values().stream().mapToLong(Group::count).sum();
  }

  public int columns() {
    return columns;
  }
}
