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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Sums of {@link DeviceValue}s and a row count per group, merged from every batch. A group's key
 * holds each group column's value: a String for a string column, a Long otherwise (dates as days,
 * timestamps as microseconds), null for a null.
 */
public final class GroupedAggregate {
  private final int values;
  private final Map<List<Object>, double[]> groups = Maps.newHashMap();

  public GroupedAggregate(int values) {
    this.values = values;
  }

  /**
   * Merges a group's partial sums, row count and per-value non-null counts: what a {@link
   * GroupedStage} that groups rows itself reports for each of its groups.
   */
  public synchronized void add(List<Object> key, double[] sums, double rows, double[] nonNull) {
    double[] totals = groups.computeIfAbsent(key, ignored -> new double[2 * values + 1]);
    for (int v = 0; v < values; v++) {
      totals[v] += sums[v];
      totals[values + 1 + v] += nonNull[v];
    }
    totals[values] += rows;
  }

  /**
   * Each group's sums, in the order the values were given (a null value adds nothing, as in SQL's
   * SUM), then its row count, then each value's count of rows where it is not null (SQL's
   * COUNT(value), AVG's divisor, and 0 when SUM is NULL).
   */
  public synchronized Map<List<Object>, double[]> groups() {
    return Maps.newHashMap(groups);
  }

  public synchronized long rows() {
    double rows = 0;
    for (double[] totals : groups.values()) {
      rows += totals[values];
    }
    return (long) rows;
  }
}
