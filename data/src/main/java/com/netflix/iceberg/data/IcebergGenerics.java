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

package com.netflix.iceberg.data;

import com.google.common.collect.ImmutableList;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import java.util.List;

public class IcebergGenerics {
  private IcebergGenerics() {
  }

  /**
   * Returns a builder to configure a read of the given table that produces generic records.
   *
   * @param table an Iceberg table
   * @return a builder to configure the scan
   */
  public static ScanBuilder read(Table table) {
    return new ScanBuilder(table);
  }

  public static class ScanBuilder {
    private final Table table;
    private Expression where = Expressions.alwaysTrue();
    private List<String> columns = ImmutableList.of("*");
    private boolean reuseContainers = false;

    public ScanBuilder(Table table) {
      this.table = table;
    }

    public ScanBuilder reuseContainers() {
      this.reuseContainers = true;
      return this;
    }

    public ScanBuilder where(Expression rowFilter) {
      this.where = Expressions.and(where, rowFilter);
      return this;
    }

    public ScanBuilder select(String... columns) {
      this.columns = ImmutableList.copyOf(columns);
      return this;
    }

    public Iterable<Record> build() {
      return new TableScanIterable(table.newScan().filter(where).select(columns), reuseContainers);
    }
  }
}
