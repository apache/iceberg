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

package org.apache.iceberg.data;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

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
    private boolean caseSensitive = true;

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

    public ScanBuilder caseInsensitive() {
      this.caseSensitive = false;
      return this;
    }

    public ScanBuilder select(String... selectedColumns) {
      this.columns = ImmutableList.copyOf(selectedColumns);
      return this;
    }

    public Iterable<Record> build() {
      return new TableScanIterable(
        table
          .newScan()
          .filter(where)
          .caseSensitive(caseSensitive)
          .select(columns),
        reuseContainers
      );
    }
  }
}
