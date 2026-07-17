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
package org.apache.iceberg.flink.source.lookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

@Internal
public class IcebergLookUpReader {

  private final Table table;
  private final Schema projectedSchema;
  private final List<Expression> baseFilters;
  private final boolean caseSensitive;
  private final String nameMapping;

  public IcebergLookUpReader(
      Table table,
      Schema projectedSchema,
      List<Expression> baseFilters,
      boolean caseSensitive,
      String nameMapping) {
    this.table = table;
    this.projectedSchema = projectedSchema;
    this.baseFilters = baseFilters == null ? Collections.emptyList() : baseFilters;
    this.caseSensitive = caseSensitive;
    this.nameMapping = nameMapping;
  }

  public void read(Expression extraFilter, Consumer<RowData> consumer) throws IOException {
    Expression combined = Expressions.alwaysTrue();
    for (Expression f : baseFilters) {
      combined = Expressions.and(combined, f);
    }

    if (extraFilter != null) {
      combined = Expressions.and(combined, extraFilter);
    }

    List<Expression> readerFilters = Lists.newArrayList(baseFilters);
    if (extraFilter != null) {
      readerFilters.add(extraFilter);
    }

    RowDataFileScanTaskReader fileReader =
        new RowDataFileScanTaskReader(
            table.schema(), projectedSchema, nameMapping, caseSensitive, readerFilters);

    TableScan scan =
        table.newScan().caseSensitive(caseSensitive).project(projectedSchema).filter(combined);

    try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
      for (CombinedScanTask task : tasks) {
        try (DataIterator<RowData> rows =
            new DataIterator<>(fileReader, task, table.io(), table.encryption())) {
          while (rows.hasNext()) {
            consumer.accept(rows.next());
          }
        }
      }
    }
  }
}
