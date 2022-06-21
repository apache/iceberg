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

package org.apache.iceberg.flink.source;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class FlinkLookupFunction extends TableFunction<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkLookupFunction.class);

  private final TableLoader tableLoader;
  private Table table;
  private final Schema projectedSchema;
  private final RowDataSerializer rowDataSerializer;
  private final String[] fieldNames;
  private final String[] filterFieldNames;
  private final FlinkLookupOptions lookupOptions;
  private final Cache<RowData, List<RowData>> cache;

  public FlinkLookupFunction(
      TableLoader tableLoader,
      FlinkLookupOptions lookupOptions,
      Schema projectedSchema,
      String[] fieldNames,
      String[] filterFieldNames) {
    this.tableLoader = tableLoader;
    this.lookupOptions = lookupOptions;
    this.projectedSchema = projectedSchema;
    this.fieldNames = fieldNames;
    this.filterFieldNames = filterFieldNames;

    this.rowDataSerializer = new RowDataSerializer(FlinkSchemaUtil.convert(projectedSchema));
    this.cache = lookupOptions.getCacheMaxSize() <= 0 ? null :
        Caffeine.newBuilder()
            .expireAfterWrite(lookupOptions.getCacheExpireMs(), TimeUnit.MILLISECONDS)
            .maximumSize(lookupOptions.getCacheMaxSize())
            .build();
  }

  public void eval(Object... keys) throws IOException {
    GenericRowData cacheKey = GenericRowData.of(keys);
    if (this.cache != null) {
      List<RowData> cachedRows = this.cache.getIfPresent(cacheKey);
      if (cachedRows != null) {
        for (RowData cachedRow : cachedRows) {
          collect(cachedRow);
        }
        return;
      }
    }

    try (CloseableIterable<RowData> rows = findRows(keys)) {
      List<RowData> cacheRows = new LinkedList<>();
      for (RowData row : rows) {
        collect(row);
        cacheRows.add(row);
      }
      if (this.cache != null) {
        if (!cacheRows.isEmpty() || !this.lookupOptions.isCacheIgnoreEmpty()) {
          this.cache.put(cacheKey, cacheRows);
        }
      }
    }
  }

  private CloseableIterable<RowData> findRows(Object... keys) {
    Expression rowFilter = createRowFilter(keys);

    for (int retry = 0; retry <= this.lookupOptions.getMaxRetries(); retry++) {
      try {
        table.refresh();
        CloseableIterable<CombinedScanTask> iterable = table.newScan()
            .select(this.fieldNames)
            .filter(rowFilter)
            .planTasks();
        CloseableIterable<CloseableIterable<RowData>> iterables = CloseableIterable.transform(
            iterable, createTableRowDataReader(table));
        return CloseableIterable.concat(iterables);
      } catch (Exception e) {
        LOG.error("Iceberg execute error, retry times = {}", new Object[] {Integer.valueOf(retry)}, e);
        if (retry >= this.lookupOptions.getMaxRetries()) {
          throw new RuntimeException("Execution of Iceberg failed.", e);
        }

        try {
          int retryBackoffMills = Math.min(
              this.lookupOptions.getMaxRetryBackoffMills(),
              this.lookupOptions.getBaseRetryBackoffMills() * (1 << retry));
          Thread.sleep(retryBackoffMills);
        } catch (InterruptedException e1) {
          throw new RuntimeException(e1);
        }
      }
    }
    return CloseableIterable.empty();
  }

  private Expression createRowFilter(Object... keys) {
    Expression rowFilter = null;
    for (int i = 0; i < filterFieldNames.length; i++) {
      String filterFieldName = filterFieldNames[i];
      Object filterFieldValue = keys[i];
      if (rowFilter == null) {
        rowFilter = Expressions.equal(filterFieldName, filterFieldValue);
      } else {
        rowFilter = Expressions.and(
            rowFilter,
            Expressions.equal(filterFieldName, filterFieldValue));
      }
    }
    return rowFilter;
  }

  private Function<CombinedScanTask, CloseableIterable<RowData>> createTableRowDataReader(Table snapshotTable) {
    Schema schema = snapshotTable.schema();
    FileScanTaskReader<RowData> fileScanTaskReader = new RowDataFileScanTaskReader(
        schema, this.projectedSchema, null, false, true);
    return combinedScanTask -> {
      DataIterator<RowData> dataIterator = new DataIterator<>(
          fileScanTaskReader, combinedScanTask, snapshotTable.io(), snapshotTable.encryption());
      CloseableIterable<RowData> dataIterable =  CloseableIterable.combine(() -> dataIterator, dataIterator);
      return CloseableIterable.transform(dataIterable, rowDataSerializer::copy);
    };
  }

  Cache<RowData, List<RowData>> getCache() {
    return cache;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    this.tableLoader.open();
    this.table = this.tableLoader.loadTable();
  }

  @Override
  public void close() throws Exception {
    this.tableLoader.close();
    if (this.cache != null) {
      this.cache.cleanUp();
    }
  }
}
