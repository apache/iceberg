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

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkLookupFunction extends TableFunction<RowData> {
  private static final long serialVersionUID = -7248058804931465381L;
  private static final Logger LOG = LoggerFactory.getLogger(FlinkLookupFunction.class);

  // the max number of retries before throwing exception, in case of failure to load the table into
  // cache
  private static final int MAX_RETRIES = 3;

  private static final Duration RETRY_INTERVAL = Duration.ofSeconds(10);

  private final RowData.FieldGetter[] lookupFieldGetters;
  private final Duration reloadInterval;
  private final TypeSerializer<RowData> serializer;
  private final RowType rowType;

  // cache for lookup data
  private transient Map<RowData, List<RowData>> cache;
  // timestamp when cache expires
  private transient long nextLoadTime;

  private final TableLoader tableLoader;

  private FlinkInputFormat inputFormat;

  private final ScanContext context;

  public FlinkLookupFunction(
      RowType rowType,
      int[] lookupKeys,
      TableLoader tableLoader,
      Map<String, String> properties,
      ReadableConfig readableConfig) {
    this.rowType = rowType;
    this.lookupFieldGetters = new RowData.FieldGetter[lookupKeys.length];
    for (int i = 0; i < lookupKeys.length; i++) {
      lookupFieldGetters[i] =
          RowData.createFieldGetter(rowType.getTypeAt(lookupKeys[i]), lookupKeys[i]);
    }
    tableLoader.open();
    this.context =
        ScanContext.builder()
            .resolveConfig(tableLoader.loadTable(), properties, readableConfig)
            .project(tableLoader.loadTable().schema())
            .build();
    this.reloadInterval = context.reloadInterval();
    this.serializer = InternalSerializers.create(rowType);
    this.tableLoader = tableLoader;
  }

  @Override
  public void open(FunctionContext functionContext) throws Exception {
    super.open(functionContext);
    cache = Maps.newHashMap();
    nextLoadTime = -1L;
  }

  @Override
  public TypeInformation<RowData> getResultType() {
    return InternalTypeInfo.of(rowType);
  }

  public void eval(Object... values) {
    checkCacheReload();
    RowData lookupKey = GenericRowData.of(values);
    List<RowData> matchedRows = cache.get(lookupKey);
    if (matchedRows != null) {
      for (RowData matchedRow : matchedRows) {
        collect(matchedRow);
      }
    }
  }

  private void checkCacheReload() {
    if (nextLoadTime > System.currentTimeMillis()) {
      return;
    }

    if (nextLoadTime > 0) {
      LOG.info(
          "Lookup join cache has expired after {} minute(s), reloading",
          reloadInterval.toMinutes());
    } else {
      LOG.info("Populating lookup join cache");
    }

    int numRetry = 0;
    FlinkInputSplit[] inputSplits = getInputSplits();
    while (true) {
      cache.clear();
      try {
        for (int i = 0; i < inputSplits.length; i++) {
          long count = 0;
          GenericRowData reuse = new GenericRowData(rowType.getFieldCount());
          inputFormat.open(inputSplits[i]);
          while (!inputFormat.reachedEnd()) {
            RowData row = inputFormat.nextRecord(reuse);
            RowData key = extractLookupKey(row);
            List<RowData> rows = cache.computeIfAbsent(key, k -> Lists.newArrayList());
            rows.add(serializer.copy(row));
          }

          nextLoadTime = System.currentTimeMillis() + reloadInterval.toMillis();
          LOG.info("Loaded {} row(s) into lookup join cache", count);
          return;
        }
      } catch (Exception e) {
        if (numRetry >= MAX_RETRIES) {
          throw new FlinkRuntimeException(
              String.format("Failed to load table into cache after %d retries", numRetry), e);
        }

        numRetry++;
        long toSleep = numRetry * RETRY_INTERVAL.toMillis();
        LOG.warn("Failed to load table into cache, will retry in {} seconds", toSleep / 1000, e);
        try {
          Thread.sleep(toSleep);
        } catch (InterruptedException ex) {
          LOG.warn("Interrupted while waiting to retry failed cache load, aborting", ex);
          throw new FlinkRuntimeException(ex);
        }
      }
    }
  }

  private FlinkInputSplit[] getInputSplits() {
    tableLoader.open();
    inputFormat =
        new FlinkInputFormat(
            tableLoader,
            tableLoader.loadTable().schema(),
            tableLoader.loadTable().io(),
            tableLoader.loadTable().encryption(),
            context);

    FlinkInputSplit[] inputSplits = null;

    try {
      inputSplits = inputFormat.createInputSplits(0);
    } catch (IOException e) {
      LOG.error("Failed to create inputSplits.", e);
    }
    return inputSplits;
  }

  private RowData extractLookupKey(RowData row) {
    GenericRowData key = new GenericRowData(lookupFieldGetters.length);
    for (int i = 0; i < lookupFieldGetters.length; i++) {
      key.setField(i, lookupFieldGetters[i].getFieldOrNull(row));
    }
    return key;
  }
}
