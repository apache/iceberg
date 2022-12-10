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

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkLookupFunction extends TableFunction<RowData> {
  private static final long serialVersionUID = -7248058804931465381L;

  private static final Logger LOG = LoggerFactory.getLogger(FlinkLookupFunction.class);
  // the max number of retries before throwing exception, in case of failure to load the table into
  // cache
  private static final int MAX_RETRIES = 3;

  private final String[] projectedFields;

  private final DataType[] projectedTypes;

  private final String[] lookupKeys;

  private final long cacheMaxSize;

  private final long cacheExpireMs;

  private final int[] lookupCols;

  private transient LoadingCache<Object, List<RowData>> cache;
  // serializer to copy RowData
  private transient TypeSerializer<RowData> serializer;
  // converters to convert data from internal to external in order to generate keys for the cache
  private final DataFormatConverter[] converters;

  private FlinkInputFormat inputFormat;

  private final ScanContext context;

  private final Schema icebergSchema;

  private final TableLoader tableLoader;

  private final EncryptionManager encryption;

  private final FileIO io;

  private static final long MIN_RETRY_SLEEP_TIMEMS = 10000;

  private static final long MAX_RETRY_SLEEP_TIMEMS = 600000;

  private static final long MAX_RETRY_URATIONMS = 600000;

  private static final double SCALE_FACTOR = 2.0;

  private static final ExecutorService EXECUTOR_SERVICE =
      ThreadPools.newScheduledPool("iceberg-thread-pool-" + FlinkLookupFunction.class, 3);

  public FlinkLookupFunction(
      TableSchema schema,
      String[] lookupKeys,
      TableLoader tableLoader,
      Map<String, String> properties,
      long limit,
      List<Expression> filters) {
    Preconditions.checkNotNull(schema, "Table schema can not be null.");
    this.lookupCols = new int[lookupKeys.length];
    this.converters = new DataFormatConverter[lookupKeys.length];
    this.tableLoader = tableLoader;
    this.io = tableLoader.loadTable().io();
    this.icebergSchema = tableLoader.loadTable().schema();
    this.encryption = tableLoader.loadTable().encryption();
    this.context =
        ScanContext.builder()
            .fromProperties(properties)
            .project(icebergSchema)
            .filters(filters)
            .limit(limit)
            .build();
    this.projectedFields = schema.getFieldNames();
    this.projectedTypes = schema.getFieldDataTypes();
    this.lookupKeys = lookupKeys;
    this.cacheMaxSize = context.cacheMaxSize();
    this.cacheExpireMs = context.cacheExpireMs();

    Map<String, Integer> nameToIndex =
        IntStream.range(0, projectedFields.length)
            .boxed()
            .collect(Collectors.toMap(i -> projectedFields[i], i -> i));
    for (int i = 0; i < lookupKeys.length; i++) {
      Integer index = nameToIndex.get(lookupKeys[i]);
      Preconditions.checkArgument(
          index != null, "Lookup keys %s not selected", Arrays.toString(lookupKeys));
      converters[i] = DataFormatConverters.getConverterForDataType(projectedTypes[index]);
      lookupCols[i] = index;
    }
  }

  @Override
  public void open(FunctionContext functionContext) throws Exception {
    super.open(functionContext);
    TypeInformation<RowData> rowDataTypeInfo =
        InternalTypeInfo.ofFields(
            Arrays.stream(projectedTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new),
            projectedFields);
    serializer = rowDataTypeInfo.createSerializer(new ExecutionConfig());
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    if (cacheMaxSize == -1 || cacheExpireMs == -1) {
      builder.maximumSize(0);
    } else {
      builder.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS).maximumSize(cacheMaxSize);
    }

    cache =
        builder
            .scheduler(Scheduler.systemScheduler())
            .build(
                new CacheLoader<Object, List<RowData>>() {
                  @Override
                  public @Nullable List<RowData> load(@NonNull Object keys) throws Exception {

                    Object[] values = new Object[((Object[]) keys).length];
                    for (int i = 0; i < values.length; i++) {
                      values[i] = converters[i].toExternal(((Object[]) keys)[i]);
                    }

                    Row probeKey = Row.of(values);

                    List<Expression> filters = Lists.newArrayList();
                    for (int i = 0; i < lookupKeys.length; i++) {
                      filters.add(Expressions.equal(lookupKeys[i], values[i]));
                    }

                    inputFormat =
                        new FlinkInputFormat(
                            tableLoader,
                            icebergSchema,
                            io,
                            encryption,
                            context.copyWithFilters(filters));

                    FlinkInputSplit[] inputSplits = null;
                    List<RowData> results = Lists.newArrayList();
                    try {
                      inputSplits = inputFormat.createInputSplits(0);
                    } catch (IOException e) {
                      LOG.error("Failed to create inputsplits.", e);
                    }

                    GenericRowData reuse = new GenericRowData(projectedFields.length);

                    Tasks.foreach(inputSplits)
                        .executeWith(EXECUTOR_SERVICE)
                        .retry(MAX_RETRIES)
                        .exponentialBackoff(
                            MIN_RETRY_SLEEP_TIMEMS,
                            MAX_RETRY_SLEEP_TIMEMS,
                            MAX_RETRY_URATIONMS,
                            SCALE_FACTOR)
                        .onFailure(
                            (is, ex) -> {
                              LOG.error(
                                  "Failed to lookup iceberg table after {} retries",
                                  MAX_RETRIES,
                                  ex);
                              throw ex;
                            })
                        .run(
                            split -> {
                              inputFormat.open(split);
                              while (!inputFormat.reachedEnd()) {
                                RowData row = inputFormat.nextRecord(reuse);
                                Row key = FlinkLookupFunction.this.extractKey(row);
                                if (probeKey.equals(key)) {
                                  RowData newRow = serializer.copy(row);
                                  results.add(newRow);
                                }
                              }

                              try {
                                inputFormat.close();
                              } catch (IOException e) {
                                LOG.error("Failed to close inputFormat.", e);
                              }
                            });
                    return results;
                  }
                });
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public void eval(Object... values) {
    Preconditions.checkArgument(
        values.length == lookupKeys.length, "Number of values and lookup keys mismatch");
    cache.get(values).forEach(this::collect);
  }

  private Row extractKey(RowData row) {
    Row key = new Row(lookupCols.length);
    for (int i = 0; i < lookupCols.length; i++) {
      key.setField(i, converters[i].toExternal(row, lookupCols[i]));
    }
    return key;
  }
}
