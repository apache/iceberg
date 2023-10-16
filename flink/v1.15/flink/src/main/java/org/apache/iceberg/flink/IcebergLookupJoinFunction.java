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
package org.apache.iceberg.flink;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.cache.CacheBuilder;
import org.apache.iceberg.relocated.com.google.common.cache.CacheLoader;
import org.apache.iceberg.relocated.com.google.common.cache.LoadingCache;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ListenableFuture;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ListenableFutureTask;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;

public class IcebergLookupJoinFunction implements Serializable {
  private static final String LOOKUP_CACHE_MAX_ROWS = "lookup.cache.max-rows";
  private static final String LOOKUP_CACHE_TTL = "lookup.cache.ttl";
  private static final String LOOKUP_CACHE_TYPE = "lookup.cache.type";
  private static final String LOOKUP_CACHE_MAX_ROWS_DEFAULT = "1000000";
  private static final String LOOKUP_CACHE_TTL_DEFAULT = "600";
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private transient LoadingCache<GenericRowData, GenericRowData> loadingCache = null;
  private final TableLoader loader;
  private final Map<String, String> properties;

  private final DataType[] lookupKeyTypes;
  private final String[] lookupKeyNames;
  private final int fieldCount;
  private final List<Expression> filters;

  public IcebergLookupJoinFunction(
      TableLoader loader,
      DataType[] lookupKeyTypes,
      String[] lookupKeyNames,
      Map<String, String> properties,
      int fieldCount,
      List<Expression> filters) {
    this.loader = loader;
    this.lookupKeyTypes = lookupKeyTypes;
    this.lookupKeyNames = lookupKeyNames;
    this.properties = properties;
    this.fieldCount = fieldCount;
    this.filters = filters;
  }

  TableLoader tableLoader() {
    return this.loader;
  }

  Table table() {
    return this.loader.loadTable();
  }

  Map<String, String> getProperties() {
    return this.properties;
  }

  LoadingCache<GenericRowData, GenericRowData> loadingCache() {
    return this.loadingCache;
  }

  public void createRefreshLoadingCache(
      int maxTTLSec, int maxRows, ExecutorService executorService) {
    loadingCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(maxTTLSec, TimeUnit.SECONDS)
            .maximumSize(maxRows)
            .build(getLoader(executorService));
  }

  public void createExpireLoadingCache(
      int maxTTLSec, int maxRows, ExecutorService executorService) {
    loadingCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(maxTTLSec, TimeUnit.SECONDS)
            .maximumSize(maxRows)
            .build(getLoader(executorService));
  }

  private LoadingCache<GenericRowData, GenericRowData> buildCache(Table table) {
    // use 5 async threads to refresh the cache
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    // get the configuration in WITH parameters
    String cacheMaxRows =
        getProperties().getOrDefault(LOOKUP_CACHE_MAX_ROWS, LOOKUP_CACHE_MAX_ROWS_DEFAULT);
    String cacheMaxTTL = getProperties().getOrDefault(LOOKUP_CACHE_TTL, LOOKUP_CACHE_TTL_DEFAULT);

    // the async thread for pretreatment use in the scenario with not huge data
    Thread thread =
        new Thread(
            () -> {
              loadAllRows(table);
            });

    int maxRows = Integer.parseInt(cacheMaxRows);
    int maxTTLSec = Integer.parseInt(cacheMaxTTL);

    String cacheType = properties.getOrDefault(LOOKUP_CACHE_TYPE, "lru");
    if (maxRows > 0 && maxTTLSec > 0) {
      if ("all".equals(cacheType)) {
        createRefreshLoadingCache(maxTTLSec, maxRows, executorService);
        // start the pretreatment and put the data to the cache for the scenario with not huge
        // data
        thread.start();
      } else if ("lru".equals(cacheType)) {
        createExpireLoadingCache(maxTTLSec, maxRows, executorService);
      } else {
        throw new IllegalArgumentException();
      }
    }

    return loadingCache;
  }

  @NotNull
  private CacheLoader<GenericRowData, GenericRowData> getLoader(ExecutorService executorService) {
    return new CacheLoader<GenericRowData, GenericRowData>() {
      @Override
      public GenericRowData load(@NotNull GenericRowData genericRowData) {
        return loadData(genericRowData);
      }

      @Override
      // use the async task for loading data, avoid effecting to get data from the cache
      public ListenableFuture<GenericRowData> reload(
          @NotNull GenericRowData key, @NotNull GenericRowData oldValue) {
        ListenableFutureTask<GenericRowData> futureTask =
            ListenableFutureTask.create(() -> load(key));
        executorService.execute(futureTask);
        return futureTask;
      }
    };
  }

  private GenericRowData loadData(GenericRowData lookupKey) {

    IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table());
    // search the lookup key row
    scanBuilderWithFilters(lookupKey, scanBuilder);

    try (CloseableIterable<Record> lookupResult = scanBuilder.build()) {
      // get only one record for lookup
      CloseableIterator<Record> iter = lookupResult.iterator();
      GenericRowData rowData;
      if (iter.hasNext()) {
        Record record = iter.next();
        rowData = convert(record.struct(), record);
        return rowData;
      } else {
        // need to return one instance
        return new GenericRowData(this.fieldCount);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * get the data from iceberg and change the data to GenericRowData, because the read() return the
   * GenericRecord data. Flink need to merge the GenericRowData in left table and GenericRowData in
   * right table. the method can change the map,row and array, reference to the test
   * class:org.apache.iceberg.flink.RowDataConverter
   */
  private GenericRowData convert(Types.StructType struct, Record record) {
    GenericRowData rowData = new GenericRowData(struct.fields().size());
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);

      Type fieldType = field.type();

      switch (fieldType.typeId()) {
        case STRUCT:
          rowData.setField(i, convert(fieldType.asStructType(), record.get(i)));
          break;
        case LIST:
          rowData.setField(i, convert(fieldType.asListType(), record.get(i)));
          break;
        case MAP:
          rowData.setField(i, convert(fieldType.asMapType(), record.get(i)));
          break;
        default:
          rowData.setField(i, convert(fieldType, record.get(i)));
      }
    }
    return rowData;
  }

  private Object convert(Type type, Object object) {
    if (object == null) {
      return null;
    }

    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case FIXED:
        return object;
      case DATE:
        return (int) ChronoUnit.DAYS.between(EPOCH_DAY, (LocalDate) object);
      case TIME:
        // Iceberg's time is in microseconds, while flink's time is in milliseconds.
        LocalTime localTime = (LocalTime) object;
        return (int) TimeUnit.NANOSECONDS.toMillis(localTime.toNanoOfDay());
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return TimestampData.fromInstant(((OffsetDateTime) object).toInstant());
        } else {
          return TimestampData.fromLocalDateTime((LocalDateTime) object);
        }
      case STRING:
        return StringData.fromString((String) object);
      case UUID:
        UUID uuid = (UUID) object;
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
      case BINARY:
        ByteBuffer buffer = (ByteBuffer) object;
        return Arrays.copyOfRange(
            buffer.array(),
            buffer.arrayOffset() + buffer.position(),
            buffer.arrayOffset() + buffer.remaining());
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) type;
        return DecimalData.fromBigDecimal(
            (BigDecimal) object, decimalType.precision(), decimalType.scale());
      case STRUCT:
        return convert(type.asStructType(), (Record) object);
      case LIST:
        List<?> list = (List<?>) object;
        Object[] convertedArray = new Object[list.size()];
        for (int i = 0; i < convertedArray.length; i++) {
          convertedArray[i] = convert(type.asListType().elementType(), list.get(i));
        }
        return new GenericArrayData(convertedArray);
      case MAP:
        Map<Object, Object> convertedMap = Maps.newLinkedHashMap();
        Map<?, ?> map = (Map<?, ?>) object;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          convertedMap.put(
              convert(type.asMapType().keyType(), entry.getKey()),
              convert(type.asMapType().valueType(), entry.getValue()));
        }
        return new GenericMapData(convertedMap);
      default:
        throw new UnsupportedOperationException("Not a supported type: " + type);
    }
  }

  /** use data type in sql to get the data in GenericRowData */
  private void scanBuilderWithFilters(
      GenericRowData lookup, IcebergGenerics.ScanBuilder scanBuilder) {
    filters.forEach(scanBuilder::where);

    for (int index = 0; index < lookup.getArity(); index++) {
      LogicalType fieldType = lookupKeyTypes[index].getLogicalType();
      switch (fieldType.getTypeRoot()) {
        case INTEGER:
        case DATE:
        case TIME_WITHOUT_TIME_ZONE:
        case INTERVAL_YEAR_MONTH:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getInt(index)));
          break;
        case TINYINT:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getByte(index)));
          break;
        case SMALLINT:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getShort(index)));
          break;
        case VARCHAR:
        case CHAR:
          scanBuilder.where(
              Expressions.equal(lookupKeyNames[index], lookup.getString(index).toString()));
          break;
        case BINARY:
        case VARBINARY:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getBinary(index)));
          break;
        case BOOLEAN:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getBoolean(index)));
          break;
        case DECIMAL:
          final int decimalPrecision = LogicalTypeChecks.getPrecision(fieldType);
          final int decimalScale = LogicalTypeChecks.getScale(fieldType);
          scanBuilder.where(
              Expressions.equal(
                  lookupKeyNames[index], lookup.getDecimal(index, decimalPrecision, decimalScale)));
          break;
        case FLOAT:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getFloat(index)));
          break;
        case DOUBLE:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getDouble(index)));
          break;
        case BIGINT:
        case INTERVAL_DAY_TIME:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getLong(index)));
          break;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          final int timestampPrecision = LogicalTypeChecks.getPrecision(fieldType);
          scanBuilder.where(
              Expressions.equal(
                  lookupKeyNames[index], lookup.getTimestamp(index, timestampPrecision)));
          break;
        case TIMESTAMP_WITH_TIME_ZONE:
          throw new UnsupportedOperationException();
        case ARRAY:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getArray(index)));
          break;
        case MULTISET:
        case MAP:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getMap(index)));
          break;
        case ROW:
        case STRUCTURED_TYPE:
          final int rowFieldCount = LogicalTypeChecks.getFieldCount(fieldType);
          scanBuilder.where(
              Expressions.equal(lookupKeyNames[index], lookup.getRow(index, rowFieldCount)));
          break;
        case RAW:
          scanBuilder.where(Expressions.equal(lookupKeyNames[index], lookup.getRawValue(index)));
          break;
        case NULL:
        case SYMBOL:
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  private void loadAllRows(Table table) {
    Expression filterExpressions = Expressions.alwaysTrue();
    for (Expression filter : filters) {
      filterExpressions = Expressions.and(filterExpressions, filter);
    }
    try (CloseableIterable<Record> result =
        IcebergGenerics.read(table).where(filterExpressions).build()) {
      GenericRowData rowData;
      // data pretreatment to put the data to cache
      for (Record record : result) {
        // GenericRowData rowData = new GenericRowData(getColumnsOptions().FieldCount);
        rowData = convert(record.struct(), record);

        GenericRowData lookup = new GenericRowData(this.lookupKeyNames.length);
        for (int i = 0; i < lookup.getArity(); i++) {
          lookup.setField(i, record.getField(this.lookupKeyNames[i]));
        }

        if (loadingCache().getIfPresent(lookup) == null) {
          loadingCache().put(lookup, rowData);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public AsyncTableFunction<RowData> buildAsyncTableFunction() {
    return new AsyncIcebergRowDataLookupJoinFunc(this.loader);
  }

  public TableFunction<RowData> buildTableFunction() {
    return new IcebergRowDataLookupJoinFunc(this.loader);
  }

  public class IcebergRowDataLookupJoinFunc extends TableFunction<RowData> {
    private transient LoadingCache<GenericRowData, GenericRowData> loadingCache = null;
    private final TableLoader loader;

    private IcebergRowDataLookupJoinFunc(TableLoader loader) {
      this.loader = loader;
    }

    @Override
    public void close() throws Exception {
      if (loadingCache != null) {
        loadingCache.invalidateAll();
      }
      super.close();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
      super.open(context);
      this.loader.open();
      this.loadingCache = buildCache(loader.loadTable());
    }

    public void eval(Object... obj) {
      GenericRowData row;
      GenericRowData lookupKey;
      lookupKey = GenericRowData.of(obj);

      if (loadingCache != null) {
        GenericRowData cachedRow = null;
        try {
          cachedRow = loadingCache.get(lookupKey);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
        collect(cachedRow);
      }
    }
  }

  public class AsyncIcebergRowDataLookupJoinFunc extends AsyncTableFunction<RowData> {
    private transient LoadingCache<GenericRowData, GenericRowData> loadingCache = null;
    private final TableLoader loader;

    public AsyncIcebergRowDataLookupJoinFunc(TableLoader loader) {
      this.loader = loader;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
      super.open(context);
      this.loader.open();
      this.loadingCache = buildCache(loader.loadTable());
    }

    @Override
    public void close() throws Exception {
      if (loadingCache != null) {
        loadingCache.invalidateAll();
      }
      super.close();
    }

    public void eval(CompletableFuture<List<GenericRowData>> resultFuture, Object... obj) {
      GenericRowData row;
      GenericRowData lookupKey;
      lookupKey = GenericRowData.of(obj);

      List<GenericRowData> resultList = Lists.newArrayList();
      if (loadingCache != null) {
        GenericRowData cachedRow = null;
        try {
          cachedRow = loadingCache.get(lookupKey);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
        resultList.add(cachedRow);
        resultFuture.complete(resultList);
      }
    }
  }
}
