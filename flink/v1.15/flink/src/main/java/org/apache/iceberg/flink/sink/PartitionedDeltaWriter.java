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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.FlinkAvroReader;
import org.apache.iceberg.flink.util.BloomFilterManager;
import org.apache.iceberg.flink.util.RowDataConverter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.hash.BloomFilter;
import org.apache.iceberg.relocated.com.google.common.hash.Funnels;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.BLOOM_FILTER_LIFESPAN;
import static org.apache.iceberg.TableProperties.FALSE_POSITIVE_PROBABILITY;
import static org.apache.iceberg.TableProperties.RECORDS_EACH_PARTITION;

class PartitionedDeltaWriter extends BaseDeltaTaskWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedDeltaWriter.class);
  private long recordsEachPartition = 0;
  private double falsePositiveProbability = 0;
  private int bloomFilterLifespan = 0;
  private Map<String, Integer> preLoad = Maps.newHashMap();
  private final PartitionKey partitionKey;
  private final Table table;
  private final long taskId;
  private final Schema schema;
  private final boolean upsert;
  private final String operationId;
  private final FileFormat format;
  private final Map<PartitionKey, RowDataDeltaWriter> writers = Maps.newHashMap();
  private static final Map<String, Map<PartitionKey, BloomFilterManager>> jobsBloomFilterManager =
          Maps.newConcurrentMap();
  private Map<PartitionKey, BloomFilterManager> bloomFilterManagers;

  private static final Map<String, Set<PartitionKey>> jobsPartitionHasProc = Maps.newConcurrentMap();

  private Set<PartitionKey> partitionHasProc;
  private final RowDataWrapper wrapper;
  private final StructProjection structProjection;

  PartitionedDeltaWriter(PartitionSpec spec,
                         FileFormat format,
                         FileAppenderFactory<RowData> appenderFactory,
                         OutputFileFactory fileFactory,
                         FileIO io,
                         long targetFileSize,
                         Schema schema,
                         RowType flinkSchema,
                         List<Integer> equalityFieldIds,
                         boolean upsert,
                         Map<String, Object> bloomFilterParam,
                         Table table) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, flinkSchema, equalityFieldIds,
        upsert);
    this.upsert = upsert;
    this.partitionKey = new PartitionKey(spec, schema);
    this.format = format;
    this.table = table;
    this.taskId = fileFactory.getPartitionId();
    this.schema = schema;
    this.operationId = fileFactory.getOperationId();
    this.structProjection = StructProjection.create(schema, TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds)));
    this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());

    if (null != bloomFilterParam) {
      this.recordsEachPartition = (Long) bloomFilterParam.get(RECORDS_EACH_PARTITION);
      this.falsePositiveProbability = (Double) bloomFilterParam.get(FALSE_POSITIVE_PROBABILITY);
      this.bloomFilterLifespan = (Integer) bloomFilterParam.get(BLOOM_FILTER_LIFESPAN);
      this.preLoad = (Map<String, Integer>) bloomFilterParam.get(FlinkSink.BLOOM_FILTER_PREVIOUS_LOAD);
    }
  }

  public void initBloomFilter() {

    if (!upsert || recordsEachPartition <= 0) {
      return;
    }

    bloomFilterManagers = jobsBloomFilterManager.get(operationId);
    partitionHasProc = jobsPartitionHasProc.get(operationId);

    if (bloomFilterManagers == null || partitionHasProc == null) {
      LOG.info("start to create bloom filter operationId = {}", operationId);

      /* Initialize the bloomFilter of this task */
      bloomFilterManagers = Maps.newConcurrentMap();
      jobsBloomFilterManager.put(operationId, bloomFilterManagers);

      /* Initialize the set which contain the partition which has processed of this task */
      partitionHasProc = Sets.newConcurrentHashSet();
      jobsPartitionHasProc.put(operationId, partitionHasProc);

      loadTableCreateBloomFilter();
      LOG.info("finished create bloom filter operationId = {}", operationId);
    }
  }

  void loadTableCreateBloomFilter() {
    if ((preLoad == null) || (preLoad.size() == 0)) {
      return;
    }

    Set<String> keysProcess = Sets.newHashSet();
    preLoad.forEach((key, value) -> {
      if ((value & 0xFFFF) == taskId) {
        keysProcess.add(key);
      }
    });

    if (keysProcess.isEmpty()) {
      return;
    }

    Map<String, String> partitionKeyMap = new LinkedHashMap<>();
    keysProcess.forEach(keyPath -> {
      String[] pairs = keyPath.split("/", keyPath.length());

      for (String key : pairs) {
        String [] values = key.split("=", keyPath.length());
        partitionKeyMap.put(values[0], values[1]);
      }

      boolean isNeedCreateBloomFilter = ((preLoad.get(keyPath) >> 16) == 1);
      createBloomFilter(partitionKeyMap, isNeedCreateBloomFilter);

      LOG.info("Task {} has finished create bloom for partition {} need create {}",
              taskId, keyPath, isNeedCreateBloomFilter);
    });
  }

  public String getKey(RowData rowData) {
    StructProjection rowKey = structProjection.wrap(wrapper.wrap(rowData));
    StringBuilder keyString = new StringBuilder();

    for (int i = 0; i < rowKey.size(); i++) {
      Object obj = rowKey.get(i, Object.class);
      keyString.append("&").append(obj.toString());
    }

    return keyString.toString();
  }

  void processParquetAndOrc(String filePath, BloomFilterManager bloomFilterManager) {
    CloseableIterable<Record> reader = null;
    long hasProcessed = 0L;
    long localTime = System.currentTimeMillis();

    if (bloomFilterManager == null) {
      return;
    }

    switch (format) {
      case PARQUET:
        reader = Parquet.read(table.io().newInputFile(filePath))
                .project(table.schema())
                .reuseContainers()
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                .build();
        break;
      case ORC:
        reader = ORC.read(table.io().newInputFile(filePath))
                .project(table.schema())
                .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(table.schema(), fileSchema)).build();
        break;
      default:
        break;
    }

    if (reader == null) {
      return;
    }

    for (Record record : reader) {
      RowData rowData = RowDataConverter.convert(schema, record);
      synchronized (structProjection) {
        String key = getKey(rowData);
        bloomFilterManager.setKeyToFilter(key);
      }

      hasProcessed++;
    }

    try {
      reader.close();
      LOG.info("finish proc {} records file {} use {} ms",
              hasProcessed, filePath, System.currentTimeMillis() - localTime);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close datafile reader", e);
    }
  }

  void processAvroFile(String filePath, BloomFilterManager bloomFilterManager) {
    CloseableIterable<RowData> reader = null;
    long hasProcessed = 0L;
    long localTime = System.currentTimeMillis();

    if (bloomFilterManager == null) {
      return;
    }

    if (format == FileFormat.AVRO) {
      reader = Avro.read(table.io().newInputFile(filePath))
              .project(table.schema())
              .reuseContainers()
              .createReaderFunc(fileSchema -> new FlinkAvroReader(schema, fileSchema)).build();
    }

    if (reader == null) {
      return;
    }

    for (RowData rowData : reader) {
      synchronized (structProjection) {
        String key = getKey(rowData);
        bloomFilterManager.setKeyToFilter(key);
        hasProcessed++;
      }
    }

    try {
      reader.close();
      LOG.info("finish proc {} records avro file {} use {} ms",
              hasProcessed, filePath, System.currentTimeMillis() - localTime);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close datafile reader", e);
    }
  }

  void generateBloomFilter(String filePath, BloomFilterManager bloomFilter) {
    switch (format) {
      case PARQUET:
      case ORC:
        processParquetAndOrc(filePath, bloomFilter);
        break;
      case AVRO:
        processAvroFile(filePath, bloomFilter);
        break;
      default:
        break;
    }
  }

  GenericRowData createRowDataWithKey(Map<String, String> partitionKeyMap) {
    GenericRowData rowData = new GenericRowData(table.schema().columns().size());

    for (Map.Entry<String, String> entry : partitionKeyMap.entrySet()) {
      int fieldId = table.schema().findField(entry.getKey()).fieldId();

      switch (table.schema().findField(fieldId).type().typeId()) {
        case TIMESTAMP:
          rowData.setField(fieldId - 1, TimestampData.fromTimestamp(Timestamp.valueOf(entry.getValue())));
          break;
        case STRING:
          rowData.setField(fieldId - 1, StringData.fromString(entry.getValue()));
          break;
        default:
          rowData.setField(fieldId - 1, entry.getValue());
          break;
      }
    }

    return rowData;
  }

  void createBloomFilter(Map<String, String> partitionKeyMap, boolean isCreateBloomFilter) {
    RowData rowData = createRowDataWithKey(partitionKeyMap);

    if (!isCreateBloomFilter) {
      partitionKey.partition(wrapper().wrap(rowData));
      partitionHasProc.add(partitionKey.copy());
      return;
    }

    BloomFilterManager bloomFilterManager = getBloomFilter(rowData);
    if (bloomFilterManager == null) {
      return;
    }

    TableScan tableScan = table.newScan()
            .useSnapshot(table.currentSnapshot().snapshotId())
            .caseSensitive(true);

    for (Map.Entry<String, String> entry : partitionKeyMap.entrySet()) {
      tableScan = tableScan.filter(Expressions.equal(entry.getKey(), entry.getValue()));
    }

    CloseableIterable<FileScanTask> fileScanTasks = tableScan
            .ignoreResiduals()
            .planFiles();

    ExecutorService executorService = Executors.newFixedThreadPool(10);

    for (FileScanTask task : fileScanTasks) {
      String filePath = task.file().path().toString();
      Future<?> procBloom = executorService.submit(() -> generateBloomFilter(filePath, bloomFilterManager));
      LOG.warn("thread to process file path = {} procBloom = {}", filePath, procBloom);
    }

    executorService.shutdown();

    try {
      boolean result = executorService.awaitTermination(60, TimeUnit.MINUTES);
      LOG.info("wait thread pool to finish task id = {}, result = {}", taskId, result);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    try {
      fileScanTasks.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close file scan tasks", e);
    }
  }

  void checkBloomFilterExpiration() {
    Iterator<Map.Entry<PartitionKey, BloomFilterManager>> iter = bloomFilterManagers.entrySet().iterator();
    SortedSet<String> validBloomFilterSet = new TreeSet<>();
    while (iter.hasNext()) {
      Map.Entry<PartitionKey, BloomFilterManager> entry = iter.next();
      BloomFilterManager bfm = entry.getValue();
      if (System.currentTimeMillis() - bfm.getBloomFilterCreateTime() > 86400000L * bloomFilterLifespan) {
        iter.remove();
        LOG.info("The key {}'s bloom filter has time out and free.", entry.getKey());
      } else {
        validBloomFilterSet.add(entry.getKey().toString());
      }
    }

    /*
     * Used to supplement the timeout algorithm defect.The Hash algorithm has data skew,
     * which results in the data in the last few days being distributed unevenly among several writers.
     * The variance is less than or equal to 3. Therefore, based on the retention period of BloomFilters,
     * you can roughly calculate how many BloomFilters are retained by each writer.This ensures that the bloomFilter
     * we expect to keep will remain and will not be aged out.
     */
    int shouldSave = (int) (bloomFilterLifespan * 0.2 + 1);
    while (validBloomFilterSet.size() > shouldSave) {
      String shouldFreeValue = validBloomFilterSet.first();

      iter = bloomFilterManagers.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<PartitionKey, BloomFilterManager> entry = iter.next();
        if (entry.getKey().toString().equals(shouldFreeValue)) {
          iter.remove();
          validBloomFilterSet.remove(shouldFreeValue);
          LOG.info("The key {}'s bloom filter has number = {} out and free.", shouldSave, entry.getKey());
          break;
        }
      }
    }
  }

  @Override
  BloomFilterManager getBloomFilter(RowData row) {
    if (!upsert || (recordsEachPartition <= 0)) {
      return null;
    }

    partitionKey.partition(wrapper().wrap(row));

    // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
    PartitionKey copiedKey = partitionKey.copy();

    BloomFilterManager bloomFilterManager = bloomFilterManagers.get(copiedKey);
    if (bloomFilterManager == null) {

      /* this partition had time out then return */
      if (partitionHasProc.contains(copiedKey)) {
        return null;
      }

      /* new partition add the partition key to sets */
      partitionHasProc.add(copiedKey);

      BloomFilter<CharSequence> filter = BloomFilter.create(Funnels.unencodedCharsFunnel(),
              recordsEachPartition, falsePositiveProbability);

      bloomFilterManager = new BloomFilterManager(filter, System.currentTimeMillis());
      bloomFilterManagers.put(copiedKey, bloomFilterManager);

      /* check every bloom filter manager is timeout */
      checkBloomFilterExpiration();
    }

    return bloomFilterManager;
  }

  public void freeResource() {
    if (jobsBloomFilterManager.get(operationId) != null) {
      bloomFilterManagers = null;
      jobsBloomFilterManager.remove(operationId);
    }

    if (jobsPartitionHasProc.get(operationId) != null) {
      partitionHasProc = null;
      jobsPartitionHasProc.remove(operationId);
    }
  }

  @Override
  RowDataDeltaWriter route(RowData row) {
    partitionKey.partition(wrapper().wrap(row));

    RowDataDeltaWriter writer = writers.get(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RowDataDeltaWriter(copiedKey);
      writers.put(copiedKey, writer);
    }

    return writer;
  }

  @Override
  public void close() {
    try {
      Tasks.foreach(writers.values())
          .throwFailureWhenFinished()
          .noRetry()
          .run(RowDataDeltaWriter::close, IOException.class);

      writers.clear();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close equality delta writer", e);
    }
  }
}
