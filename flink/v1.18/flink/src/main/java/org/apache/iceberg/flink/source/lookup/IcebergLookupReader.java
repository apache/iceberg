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

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg Lookup 数据读取器，封装从 Iceberg 表读取数据的逻辑。
 *
 * <p>支持两种读取模式：
 *
 * <ul>
 *   <li>全量读取：用于 ALL 模式，读取整个表的数据
 *   <li>按键查询：用于 PARTIAL 模式，根据 Lookup 键过滤数据
 * </ul>
 *
 * <p>特性：
 *
 * <ul>
 *   <li>支持投影下推：仅读取 SQL 中选择的列
 *   <li>支持谓词下推：将 Lookup 键条件下推到文件扫描层
 *   <li>支持分区裁剪：利用分区信息减少扫描的文件数量
 * </ul>
 */
@Internal
public class IcebergLookupReader implements Closeable, Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergLookupReader.class);

  private final TableLoader tableLoader;
  private final Schema projectedSchema;
  private final int[] lookupKeyIndices;
  private final String[] lookupKeyNames;
  private final boolean caseSensitive;

  private transient Table table;
  private transient FileIO io;
  private transient EncryptionManager encryption;
  private transient boolean initialized;

  /**
   * 创建 IcebergLookupReader 实例
   *
   * @param tableLoader 表加载器
   * @param projectedSchema 投影后的 Schema（仅包含需要的列）
   * @param lookupKeyIndices Lookup 键在投影 Schema 中的索引
   * @param lookupKeyNames Lookup 键的字段名称
   * @param caseSensitive 是否区分大小写
   */
  public IcebergLookupReader(
      TableLoader tableLoader,
      Schema projectedSchema,
      int[] lookupKeyIndices,
      String[] lookupKeyNames,
      boolean caseSensitive) {
    this.tableLoader = Preconditions.checkNotNull(tableLoader, "TableLoader cannot be null");
    this.projectedSchema =
        Preconditions.checkNotNull(projectedSchema, "ProjectedSchema cannot be null");
    this.lookupKeyIndices =
        Preconditions.checkNotNull(lookupKeyIndices, "LookupKeyIndices cannot be null");
    this.lookupKeyNames =
        Preconditions.checkNotNull(lookupKeyNames, "LookupKeyNames cannot be null");
    this.caseSensitive = caseSensitive;
    this.initialized = false;
  }

  /** 初始化读取器，必须在使用前调用 */
  public void open() {
    if (!initialized) {
      if (!tableLoader.isOpen()) {
        tableLoader.open();
      }
      this.table = tableLoader.loadTable();
      this.io = table.io();
      this.encryption = table.encryption();
      this.initialized = true;
      LOG.info(
          "Initialized IcebergLookupReader for table: {}, projected columns: {}",
          table.name(),
          projectedSchema.columns().size());
    }
  }

  /** 关闭读取器，释放资源 */
  @Override
  public void close() throws IOException {
    if (tableLoader != null) {
      tableLoader.close();
    }
    initialized = false;
    LOG.info("Closed IcebergLookupReader");
  }

  /** 刷新表元数据，获取最新快照 */
  public void refresh() {
    if (table != null) {
      // 先刷新现有表对象
      table.refresh();
      LOG.info(
          "Refreshed table metadata, current snapshot: {}",
          table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : "none");
    }
  }

  /** 重新加载表，确保获取最新元数据（用于定时刷新场景） */
  public void reloadTable() {
    LOG.info("Reloading table to get latest metadata...");

    // 重新从 TableLoader 加载表，确保获取最新的元数据
    this.table = tableLoader.loadTable();
    this.io = table.io();
    this.encryption = table.encryption();

    LOG.info(
        "Table reloaded, current snapshot: {}",
        table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : "none");
  }

  /**
   * 全量读取表数据，用于 ALL 模式
   *
   * @return 所有数据的缓存条目集合
   * @throws IOException 如果读取失败
   */
  public Collection<IcebergLookupCache.CacheEntry> readAll() throws IOException {
    Preconditions.checkState(initialized, "Reader not initialized, call open() first");

    LOG.info("Starting full table scan for ALL mode");

    // 重新加载表以获取最新快照（而不仅仅是 refresh）
    // 这对于 Hadoop catalog 和其他场景非常重要
    reloadTable();

    LOG.info(
        "Table schema: {}, projected schema columns: {}",
        table.schema().columns().size(),
        projectedSchema.columns().size());

    // 构建表扫描
    TableScan scan = table.newScan().caseSensitive(caseSensitive).project(projectedSchema);

    // 按 Lookup 键分组
    Map<RowData, List<RowData>> resultMap = Maps.newHashMap();
    long rowCount = 0;

    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      for (CombinedScanTask combinedTask : tasksIterable) {
        InputFilesDecryptor decryptor = new InputFilesDecryptor(combinedTask, io, encryption);
        for (FileScanTask task : combinedTask.files()) {
          rowCount += readFileScanTask(task, resultMap, null, decryptor);
        }
      }
    }

    LOG.info(
        "Full table scan completed, read {} rows, grouped into {} keys",
        rowCount,
        resultMap.size());

    // 转换为 CacheEntry 集合
    List<IcebergLookupCache.CacheEntry> entries = Lists.newArrayList();
    for (Map.Entry<RowData, List<RowData>> entry : resultMap.entrySet()) {
      entries.add(new IcebergLookupCache.CacheEntry(entry.getKey(), entry.getValue()));
    }

    return entries;
  }

  /**
   * 按键查询数据，用于 PARTIAL 模式
   *
   * @param lookupKey Lookup 键值
   * @return 匹配的数据列表
   * @throws IOException 如果读取失败
   */
  public List<RowData> lookup(RowData lookupKey) throws IOException {
    Preconditions.checkState(initialized, "Reader not initialized, call open() first");
    Preconditions.checkNotNull(lookupKey, "Lookup key cannot be null");

    LOG.debug("Lookup for key: {}", lookupKey);

    // 构建过滤表达式
    Expression filter = buildLookupFilter(lookupKey);

    // 构建表扫描
    TableScan scan =
        table.newScan().caseSensitive(caseSensitive).project(projectedSchema).filter(filter);

    List<RowData> results = Lists.newArrayList();

    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      for (CombinedScanTask combinedTask : tasksIterable) {
        InputFilesDecryptor decryptor = new InputFilesDecryptor(combinedTask, io, encryption);
        for (FileScanTask task : combinedTask.files()) {
          readFileScanTaskToList(task, results, lookupKey, decryptor);
        }
      }
    }

    LOG.debug("Lookup completed for key: {}, found {} rows", lookupKey, results.size());
    return results;
  }

  /**
   * 构建 Lookup 过滤表达式
   *
   * @param lookupKey Lookup 键值
   * @return Iceberg 过滤表达式
   */
  private Expression buildLookupFilter(RowData lookupKey) {
    Expression filter = Expressions.alwaysTrue();

    for (int i = 0; i < lookupKeyNames.length; i++) {
      String fieldName = lookupKeyNames[i];
      Object value = getFieldValue(lookupKey, i);

      if (value == null) {
        filter = Expressions.and(filter, Expressions.isNull(fieldName));
      } else {
        filter = Expressions.and(filter, Expressions.equal(fieldName, value));
      }
    }

    return filter;
  }

  /**
   * 从 RowData 中获取指定位置的字段值
   *
   * @param rowData RowData 对象
   * @param index 字段索引
   * @return 字段值
   */
  private Object getFieldValue(RowData rowData, int index) {
    if (rowData.isNullAt(index)) {
      return null;
    }

    // 获取对应字段的类型
    Types.NestedField field = projectedSchema.columns().get(lookupKeyIndices[index]);

    switch (field.type().typeId()) {
      case BOOLEAN:
        return rowData.getBoolean(index);
      case INTEGER:
        return rowData.getInt(index);
      case LONG:
        return rowData.getLong(index);
      case FLOAT:
        return rowData.getFloat(index);
      case DOUBLE:
        return rowData.getDouble(index);
      case STRING:
        return rowData.getString(index).toString();
      case DATE:
        return rowData.getInt(index);
      case TIMESTAMP:
        return rowData.getTimestamp(index, 6).getMillisecond();
      default:
        // 对于其他类型，尝试获取通用值
        LOG.warn("Unsupported type for lookup key: {}", field.type());
        return null;
    }
  }

  /**
   * 读取 FileScanTask 并将结果按键分组到 Map 中
   *
   * @param task FileScanTask
   * @param resultMap 结果 Map
   * @param lookupKey 可选的 Lookup 键用于过滤
   * @return 读取的行数
   */
  private long readFileScanTask(
      FileScanTask task,
      Map<RowData, List<RowData>> resultMap,
      RowData lookupKey,
      InputFilesDecryptor decryptor)
      throws IOException {
    long rowCount = 0;

    RowDataFileScanTaskReader reader =
        new RowDataFileScanTaskReader(
            table.schema(),
            projectedSchema,
            table.properties().get("name-mapping"),
            caseSensitive,
            null);

    try (CloseableIterator<RowData> iterator = reader.open(task, decryptor)) {
      while (iterator.hasNext()) {
        RowData row = iterator.next();

        // 如果指定了 lookupKey，验证是否匹配
        if (lookupKey != null && !matchesLookupKey(row, lookupKey)) {
          continue;
        }

        // 复制 RowData 以避免重用问题
        RowData copiedRow = copyRowData(row);

        // 提取 Lookup 键
        RowData key = extractLookupKey(copiedRow);

        // 分组存储
        resultMap.computeIfAbsent(key, k -> Lists.newArrayList()).add(copiedRow);
        rowCount++;

        // 添加调试日志
        if (LOG.isDebugEnabled() && rowCount <= 5) {
          LOG.debug(
              "Read row {}: key={}, keyFields={}",
              rowCount,
              key,
              describeRowData(key));
        }
      }
    }

    return rowCount;
  }

  /**
   * 读取 FileScanTask 并将结果添加到列表中
   *
   * @param task FileScanTask
   * @param results 结果列表
   * @param lookupKey Lookup 键用于过滤
   */
  private void readFileScanTaskToList(
      FileScanTask task, List<RowData> results, RowData lookupKey, InputFilesDecryptor decryptor)
      throws IOException {
    RowDataFileScanTaskReader reader =
        new RowDataFileScanTaskReader(
            table.schema(),
            projectedSchema,
            table.properties().get("name-mapping"),
            caseSensitive,
            null);

    try (CloseableIterator<RowData> iterator = reader.open(task, decryptor)) {
      while (iterator.hasNext()) {
        RowData row = iterator.next();

        // 验证是否匹配 lookupKey
        if (matchesLookupKey(row, lookupKey)) {
          // 复制 RowData 以避免重用问题
          results.add(copyRowData(row));
        }
      }
    }
  }

  /**
   * 检查 RowData 是否匹配 Lookup 键
   *
   * @param row RowData
   * @param lookupKey Lookup 键
   * @return 是否匹配
   */
  private boolean matchesLookupKey(RowData row, RowData lookupKey) {
    for (int i = 0; i < lookupKeyIndices.length; i++) {
      int fieldIndex = lookupKeyIndices[i];

      boolean rowIsNull = row.isNullAt(fieldIndex);
      boolean keyIsNull = lookupKey.isNullAt(i);

      if (rowIsNull && keyIsNull) {
        continue;
      }
      if (rowIsNull || keyIsNull) {
        return false;
      }

      // 获取字段类型并比较值
      Types.NestedField field = projectedSchema.columns().get(fieldIndex);
      if (!fieldsEqual(row, fieldIndex, lookupKey, i, field.type())) {
        return false;
      }
    }
    return true;
  }

  /** 比较两个字段是否相等 */
  private boolean fieldsEqual(
      RowData row1, int index1, RowData row2, int index2, org.apache.iceberg.types.Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return row1.getBoolean(index1) == row2.getBoolean(index2);
      case INTEGER:
      case DATE:
        return row1.getInt(index1) == row2.getInt(index2);
      case LONG:
        return row1.getLong(index1) == row2.getLong(index2);
      case FLOAT:
        return Float.compare(row1.getFloat(index1), row2.getFloat(index2)) == 0;
      case DOUBLE:
        return Double.compare(row1.getDouble(index1), row2.getDouble(index2)) == 0;
      case STRING:
        return row1.getString(index1).equals(row2.getString(index2));
      case TIMESTAMP:
        return row1.getTimestamp(index1, 6).equals(row2.getTimestamp(index2, 6));
      default:
        LOG.warn("Unsupported type for comparison: {}", type);
        return false;
    }
  }

  /**
   * 从 RowData 中提取 Lookup 键
   *
   * @param row RowData
   * @return Lookup 键 RowData
   */
  private RowData extractLookupKey(RowData row) {
    GenericRowData key = new GenericRowData(lookupKeyIndices.length);
    for (int i = 0; i < lookupKeyIndices.length; i++) {
      int fieldIndex = lookupKeyIndices[i];
      Types.NestedField field = projectedSchema.columns().get(fieldIndex);
      key.setField(i, getFieldValueByType(row, fieldIndex, field.type()));
    }
    return key;
  }

  /** 根据类型获取字段值 */
  private Object getFieldValueByType(RowData row, int index, org.apache.iceberg.types.Type type) {
    if (row.isNullAt(index)) {
      return null;
    }

    switch (type.typeId()) {
      case BOOLEAN:
        return row.getBoolean(index);
      case INTEGER:
      case DATE:
        return row.getInt(index);
      case LONG:
        return row.getLong(index);
      case FLOAT:
        return row.getFloat(index);
      case DOUBLE:
        return row.getDouble(index);
      case STRING:
        return row.getString(index);
      case TIMESTAMP:
        return row.getTimestamp(index, 6);
      case BINARY:
        return row.getBinary(index);
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) type;
        return row.getDecimal(index, decimalType.precision(), decimalType.scale());
      default:
        LOG.warn("Unsupported type for extraction: {}", type);
        return null;
    }
  }

  /**
   * 复制 RowData 以避免重用问题
   *
   * @param source 源 RowData
   * @return 复制的 RowData
   */
  private RowData copyRowData(RowData source) {
    int arity = projectedSchema.columns().size();
    GenericRowData copy = new GenericRowData(arity);
    copy.setRowKind(source.getRowKind());

    for (int i = 0; i < arity; i++) {
      Types.NestedField field = projectedSchema.columns().get(i);
      copy.setField(i, getFieldValueByType(source, i, field.type()));
    }

    return copy;
  }

  /**
   * 获取表对象
   *
   * @return Iceberg 表
   */
  public Table getTable() {
    return table;
  }

  /**
   * 获取投影后的 Schema
   *
   * @return 投影 Schema
   */
  public Schema getProjectedSchema() {
    return projectedSchema;
  }

  /**
   * 获取 Lookup 键字段名称
   *
   * @return Lookup 键名称数组
   */
  public String[] getLookupKeyNames() {
    return lookupKeyNames;
  }

  /**
   * 描述 RowData 的内容，用于调试
   *
   * @param row RowData
   * @return 描述字符串
   */
  private String describeRowData(RowData row) {
    if (row == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder("[");
    int arity = row.getArity();
    for (int i = 0; i < arity; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      if (row instanceof GenericRowData) {
        Object value = ((GenericRowData) row).getField(i);
        if (value == null) {
          sb.append("null");
        } else {
          sb.append(value.getClass().getSimpleName()).append(":").append(value);
        }
      } else {
        sb.append("?");
      }
    }
    sb.append("]");
    return sb.toString();
  }
}
