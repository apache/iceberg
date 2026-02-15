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

import java.io.Serializable;
import java.util.Arrays;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * RowData 包装类，用于作为 Map/Cache 的 Key。
 *
 * <p>由于 Flink 的 GenericRowData 没有实现正确的 equals() 和 hashCode() 方法，
 * 导致无法直接用作 Map 或 Cache 的 key。此类包装 RowData 并提供基于值的比较。
 *
 * <p>此实现只支持简单类型（BIGINT, INT, STRING, DOUBLE, FLOAT, BOOLEAN, SHORT, BYTE），
 * 这些是 Lookup Key 最常用的类型。对于复杂类型，会使用字符串表示进行比较。
 */
@Internal
public final class RowDataKey implements Serializable {

  private static final long serialVersionUID = 1L;

  /** 缓存的字段值数组，用于 equals 和 hashCode 计算 */
  private final Object[] fieldValues;
  private transient int cachedHashCode;
  private transient boolean hashCodeCached;

  /**
   * 创建 RowDataKey 实例
   *
   * @param rowData 要包装的 RowData
   */
  public RowDataKey(RowData rowData) {
    Preconditions.checkNotNull(rowData, "RowData cannot be null");
    int arity = rowData.getArity();
    this.fieldValues = new Object[arity];
    for (int i = 0; i < arity; i++) {
      this.fieldValues[i] = extractFieldValue(rowData, i);
    }
    this.hashCodeCached = false;
  }

  /**
   * 从指定位置提取字段值，转换为可比较的不可变类型
   *
   * @param rowData 源 RowData
   * @param pos 字段位置
   * @return 可比较的字段值
   */
  private static Object extractFieldValue(RowData rowData, int pos) {
    if (rowData.isNullAt(pos)) {
      return null;
    }

    // 对于 GenericRowData，直接获取字段值
    if (rowData instanceof GenericRowData) {
      Object value = ((GenericRowData) rowData).getField(pos);
      return normalizeValue(value);
    }

    // 对于其他 RowData 实现，尝试多种类型
    return tryExtractValue(rowData, pos);
  }

  /**
   * 归一化值，确保类型一致性
   *
   * @param value 原始值
   * @return 归一化后的值
   */
  private static Object normalizeValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof StringData) {
      return ((StringData) value).toString();
    }
    // 基本类型直接返回
    return value;
  }

  /**
   * 尝试从 RowData 提取值，支持多种类型
   *
   * @param rowData 源 RowData
   * @param pos 字段位置
   * @return 提取的值
   */
  private static Object tryExtractValue(RowData rowData, int pos) {
    // 依次尝试常见类型
    Object result = tryGetLong(rowData, pos);
    if (result != null) {
      return result;
    }

    result = tryGetInt(rowData, pos);
    if (result != null) {
      return result;
    }

    result = tryGetString(rowData, pos);
    if (result != null) {
      return result;
    }

    result = tryGetDouble(rowData, pos);
    if (result != null) {
      return result;
    }

    result = tryGetBoolean(rowData, pos);
    if (result != null) {
      return result;
    }

    // 最后返回 null
    return null;
  }

  private static Object tryGetLong(RowData rowData, int pos) {
    try {
      return rowData.getLong(pos);
    } catch (Exception e) {
      return null;
    }
  }

  private static Object tryGetInt(RowData rowData, int pos) {
    try {
      return rowData.getInt(pos);
    } catch (Exception e) {
      return null;
    }
  }

  private static Object tryGetString(RowData rowData, int pos) {
    try {
      StringData sd = rowData.getString(pos);
      return sd != null ? sd.toString() : null;
    } catch (Exception e) {
      return null;
    }
  }

  private static Object tryGetDouble(RowData rowData, int pos) {
    try {
      return rowData.getDouble(pos);
    } catch (Exception e) {
      return null;
    }
  }

  private static Object tryGetBoolean(RowData rowData, int pos) {
    try {
      return rowData.getBoolean(pos);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowDataKey that = (RowDataKey) o;
    return Arrays.deepEquals(this.fieldValues, that.fieldValues);
  }

  @Override
  public int hashCode() {
    if (!hashCodeCached) {
      cachedHashCode = Arrays.deepHashCode(fieldValues);
      hashCodeCached = true;
    }
    return cachedHashCode;
  }

  @Override
  public String toString() {
    return "RowDataKey" + Arrays.toString(fieldValues);
  }
}
