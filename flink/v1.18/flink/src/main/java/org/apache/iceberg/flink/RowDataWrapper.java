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

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;

public class RowDataWrapper implements StructLike {

  private final LogicalType[] types;
  private final PositionalGetter<?>[] getters;
  private RowData rowData = null;

  public RowDataWrapper(RowType rowType, Types.StructType struct) {
    int size = rowType.getFieldCount();

    types = (LogicalType[]) Array.newInstance(LogicalType.class, size);
    getters = (PositionalGetter[]) Array.newInstance(PositionalGetter.class, size);

    for (int i = 0; i < size; i++) {
      types[i] = rowType.getTypeAt(i);
      getters[i] = buildGetter(types[i], struct.fields().get(i).type());
    }
  }

  public RowDataWrapper wrap(RowData data) {
    this.rowData = data;
    return this;
  }

  @Override
  public int size() {
    return types.length;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (rowData.isNullAt(pos)) {
      return null;
    } else if (getters[pos] != null) {
      return javaClass.cast(getters[pos].get(rowData, pos));
    }

    Object value = RowData.createFieldGetter(types[pos], pos).getFieldOrNull(rowData);
    return javaClass.cast(value);
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException(
        "Could not set a field in the RowDataWrapper because rowData is read-only");
  }

  private interface PositionalGetter<T> {
    T get(RowData data, int pos);
  }

  private static PositionalGetter<?> buildGetter(LogicalType logicalType, Type type) {
    switch (logicalType.getTypeRoot()) {
      case TINYINT:
        return (row, pos) -> (int) row.getByte(pos);
      case SMALLINT:
        return (row, pos) -> (int) row.getShort(pos);
      case CHAR:
      case VARCHAR:
        return (row, pos) -> row.getString(pos).toString();

      case BINARY:
      case VARBINARY:
        if (Type.TypeID.UUID == type.typeId()) {
          return (row, pos) -> UUIDUtil.convert(row.getBinary(pos));
        } else {
          return (row, pos) -> ByteBuffer.wrap(row.getBinary(pos));
        }

      case DECIMAL:
        DecimalType decimalType = (DecimalType) logicalType;
        return (row, pos) ->
            row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale()).toBigDecimal();

      case TIME_WITHOUT_TIME_ZONE:
        // Time in RowData is in milliseconds (Integer), while iceberg's time is microseconds
        // (Long).
        return (row, pos) -> ((long) row.getInt(pos)) * 1_000;

      case TIMESTAMP_WITHOUT_TIME_ZONE:
        TimestampType timestampType = (TimestampType) logicalType;
        return (row, pos) -> {
          LocalDateTime localDateTime =
              row.getTimestamp(pos, timestampType.getPrecision()).toLocalDateTime();
          return DateTimeUtil.microsFromTimestamp(localDateTime);
        };

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        LocalZonedTimestampType lzTs = (LocalZonedTimestampType) logicalType;
        return (row, pos) -> {
          TimestampData timestampData = row.getTimestamp(pos, lzTs.getPrecision());
          return timestampData.getMillisecond() * 1000
              + timestampData.getNanoOfMillisecond() / 1000;
        };

      case ROW:
        RowType rowType = (RowType) logicalType;
        Types.StructType structType = (Types.StructType) type;

        RowDataWrapper nestedWrapper = new RowDataWrapper(rowType, structType);
        return (row, pos) -> nestedWrapper.wrap(row.getRow(pos, rowType.getFieldCount()));

      default:
        return null;
    }
  }
}
