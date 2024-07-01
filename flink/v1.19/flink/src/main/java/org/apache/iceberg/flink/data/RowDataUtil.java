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
package org.apache.iceberg.flink.data;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;

public class RowDataUtil {

  private RowDataUtil() {}

  public static Object convertConstant(Type type, Object value) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case DECIMAL: // DecimalData
        Types.DecimalType decimal = (Types.DecimalType) type;
        return DecimalData.fromBigDecimal((BigDecimal) value, decimal.precision(), decimal.scale());
      case STRING: // StringData
        if (value instanceof Utf8) {
          Utf8 utf8 = (Utf8) value;
          return StringData.fromBytes(utf8.getBytes(), 0, utf8.getByteLength());
        }
        return StringData.fromString(value.toString());
      case FIXED: // byte[]
        if (value instanceof byte[]) {
          return value;
        } else if (value instanceof GenericData.Fixed) {
          return ((GenericData.Fixed) value).bytes();
        }
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case BINARY: // byte[]
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case TIME: // int mills instead of long
        return (int) ((Long) value / 1000);
      case TIMESTAMP: // TimestampData
        return TimestampData.fromLocalDateTime(DateTimeUtil.timestampFromMicros((Long) value));
      default:
    }
    return value;
  }

  /**
   * Similar to the private {@link RowDataSerializer#copyRowData(RowData, RowData)} method. This
   * skips the check the arity of rowType and from, because the from RowData may contains additional
   * column for position deletes. Using {@link RowDataSerializer#copy(RowData, RowData)} will fail
   * the arity check.
   */
  public static RowData clone(
      RowData from,
      RowData reuse,
      RowType rowType,
      TypeSerializer[] fieldSerializers,
      RowData.FieldGetter[] fieldGetters) {
    GenericRowData ret;
    if (reuse instanceof GenericRowData) {
      ret = (GenericRowData) reuse;
    } else {
      ret = new GenericRowData(from.getArity());
    }
    ret.setRowKind(from.getRowKind());
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      if (!from.isNullAt(i)) {
        ret.setField(i, fieldSerializers[i].copy(fieldGetters[i].getFieldOrNull(from)));
      } else {
        ret.setField(i, null);
      }
    }
    return ret;
  }
}
