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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import org.apache.flink.types.Row;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

class RowWrapper implements StructLike {

  private final Type[] types;
  private final PositionalGetter[] getters;
  private Row row = null;

  RowWrapper(Types.StructType type) {
    int size = type.fields().size();

    types = (Type[]) Array.newInstance(Type.class, size);
    for (int i = 0; i < size; i++) {
      types[i] = type.fields().get(i).type();
    }

    getters = (PositionalGetter[]) Array.newInstance(PositionalGetter.class, size);
    for (int i = 0; i < size; i++) {
      getters[i] = buildGetter(types[i]);
    }
  }

  RowWrapper wrap(Row data) {
    this.row = data;
    return this;
  }

  @Override
  public int size() {
    return types.length;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (row.getField(pos) == null) {
      return null;
    } else if (getters[pos] != null) {
      return javaClass.cast(getters[pos].get(row, pos));
    }

    return javaClass.cast(row.getField(pos));
  }

  @Override
  public <T> void set(int pos, T value) {
    row.setField(pos, value);
  }

  private interface PositionalGetter<T> {
    T get(Row row, int pos);
  }

  private static PositionalGetter buildGetter(Type type) {
    switch (type.typeId()) {
      case DATE:
        return (r, pos) -> DateTimeUtil.daysFromDate((LocalDate) r.getField(pos));
      case TIME:
        return (r, pos) -> DateTimeUtil.microsFromTime((LocalTime) r.getField(pos));
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return (r, pos) -> DateTimeUtil.microsFromTimestamptz((OffsetDateTime) r.getField(pos));
        } else {
          return (r, pos) -> DateTimeUtil.microsFromTimestamp((LocalDateTime) r.getField(pos));
        }
      case FIXED:
        return (r, pos) -> ByteBuffer.wrap((byte[]) r.getField(pos));
      case STRUCT:
        RowWrapper nestedWrapper = new RowWrapper((Types.StructType) type);
        return (r, pos) -> nestedWrapper.wrap((Row) r.getField(pos));
      default:
        return null;
    }
  }
}
