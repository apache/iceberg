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
package org.apache.iceberg.data;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;

/** Utility methods for working with Iceberg's generic data model */
public class GenericDataUtil {
  private GenericDataUtil() {}

  /**
   * Convert a value from Iceberg's internal data model to the generic data model.
   *
   * @param type a data type
   * @param value value to convert
   * @return the value in the generic data model representation
   */
  public static Object internalToGeneric(Type type, Object value) {
    if (null == value) {
      return null;
    }

    switch (type.typeId()) {
      case DATE:
        return DateTimeUtil.dateFromDays((Integer) value);
      case TIME:
        return DateTimeUtil.timeFromMicros((Long) value);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return DateTimeUtil.timestamptzFromMicros((Long) value);
        } else {
          return DateTimeUtil.timestampFromMicros((Long) value);
        }
      case FIXED:
        return ByteBuffers.toByteArray((ByteBuffer) value);
    }
    return value;
  }

  /**
   * Convert a value from Iceberg's generic data model to the internal data model.
   *
   * @param type a data type
   * @param value value to convert
   * @return the value in the internal data model representation
   */
  public static Object genericToInternal(Type type, Object value) {
    if (null == value) {
      return null;
    }

    switch (type.typeId()) {
      case DATE:
        return DateTimeUtil.daysFromDate((LocalDate) value);
      case TIME:
        return DateTimeUtil.microsFromTime((LocalTime) value);
      case TIMESTAMP:
        return DateTimeUtil.microsFromTimestamp((LocalDateTime) value);
      case FIXED:
        return ByteBuffer.wrap((byte[]) value);
    }
    return value;
  }
}
