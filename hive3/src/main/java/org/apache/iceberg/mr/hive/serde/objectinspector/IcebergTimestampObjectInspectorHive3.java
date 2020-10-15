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

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public abstract class IcebergTimestampObjectInspectorHive3 extends AbstractPrimitiveJavaObjectInspector
    implements TimestampObjectInspector, IcebergReadObjectInspector {

  private static final IcebergTimestampObjectInspectorHive3 INSTANCE_WITH_ZONE =
      new IcebergTimestampObjectInspectorHive3() {
        @Override
        Instant toInstant(Object o) {
          return ((OffsetDateTime) o).toInstant();
        }

        @Override
        public Object getIcebergObject(Object o) {
          return o == null ? null :
              OffsetDateTime.ofInstant(writableToInstant((TimestampWritableV2) o), ZoneOffset.UTC);
        }
      };

  private static final IcebergTimestampObjectInspectorHive3 INSTANCE_WITHOUT_ZONE =
      new IcebergTimestampObjectInspectorHive3() {
        @Override
        Instant toInstant(Object o) {
          return ((LocalDateTime) o).toInstant(ZoneOffset.UTC);
        }

        @Override
        public Object getIcebergObject(Object o) {
          return o == null ? null :
              LocalDateTime.ofInstant(writableToInstant((TimestampWritableV2) o), ZoneOffset.UTC);
        }
      };

  public static IcebergTimestampObjectInspectorHive3 get(boolean adjustToUTC) {
    return adjustToUTC ? INSTANCE_WITH_ZONE : INSTANCE_WITHOUT_ZONE;
  }

  private IcebergTimestampObjectInspectorHive3() {
    super(TypeInfoFactory.timestampTypeInfo);
  }


  abstract Instant toInstant(Object object);

  @Override
  public Timestamp getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }

    Instant instant = toInstant(o);
    return Timestamp.ofEpochSecond(instant.getEpochSecond(), instant.getNano());
  }

  @Override
  public TimestampWritableV2 getPrimitiveWritableObject(Object o) {
    Timestamp ts = getPrimitiveJavaObject(o);
    return ts == null ? null : new TimestampWritableV2(ts);
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof Timestamp) {
      Timestamp ts = (Timestamp) o;
      Timestamp copy = new Timestamp(ts);
      copy.setNanos(ts.getNanos());
      return copy;
    } else if (o instanceof OffsetDateTime) {
      return OffsetDateTime.of(((OffsetDateTime) o).toLocalDateTime(), ((OffsetDateTime) o).getOffset());
    } else if (o instanceof LocalDateTime) {
      return LocalDateTime.of(((LocalDateTime) o).toLocalDate(), ((LocalDateTime) o).toLocalTime());
    } else {
      return o;
    }
  }

  private static Instant writableToInstant(TimestampWritableV2 writable) {
    Timestamp timestamp = writable.getTimestamp();
    return Instant.ofEpochSecond(timestamp.toEpochSecond(), timestamp.getNanos());
  }
}
