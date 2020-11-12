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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class IcebergTimestampLocalTZObjectInspectorHive3 extends AbstractPrimitiveJavaObjectInspector
    implements TimestampLocalTZObjectInspector {
  IcebergTimestampLocalTZObjectInspectorHive3() {
    super(TypeInfoFactory.timestampLocalTZTypeInfo);
  }

  private Instant toInstant(Object object) {
    return ((OffsetDateTime) object).toInstant();
  }

  @Override
  public TimestampTZ getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }

    Instant instant = toInstant(o);
    return new TimestampTZ(ZonedDateTime.ofInstant(instant, ZoneOffset.UTC));
  }

  @Override
  public TimestampLocalTZWritable getPrimitiveWritableObject(Object o) {
    TimestampTZ ts = getPrimitiveJavaObject(o);
    return ts == null ? null : new TimestampLocalTZWritable(ts);
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof TimestampTZ) {
      TimestampTZ ts = (TimestampTZ) o;
      TimestampTZ copy = new TimestampTZ(ts.getZonedDateTime());
      return copy;
    } else if (o instanceof OffsetDateTime) {
      return OffsetDateTime.of(((OffsetDateTime) o).toLocalDateTime(), ((OffsetDateTime) o).getOffset());
    } else {
      return o;
    }
  }
}
