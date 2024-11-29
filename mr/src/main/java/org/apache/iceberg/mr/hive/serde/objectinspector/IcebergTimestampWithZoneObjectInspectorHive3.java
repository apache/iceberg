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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class IcebergTimestampWithZoneObjectInspectorHive3
    extends AbstractPrimitiveJavaObjectInspector
    implements TimestampLocalTZObjectInspector, WriteObjectInspector {

  private static final IcebergTimestampWithZoneObjectInspectorHive3 INSTANCE =
      new IcebergTimestampWithZoneObjectInspectorHive3();

  public static IcebergTimestampWithZoneObjectInspectorHive3 get() {
    return INSTANCE;
  }

  private IcebergTimestampWithZoneObjectInspectorHive3() {
    super(TypeInfoFactory.timestampLocalTZTypeInfo);
  }

  @Override
  public OffsetDateTime convert(Object o) {
    if (o == null) {
      return null;
    }
    ZonedDateTime zdt = ((TimestampTZ) o).getZonedDateTime();
    return OffsetDateTime.of(zdt.toLocalDateTime(), zdt.getOffset());
  }

  @Override
  public TimestampTZ getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }
    OffsetDateTime odt = (OffsetDateTime) o;
    ZonedDateTime zdt = odt.atZoneSameInstant(ZoneOffset.UTC);
    return new TimestampTZ(zdt);
  }

  @Override
  public TimestampLocalTZWritable getPrimitiveWritableObject(Object o) {
    TimestampTZ tsTz = getPrimitiveJavaObject(o);
    return tsTz == null ? null : new TimestampLocalTZWritable(tsTz);
  }

  @Override
  public Object copyObject(Object o) {
    if (o instanceof TimestampTZ) {
      TimestampTZ ts = (TimestampTZ) o;
      return new TimestampTZ(ts.getZonedDateTime());
    } else if (o instanceof OffsetDateTime) {
      OffsetDateTime odt = (OffsetDateTime) o;
      return OffsetDateTime.of(odt.toLocalDateTime(), odt.getOffset());
    } else {
      return o;
    }
  }
}
