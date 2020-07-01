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

package org.apache.iceberg.mr.mapred.serde.objectinspector;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public abstract class IcebergTimestampObjectInspector extends AbstractPrimitiveJavaObjectInspector
                                                      implements TimestampObjectInspector {

  private static final IcebergTimestampObjectInspector INSTANCE_WITH_ZONE = new IcebergTimestampObjectInspector() {
    @Override
    LocalDateTime toLocalDateTime(Object o) {
      return ((OffsetDateTime) o).toLocalDateTime();
    }
  };

  private static final IcebergTimestampObjectInspector INSTANCE_WITHOUT_ZONE = new IcebergTimestampObjectInspector() {
    @Override
    LocalDateTime toLocalDateTime(Object o) {
      return (LocalDateTime) o;
    }
  };

  public static IcebergTimestampObjectInspector get(boolean adjustToUTC) {
    return adjustToUTC ? INSTANCE_WITH_ZONE : INSTANCE_WITHOUT_ZONE;
  }

  private IcebergTimestampObjectInspector() {
    super(TypeInfoFactory.timestampTypeInfo);
  }


  abstract LocalDateTime toLocalDateTime(Object object);

  @Override
  public Timestamp getPrimitiveJavaObject(Object o) {
    return o == null ? null : Timestamp.valueOf(toLocalDateTime(o));
  }

  @Override
  public TimestampWritable getPrimitiveWritableObject(Object o) {
    Timestamp ts = getPrimitiveJavaObject(o);
    return ts == null ? null : new TimestampWritable(ts);
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }

    Timestamp ts = (Timestamp) o;
    Timestamp copy = new Timestamp(ts.getTime());
    copy.setNanos(ts.getNanos());
    return copy;
  }

}
