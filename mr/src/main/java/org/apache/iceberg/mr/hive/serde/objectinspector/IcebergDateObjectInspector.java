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


import java.time.LocalDate;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.util.DateTimeUtil;

public final class IcebergDateObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements DateObjectInspector, WriteObjectInspector {

  private static final IcebergDateObjectInspector INSTANCE = new IcebergDateObjectInspector();

  public static IcebergDateObjectInspector get() {
    return INSTANCE;
  }

  private IcebergDateObjectInspector() {
    super(TypeInfoFactory.dateTypeInfo);
  }

  @Override
  public Date getPrimitiveJavaObject(Object o) {
    return o == null ? null : Date.valueOf(String.valueOf(o));
  }

  @Override
  public DateWritableV2 getPrimitiveWritableObject(Object o) {
    return o == null ? null : new DateWritableV2(DateTimeUtil.daysFromDate((LocalDate) o));
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof Date) {
      return new Date((Date) o);
    } else if (o instanceof LocalDate) {
      return LocalDate.of(
          ((LocalDate) o).getYear(), ((LocalDate) o).getMonth(), ((LocalDate) o).getDayOfMonth());
    } else {
      return o;
    }
  }

  @Override
  public LocalDate convert(Object o) {
    return o == null ? null : (LocalDate) o;
  }
}
