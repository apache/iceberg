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

import java.time.LocalTime;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

public class IcebergTimeObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements StringObjectInspector, WriteObjectInspector {

  private static final IcebergTimeObjectInspector INSTANCE = new IcebergTimeObjectInspector();

  private IcebergTimeObjectInspector() {
    super(TypeInfoFactory.stringTypeInfo);
  }

  public static IcebergTimeObjectInspector get() {
    return INSTANCE;
  }

  @Override
  public String getPrimitiveJavaObject(Object o) {
    return o == null ? null : o.toString();
  }

  @Override
  public Text getPrimitiveWritableObject(Object o) {
    String value = getPrimitiveJavaObject(o);
    return value == null ? null : new Text(value);
  }

  @Override
  public LocalTime convert(Object o) {
    return o == null ? null : LocalTime.parse((String) o);
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof Text) {
      return new Text((Text) o);
    } else {
      return o;
    }
  }
}
