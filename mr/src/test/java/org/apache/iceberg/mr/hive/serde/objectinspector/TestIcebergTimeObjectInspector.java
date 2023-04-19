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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergTimeObjectInspector {

  @Test
  public void testIcebergTimeObjectInspector() {

    IcebergTimeObjectInspector oi = IcebergTimeObjectInspector.get();

    Assert.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assert.assertEquals(
        PrimitiveObjectInspector.PrimitiveCategory.STRING, oi.getPrimitiveCategory());

    Assert.assertEquals(TypeInfoFactory.stringTypeInfo, oi.getTypeInfo());
    Assert.assertEquals(TypeInfoFactory.stringTypeInfo.getTypeName(), oi.getTypeName());

    Assert.assertEquals(String.class, oi.getJavaPrimitiveClass());
    Assert.assertEquals(Text.class, oi.getPrimitiveWritableClass());

    Assert.assertNull(oi.copyObject(null));
    Assert.assertNull(oi.getPrimitiveJavaObject(null));
    Assert.assertNull(oi.getPrimitiveWritableObject(null));
    Assert.assertNull(oi.convert(null));

    LocalTime localTime = LocalTime.now();
    String time = localTime.toString();
    Text text = new Text(time);

    Assert.assertEquals(time, oi.getPrimitiveJavaObject(text));
    Assert.assertEquals(text, oi.getPrimitiveWritableObject(time));
    Assert.assertEquals(localTime, oi.convert(time));

    Text copy = (Text) oi.copyObject(text);

    Assert.assertEquals(text, copy);
    Assert.assertNotSame(text, copy);

    Assert.assertFalse(oi.preferWritable());
  }
}
