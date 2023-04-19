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

import java.sql.Date;
import java.time.LocalDate;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergDateObjectInspector {

  @Test
  public void testIcebergDateObjectInspector() {
    DateObjectInspector oi = IcebergDateObjectInspector.get();

    Assert.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assert.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.DATE, oi.getPrimitiveCategory());

    Assert.assertEquals(TypeInfoFactory.dateTypeInfo, oi.getTypeInfo());
    Assert.assertEquals(TypeInfoFactory.dateTypeInfo.getTypeName(), oi.getTypeName());

    Assert.assertEquals(Date.class, oi.getJavaPrimitiveClass());
    Assert.assertEquals(DateWritable.class, oi.getPrimitiveWritableClass());

    Assert.assertNull(oi.copyObject(null));
    Assert.assertNull(oi.getPrimitiveJavaObject(null));
    Assert.assertNull(oi.getPrimitiveWritableObject(null));

    LocalDate local = LocalDate.of(2020, 1, 1);
    Date date = Date.valueOf("2020-01-01");

    Assert.assertEquals(date, oi.getPrimitiveJavaObject(local));
    Assert.assertEquals(new DateWritable(date), oi.getPrimitiveWritableObject(local));

    Date copy = (Date) oi.copyObject(date);

    Assert.assertEquals(date, copy);
    Assert.assertNotSame(date, copy);

    Assert.assertFalse(oi.preferWritable());
  }
}
