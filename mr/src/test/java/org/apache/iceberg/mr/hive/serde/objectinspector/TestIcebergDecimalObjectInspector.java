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

import java.math.BigDecimal;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergDecimalObjectInspector {

  @Test
  public void testCache() {
    HiveDecimalObjectInspector oi = IcebergDecimalObjectInspector.get(38, 18);

    Assert.assertSame(oi, IcebergDecimalObjectInspector.get(38, 18));
    Assert.assertNotSame(oi, IcebergDecimalObjectInspector.get(28, 18));
    Assert.assertNotSame(oi, IcebergDecimalObjectInspector.get(38, 28));
  }

  @Test
  public void testIcebergDecimalObjectInspector() {
    HiveDecimalObjectInspector oi = IcebergDecimalObjectInspector.get(38, 18);

    Assert.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assert.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL, oi.getPrimitiveCategory());

    Assert.assertEquals(new DecimalTypeInfo(38, 18), oi.getTypeInfo());
    Assert.assertEquals(TypeInfoFactory.decimalTypeInfo.getTypeName(), oi.getTypeName());

    Assert.assertEquals(38, oi.precision());
    Assert.assertEquals(18, oi.scale());

    Assert.assertEquals(HiveDecimal.class, oi.getJavaPrimitiveClass());
    Assert.assertEquals(HiveDecimalWritable.class, oi.getPrimitiveWritableClass());

    Assert.assertNull(oi.copyObject(null));
    Assert.assertNull(oi.getPrimitiveJavaObject(null));
    Assert.assertNull(oi.getPrimitiveWritableObject(null));

    HiveDecimal one = HiveDecimal.create(BigDecimal.ONE);

    Assert.assertEquals(one, oi.getPrimitiveJavaObject(BigDecimal.ONE));
    Assert.assertEquals(new HiveDecimalWritable(one), oi.getPrimitiveWritableObject(BigDecimal.ONE));

    HiveDecimal copy = (HiveDecimal) oi.copyObject(one);

    Assert.assertEquals(one, copy);
    Assert.assertNotSame(one, copy);

    Assert.assertFalse(oi.preferWritable());
  }

}
