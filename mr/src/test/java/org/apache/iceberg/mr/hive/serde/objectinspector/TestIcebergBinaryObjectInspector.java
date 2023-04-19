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

import java.nio.ByteBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergBinaryObjectInspector {

  @Test
  public void testIcebergByteBufferObjectInspector() {
    BinaryObjectInspector oi = IcebergBinaryObjectInspector.get();

    Assert.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assert.assertEquals(
        PrimitiveObjectInspector.PrimitiveCategory.BINARY, oi.getPrimitiveCategory());

    Assert.assertEquals(TypeInfoFactory.binaryTypeInfo, oi.getTypeInfo());
    Assert.assertEquals(TypeInfoFactory.binaryTypeInfo.getTypeName(), oi.getTypeName());

    Assert.assertEquals(byte[].class, oi.getJavaPrimitiveClass());
    Assert.assertEquals(BytesWritable.class, oi.getPrimitiveWritableClass());

    Assert.assertNull(oi.copyObject(null));
    Assert.assertNull(oi.getPrimitiveJavaObject(null));
    Assert.assertNull(oi.getPrimitiveWritableObject(null));

    byte[] bytes = new byte[] {0, 1, 2, 3};

    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    Assert.assertArrayEquals(bytes, oi.getPrimitiveJavaObject(buffer));
    Assert.assertEquals(new BytesWritable(bytes), oi.getPrimitiveWritableObject(buffer));

    ByteBuffer slice = ByteBuffer.wrap(bytes, 1, 2).slice();
    Assert.assertArrayEquals(new byte[] {1, 2}, oi.getPrimitiveJavaObject(slice));
    Assert.assertEquals(new BytesWritable(new byte[] {1, 2}), oi.getPrimitiveWritableObject(slice));

    slice.position(1);
    Assert.assertArrayEquals(new byte[] {2}, oi.getPrimitiveJavaObject(slice));
    Assert.assertEquals(new BytesWritable(new byte[] {2}), oi.getPrimitiveWritableObject(slice));

    byte[] copy = (byte[]) oi.copyObject(bytes);

    Assert.assertArrayEquals(bytes, copy);
    Assert.assertNotSame(bytes, copy);

    Assert.assertFalse(oi.preferWritable());
  }
}
