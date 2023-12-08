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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.jupiter.api.Test;

public class TestIcebergBinaryObjectInspector {

  @Test
  public void testIcebergByteBufferObjectInspector() {
    BinaryObjectInspector oi = IcebergBinaryObjectInspector.get();

    assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
    assertThat(oi.getPrimitiveCategory())
        .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.BINARY);

    assertThat(oi.getTypeInfo()).isEqualTo(TypeInfoFactory.binaryTypeInfo);
    assertThat(oi.getTypeName()).isEqualTo(TypeInfoFactory.binaryTypeInfo.getTypeName());

    assertThat(oi.getJavaPrimitiveClass()).isEqualTo(byte[].class);
    assertThat(oi.getPrimitiveWritableClass()).isEqualTo(BytesWritable.class);

    assertThat(oi.copyObject(null)).isNull();
    assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    assertThat(oi.getPrimitiveWritableObject(null)).isNull();

    byte[] bytes = new byte[] {0, 1, 2, 3};

    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    assertThat(oi.getPrimitiveJavaObject(buffer)).isEqualTo(bytes);
    assertThat(oi.getPrimitiveWritableObject(buffer)).isEqualTo(new BytesWritable(bytes));

    ByteBuffer slice = ByteBuffer.wrap(bytes, 1, 2).slice();
    assertThat(oi.getPrimitiveJavaObject(slice)).isEqualTo(new byte[] {1, 2});
    assertThat(oi.getPrimitiveWritableObject(slice))
        .isEqualTo(new BytesWritable(new byte[] {1, 2}));

    slice.position(1);
    assertThat(oi.getPrimitiveJavaObject(slice)).isEqualTo(new byte[] {2});
    assertThat(oi.getPrimitiveWritableObject(slice)).isEqualTo(new BytesWritable(new byte[] {2}));

    byte[] copy = (byte[]) oi.copyObject(bytes);

    assertThat(copy).isEqualTo(bytes);
    assertThat(copy).isNotSameAs(bytes);

    assertThat(oi.preferWritable()).isFalse();
  }
}
