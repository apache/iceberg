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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergFixedObjectInspector {

  @Test
  public void testIcebergFixedObjectInspector() {
    IcebergFixedObjectInspector oi = IcebergFixedObjectInspector.get();

    Assertions.assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
    Assertions.assertThat(oi.getPrimitiveCategory())
        .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.BINARY);

    Assertions.assertThat(oi.getTypeInfo()).isEqualTo(TypeInfoFactory.binaryTypeInfo);
    Assertions.assertThat(oi.getTypeName()).isEqualTo(TypeInfoFactory.binaryTypeInfo.getTypeName());

    Assertions.assertThat(oi.getJavaPrimitiveClass()).isEqualTo(byte[].class);
    Assertions.assertThat(oi.getPrimitiveWritableClass()).isEqualTo(BytesWritable.class);

    Assertions.assertThat(oi.copyObject(null)).isNull();
    Assertions.assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    Assertions.assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    Assertions.assertThat(oi.convert(null)).isNull();

    byte[] bytes = new byte[] {0, 1};
    BytesWritable bytesWritable = new BytesWritable(bytes);

    Assertions.assertThat(oi.getPrimitiveJavaObject(bytes)).isEqualTo(bytes);
    Assertions.assertThat(oi.getPrimitiveWritableObject(bytes)).isEqualTo(bytesWritable);
    Assertions.assertThat(oi.convert(bytes)).isEqualTo(bytes);

    byte[] copy = (byte[]) oi.copyObject(bytes);

    Assertions.assertThat(copy).isEqualTo(bytes);
    Assertions.assertThat(copy).isNotSameAs(bytes);

    Assertions.assertThat(oi.preferWritable()).isFalse();
  }
}
