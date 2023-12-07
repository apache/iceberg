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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergTimeObjectInspector {

  @Test
  public void testIcebergTimeObjectInspector() {

    IcebergTimeObjectInspector oi = IcebergTimeObjectInspector.get();

    Assertions.assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
    Assertions.assertThat(oi.getPrimitiveCategory())
        .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.STRING);

    Assertions.assertThat(oi.getTypeInfo()).isEqualTo(TypeInfoFactory.stringTypeInfo);
    Assertions.assertThat(oi.getTypeName()).isEqualTo(TypeInfoFactory.stringTypeInfo.getTypeName());

    Assertions.assertThat(oi.getJavaPrimitiveClass()).isEqualTo(String.class);
    Assertions.assertThat(oi.getPrimitiveWritableClass()).isEqualTo(Text.class);

    Assertions.assertThat(oi.copyObject(null)).isNull();
    Assertions.assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    Assertions.assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    Assertions.assertThat(oi.convert(null)).isNull();

    LocalTime localTime = LocalTime.now();
    String time = localTime.toString();
    Text text = new Text(time);

    Assertions.assertThat(oi.getPrimitiveJavaObject(text)).isEqualTo(time);
    Assertions.assertThat(oi.getPrimitiveWritableObject(time)).isEqualTo(text);
    Assertions.assertThat(oi.convert(time)).isEqualTo(localTime);

    Text copy = (Text) oi.copyObject(text);

    Assertions.assertThat(copy).isEqualTo(text);
    Assertions.assertThat(copy).isNotSameAs(text);

    Assertions.assertThat(oi.preferWritable()).isFalse();
  }
}
