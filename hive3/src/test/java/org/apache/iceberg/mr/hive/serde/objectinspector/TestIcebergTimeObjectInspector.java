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

import java.time.LocalTime;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TestIcebergTimeObjectInspector {

  @Test
  public void testIcebergTimeObjectInspector() {

    IcebergTimeObjectInspector oi = IcebergTimeObjectInspector.get();

    assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
    assertThat(oi.getPrimitiveCategory())
        .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.STRING);

    assertThat(oi.getTypeInfo()).isEqualTo(TypeInfoFactory.stringTypeInfo);
    assertThat(oi.getTypeName()).isEqualTo(TypeInfoFactory.stringTypeInfo.getTypeName());

    assertThat(oi.getJavaPrimitiveClass()).isEqualTo(String.class);
    assertThat(oi.getPrimitiveWritableClass()).isEqualTo(Text.class);

    assertThat(oi.copyObject(null)).isNull();
    assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    assertThat(oi.convert(null)).isNull();

    LocalTime localTime = LocalTime.now();
    String time = localTime.toString();
    Text text = new Text(time);

    assertThat(oi.getPrimitiveJavaObject(text)).isEqualTo(time);
    assertThat(oi.getPrimitiveWritableObject(time)).isEqualTo(text);
    assertThat(oi.convert(time)).isEqualTo(localTime);

    Text copy = (Text) oi.copyObject(text);

    assertThat(copy).isEqualTo(text);
    assertThat(copy).isNotSameAs(text);

    assertThat(oi.preferWritable()).isFalse();
  }
}
