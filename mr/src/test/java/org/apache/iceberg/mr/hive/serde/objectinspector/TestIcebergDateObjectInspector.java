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

import java.sql.Date;
import java.time.LocalDate;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class TestIcebergDateObjectInspector {

  @Test
  public void testIcebergDateObjectInspector() {
    DateObjectInspector oi = IcebergDateObjectInspector.get();

    assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
    assertThat(oi.getPrimitiveCategory())
        .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.DATE);

    assertThat(oi.getTypeInfo()).isEqualTo(TypeInfoFactory.dateTypeInfo);
    assertThat(oi.getTypeName()).isEqualTo(TypeInfoFactory.dateTypeInfo.getTypeName());

    assertThat(oi.getJavaPrimitiveClass()).isEqualTo(Date.class);
    assertThat(oi.getPrimitiveWritableClass()).isEqualTo(DateWritable.class);

    assertThat(oi.copyObject(null)).isNull();
    assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    assertThat(oi.getPrimitiveWritableObject(null)).isNull();

    LocalDate local = LocalDate.of(2020, 1, 1);
    Date date = Date.valueOf("2020-01-01");

    assertThat(oi.getPrimitiveJavaObject(local)).isEqualTo(date);
    assertThat(oi.getPrimitiveWritableObject(local)).isEqualTo(new DateWritable(date));

    Date copy = (Date) oi.copyObject(date);

    assertThat(copy).isEqualTo(date);
    assertThat(copy).isNotSameAs(date);

    assertThat(oi.preferWritable()).isFalse();
  }
}
