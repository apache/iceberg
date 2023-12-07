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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergDateObjectInspector {

  @Test
  public void testIcebergDateObjectInspector() {
    DateObjectInspector oi = IcebergDateObjectInspector.get();

    Assertions.assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
    Assertions.assertThat(oi.getPrimitiveCategory())
        .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.DATE);

    Assertions.assertThat(oi.getTypeInfo()).isEqualTo(TypeInfoFactory.dateTypeInfo);
    Assertions.assertThat(oi.getTypeName()).isEqualTo(TypeInfoFactory.dateTypeInfo.getTypeName());

    Assertions.assertThat(oi.getJavaPrimitiveClass()).isEqualTo(Date.class);
    Assertions.assertThat(oi.getPrimitiveWritableClass()).isEqualTo(DateWritable.class);

    Assertions.assertThat(oi.copyObject(null)).isNull();
    Assertions.assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    Assertions.assertThat(oi.getPrimitiveWritableObject(null)).isNull();

    LocalDate local = LocalDate.of(2020, 1, 1);
    Date date = Date.valueOf("2020-01-01");

    Assertions.assertThat(oi.getPrimitiveJavaObject(local)).isEqualTo(date);
    Assertions.assertThat(oi.getPrimitiveWritableObject(local)).isEqualTo(new DateWritable(date));

    Date copy = (Date) oi.copyObject(date);

    Assertions.assertThat(copy).isEqualTo(date);
    Assertions.assertThat(copy).isNotSameAs(date);

    Assertions.assertThat(oi.preferWritable()).isFalse();
  }
}
