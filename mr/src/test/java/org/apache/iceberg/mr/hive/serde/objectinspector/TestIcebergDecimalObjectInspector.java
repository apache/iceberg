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

import java.math.BigDecimal;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class TestIcebergDecimalObjectInspector {

  @Test
  public void testCache() {
    HiveDecimalObjectInspector oi = IcebergDecimalObjectInspector.get(38, 18);

    assertThat(IcebergDecimalObjectInspector.get(38, 18)).isSameAs(oi);
    assertThat(IcebergDecimalObjectInspector.get(28, 18)).isNotSameAs(oi);
    assertThat(IcebergDecimalObjectInspector.get(38, 28)).isNotSameAs(oi);
  }

  @Test
  public void testIcebergDecimalObjectInspector() {
    HiveDecimalObjectInspector oi = IcebergDecimalObjectInspector.get(38, 18);

    assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
    assertThat(oi.getPrimitiveCategory())
        .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL);

    assertThat(oi.getTypeInfo()).isEqualTo(new DecimalTypeInfo(38, 18));
    assertThat(oi.getTypeName())
        .isEqualTo(TypeInfoFactory.decimalTypeInfo.getTypeName(), oi.getTypeName());

    assertThat(oi.precision()).isEqualTo(38);
    assertThat(oi.scale()).isEqualTo(18);

    assertThat(oi.getJavaPrimitiveClass()).isEqualTo(HiveDecimal.class);
    assertThat(oi.getPrimitiveWritableClass()).isEqualTo(HiveDecimalWritable.class);

    assertThat(oi.copyObject(null)).isNull();
    assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    assertThat(oi.getPrimitiveWritableObject(null)).isNull();

    HiveDecimal one = HiveDecimal.create(BigDecimal.ONE);

    assertThat(oi.getPrimitiveJavaObject(BigDecimal.ONE)).isEqualTo(one);
    assertThat(oi.getPrimitiveWritableObject(BigDecimal.ONE))
        .isEqualTo(new HiveDecimalWritable(one));

    HiveDecimal copy = (HiveDecimal) oi.copyObject(one);

    assertThat(copy).isEqualTo(one);
    assertThat(copy).isNotSameAs(one);

    assertThat(oi.preferWritable()).isFalse();
  }
}
