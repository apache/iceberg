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

import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergTimestampObjectInspector {

  @Test
  public void testIcebergTimestampObjectInspector() {
    IcebergTimestampObjectInspector oi = IcebergTimestampObjectInspector.get();

    Assertions.assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
    Assertions.assertThat(oi.getPrimitiveCategory())
        .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP);

    Assertions.assertThat(oi.getTypeInfo()).isEqualTo(TypeInfoFactory.timestampTypeInfo);
    Assertions.assertThat(oi.getTypeName())
        .isEqualTo(TypeInfoFactory.timestampTypeInfo.getTypeName());

    Assertions.assertThat(oi.getJavaPrimitiveClass()).isEqualTo(Timestamp.class);
    Assertions.assertThat(oi.getPrimitiveWritableClass()).isEqualTo(TimestampWritable.class);

    Assertions.assertThat(oi.copyObject(null)).isNull();
    Assertions.assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    Assertions.assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    Assertions.assertThat(oi.convert(null)).isNull();

    LocalDateTime local = LocalDateTime.of(2020, 1, 1, 12, 55, 30, 5560000);
    Timestamp ts = Timestamp.valueOf(local);

    Assertions.assertThat(oi.getPrimitiveJavaObject(local)).isEqualTo(ts);
    Assertions.assertThat(oi.getPrimitiveWritableObject(local))
        .isEqualTo(new TimestampWritable(ts));

    Timestamp copy = (Timestamp) oi.copyObject(ts);

    Assertions.assertThat(copy).isEqualTo(ts);
    Assertions.assertThat(copy).isNotSameAs(ts);

    Assertions.assertThat(oi.preferWritable()).isFalse();

    Assertions.assertThat(oi.convert(ts)).isEqualTo(local);
  }
}
