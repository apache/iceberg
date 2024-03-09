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

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class TestIcebergTimestampWithZoneObjectInspector {

  @Test
  public void testIcebergTimestampObjectInspectorWithUTCAdjustment() {
    IcebergTimestampWithZoneObjectInspector oi = IcebergTimestampWithZoneObjectInspector.get();

    assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
    assertThat(oi.getPrimitiveCategory())
        .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP);

    assertThat(oi.getTypeInfo()).isEqualTo(TypeInfoFactory.timestampTypeInfo);
    assertThat(oi.getTypeName()).isEqualTo(TypeInfoFactory.timestampTypeInfo.getTypeName());

    assertThat(oi.getJavaPrimitiveClass()).isEqualTo(Timestamp.class);
    assertThat(oi.getPrimitiveWritableClass()).isEqualTo(TimestampWritable.class);

    assertThat(oi.copyObject(null)).isNull();
    assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    assertThat(oi.convert(null)).isNull();

    LocalDateTime local = LocalDateTime.of(2020, 1, 1, 16, 45, 33, 456000);
    OffsetDateTime offsetDateTime = OffsetDateTime.of(local, ZoneOffset.ofHours(-5));
    Timestamp ts = Timestamp.from(offsetDateTime.toInstant());

    assertThat(oi.getPrimitiveJavaObject(offsetDateTime)).isEqualTo(ts);
    assertThat(oi.getPrimitiveWritableObject(offsetDateTime)).isEqualTo(new TimestampWritable(ts));

    Timestamp copy = (Timestamp) oi.copyObject(ts);

    assertThat(copy).isEqualTo(ts);
    assertThat(copy).isNotSameAs(ts);

    assertThat(oi.preferWritable()).isFalse();

    assertThat(oi.convert(ts))
        .isEqualTo(
            OffsetDateTime.ofInstant(local.toInstant(ZoneOffset.ofHours(-5)), ZoneOffset.UTC));

    assertThat(offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC))
        .isEqualTo(oi.convert(Timestamp.from(offsetDateTime.toInstant())));
  }
}
