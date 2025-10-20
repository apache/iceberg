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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.Test;

public class TestStructInternalRowVariant {

  @Test
  public void testGetVariantReturnsVariantVal() {
    Types.StructType structType =
        Types.StructType.of(Types.NestedField.optional(1, "a", Types.VariantType.get()));

    GenericRecord rec = GenericRecord.create(structType);
    VariantMetadata md = Variants.metadata("k");

    org.apache.iceberg.variants.ShreddedObject obj = Variants.object(md);
    obj.put("k", Variants.of("v1"));
    Variant variant = Variant.of(md, obj);
    rec.set(0, variant);

    InternalRow row = new StructInternalRow(structType).setStruct(rec);

    VariantVal actual = row.getVariant(0);
    assertThat(actual).isNotNull();

    VariantMetadata metadata =
        VariantMetadata.from(ByteBuffer.wrap(actual.getMetadata()).order(ByteOrder.LITTLE_ENDIAN));
    assertThat(metadata.dictionarySize()).isEqualTo(1);
    assertThat(metadata.get(0)).isEqualTo("k");

    VariantValue actualValue =
        VariantValue.from(
            metadata, ByteBuffer.wrap(actual.getValue()).order(ByteOrder.LITTLE_ENDIAN));

    assertThat(actualValue.asObject().get("k").asPrimitive().get()).isEqualTo("v1");
  }

  @Test
  public void testGetVariantNull() {
    Types.StructType structType =
        Types.StructType.of(Types.NestedField.optional(1, "a", Types.VariantType.get()));
    GenericRecord rec = GenericRecord.create(structType);
    rec.set(0, null);

    InternalRow row = new StructInternalRow(structType).setStruct(rec);
    assertThat(row.getVariant(0)).isNull();
  }
}
