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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.VariantType$;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.Test;

public class TestStructInternalRowVariant {

  @Test
  public void testGetVariantReturnsVariantVal() {
    Types.StructType structType = variantStructType();
    GenericRecord rec = newRecord(structType);
    Variant variant = sampleVariant();
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
    Types.StructType structType = variantStructType();
    GenericRecord rec = newRecord(structType);
    rec.set(0, null);

    InternalRow row = new StructInternalRow(structType).setStruct(rec);
    assertThat(row.getVariant(0)).isNull();
  }

  @Test
  public void testGetVariantPassesThroughVariantVal() {
    Types.StructType structType = variantStructType();
    GenericRecord rec = newRecord(structType);

    Variant variant = sampleVariant();
    byte[] metadataBytes = new byte[variant.metadata().sizeInBytes()];
    ByteBuffer metadataBuffer = ByteBuffer.wrap(metadataBytes).order(ByteOrder.LITTLE_ENDIAN);
    variant.metadata().writeTo(metadataBuffer, 0);

    byte[] valueBytes = new byte[variant.value().sizeInBytes()];
    ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);
    variant.value().writeTo(valueBuffer, 0);

    VariantVal expected = new VariantVal(valueBytes, metadataBytes);
    rec.set(0, expected);

    InternalRow row = new StructInternalRow(structType).setStruct(rec);
    VariantVal actual = row.getVariant(0);

    assertThat(actual).isSameAs(expected);
  }

  @Test
  public void testArrayOfVariant() {
    Types.ListType listType = Types.ListType.ofOptional(2, Types.VariantType.get());
    Types.StructType structType =
        Types.StructType.of(Types.NestedField.optional(1, "arr", listType));

    GenericRecord rec = GenericRecord.create(structType);

    Variant v1 = sampleVariant();
    VariantVal v2 = toVariantVal(v1);

    List<Object> elements = Arrays.asList(v1, v2, null);
    rec.set(0, elements);

    InternalRow row = new StructInternalRow(structType).setStruct(rec);
    ArrayData arr = row.getArray(0);

    Object firstVar = arr.get(0, VariantType$.MODULE$);
    Object secondVar = arr.get(1, VariantType$.MODULE$);

    assertThat(firstVar).isInstanceOf(VariantVal.class);
    assertThat(secondVar).isInstanceOf(VariantVal.class);
    assertThat(arr.isNullAt(2)).isTrue();

    assertVariantValEqualsKV((VariantVal) firstVar, "k", "v1");
    assertVariantValEqualsKV((VariantVal) secondVar, "k", "v1");
  }

  @Test
  public void testMapWithVariant() {
    Types.MapType mapType =
        Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.VariantType.get());
    Types.StructType structType = Types.StructType.of(Types.NestedField.optional(1, "m", mapType));

    GenericRecord rec = GenericRecord.create(structType);
    Map<String, Object> map = Maps.newHashMap();
    map.put("a", sampleVariant());
    map.put("b", toVariantVal(sampleVariant()));
    rec.set(0, map);

    InternalRow row = new StructInternalRow(structType).setStruct(rec);
    MapData mapData = row.getMap(0);

    ArrayData values = mapData.valueArray();
    for (int i = 0; i < values.numElements(); i++) {
      Object variant = values.get(i, VariantType$.MODULE$);
      assertThat(variant).isInstanceOf(VariantVal.class);
      assertVariantValEqualsKV((VariantVal) variant, "k", "v1");
    }
  }

  @Test
  public void testNestedStructVariant() {
    Types.StructType variant =
        Types.StructType.of(Types.NestedField.optional(2, "v", Types.VariantType.get()));
    Types.StructType structVariant =
        Types.StructType.of(Types.NestedField.optional(1, "n", variant));

    // Case 1: nested struct holds Iceberg Variant
    GenericRecord variantStructRec = GenericRecord.create(variant);
    variantStructRec.set(0, sampleVariant());
    GenericRecord structRec = GenericRecord.create(structVariant);
    structRec.set(0, variantStructRec);

    InternalRow structRow = new StructInternalRow(structVariant).setStruct(structRec);
    InternalRow nested = structRow.getStruct(0, 1);
    VariantVal variantVal1 = nested.getVariant(0);
    assertVariantValEqualsKV(variantVal1, "k", "v1");

    // Case 2: nested struct holds Spark VariantVal (pass-through)
    GenericRecord variantStructRec2 = GenericRecord.create(variant);
    variantStructRec2.set(0, toVariantVal(sampleVariant()));
    GenericRecord structRec2 = GenericRecord.create(structVariant);
    structRec2.set(0, variantStructRec2);

    InternalRow structRow2 = new StructInternalRow(structVariant).setStruct(structRec2);
    InternalRow nested2 = structRow2.getStruct(0, 1);
    VariantVal variantVal2 = nested2.getVariant(0);
    assertVariantValEqualsKV(variantVal2, "k", "v1");
  }

  @Test
  public void testGetWithVariantType() {
    Types.StructType structType = variantStructType();
    GenericRecord rec = newRecord(structType);
    rec.set(0, sampleVariant());

    InternalRow row = new StructInternalRow(structType).setStruct(rec);
    Object obj = row.get(0, VariantType$.MODULE$);
    assertThat(obj).isInstanceOf(VariantVal.class);
    assertVariantValEqualsKV((VariantVal) obj, "k", "v1");
  }

  private static Types.StructType variantStructType() {
    return Types.StructType.of(Types.NestedField.optional(1, "a", Types.VariantType.get()));
  }

  private static GenericRecord newRecord(Types.StructType structType) {
    return GenericRecord.create(structType);
  }

  private static Variant sampleVariant() {
    VariantMetadata md = Variants.metadata("k");
    org.apache.iceberg.variants.ShreddedObject obj = Variants.object(md);
    obj.put("k", Variants.of("v1"));
    return Variant.of(md, obj);
  }

  private static VariantVal toVariantVal(Variant variant) {
    byte[] metadataBytes = new byte[variant.metadata().sizeInBytes()];
    ByteBuffer metadataBuffer = ByteBuffer.wrap(metadataBytes).order(ByteOrder.LITTLE_ENDIAN);
    variant.metadata().writeTo(metadataBuffer, 0);

    byte[] valueBytes = new byte[variant.value().sizeInBytes()];
    ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);
    variant.value().writeTo(valueBuffer, 0);

    return new VariantVal(valueBytes, metadataBytes);
  }

  private static void assertVariantValEqualsKV(VariantVal vv, String key, String expected) {
    VariantMetadata metadata =
        VariantMetadata.from(ByteBuffer.wrap(vv.getMetadata()).order(ByteOrder.LITTLE_ENDIAN));
    VariantValue value =
        VariantValue.from(metadata, ByteBuffer.wrap(vv.getValue()).order(ByteOrder.LITTLE_ENDIAN));
    assertThat(value.asObject().get(key).asPrimitive().get()).isEqualTo(expected);
  }
}
