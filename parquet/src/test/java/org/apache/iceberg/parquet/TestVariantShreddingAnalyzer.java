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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestVariantShreddingAnalyzer {

  @Test
  public void testDepthLimitStopsObjectRecursion() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    // Each level has {"a": <nested>, "x": 1} so objects always have a shreddable primitive
    VariantMetadata meta = Variants.metadata("a", "x");
    ShreddedObject innermost = Variants.object(meta);
    innermost.put("a", Variants.of(42));
    innermost.put("x", Variants.of(1));

    for (int i = 0; i < 54; i++) {
      ShreddedObject wrapper = Variants.object(meta);
      wrapper.put("a", innermost);
      wrapper.put("x", Variants.of(1));
      innermost = wrapper;
    }

    Type schema = analyzer.analyzeAndCreateSchema(List.of(innermost), 0);
    assertThat(schema).isNotNull();
    assertThat(schema.getName()).isEqualTo("typed_value");

    int shreddedDepth = countObjectDepth(schema);
    assertThat(shreddedDepth).isLessThanOrEqualTo(50).isGreaterThan(0);
  }

  @Test
  public void testDepthLimitStopsArrayRecursion() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    // 55-level nested arrays with a primitive only at the very bottom.
    // Depth limit (50) prevents reaching the leaf, so schema is null (graceful degradation).
    VariantValue innermost = Variants.of(42);
    for (int i = 0; i < 55; i++) {
      ValueArray wrapper = Variants.array();
      wrapper.add(innermost);
      innermost = wrapper;
    }

    Type schema = analyzer.analyzeAndCreateSchema(List.of(innermost), 0);
    assertThat(schema).isNull();
  }

  @Test
  public void testArrayWithinDepthLimit() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    // 5-level nested arrays
    VariantValue innermost = Variants.of(42);
    for (int i = 0; i < 5; i++) {
      ValueArray wrapper = Variants.array();
      wrapper.add(innermost);
      innermost = wrapper;
    }

    Type schema = analyzer.analyzeAndCreateSchema(List.of(innermost), 0);
    assertThat(schema).isNotNull();
    assertThat(schema.getName()).isEqualTo("typed_value");

    int arrayDepth = countArrayDepth(schema);
    assertThat(arrayDepth).isEqualTo(5);
  }

  @Test
  public void testIntermediateFieldCapLimitsTrackedFields() {
    int numFields = 1500;
    String[] fieldNames = new String[numFields];
    for (int i = 0; i < numFields; i++) {
      fieldNames[i] = String.format(Locale.ROOT, "field_%04d", i);
    }

    VariantMetadata meta = Variants.metadata(fieldNames);
    ShreddedObject obj = Variants.object(meta);
    for (String name : fieldNames) {
      obj.put(name, Variants.of(42));
    }

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(obj), 0);

    assertThat(schema).isNotNull();
    assertThat(schema).isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.getFieldCount()).isLessThanOrEqualTo(300).isGreaterThan(0);
    assertThat(typedValue.containsField("field_0000")).isTrue();
    assertThat(typedValue.containsField("field_0299")).isTrue();
    assertThat(typedValue.containsField("field_0300")).isFalse();
  }

  @Test
  public void testFieldCapAllowsExistingFieldUpdates() {
    int numFields = 1500;
    String[] fieldNames = new String[numFields];
    for (int i = 0; i < numFields; i++) {
      fieldNames[i] = String.format(Locale.ROOT, "field_%04d", i);
    }

    VariantMetadata meta = Variants.metadata(fieldNames);

    ShreddedObject row1 = Variants.object(meta);
    for (String name : fieldNames) {
      row1.put(name, Variants.of(42));
    }

    ShreddedObject row2 = Variants.object(meta);
    for (int i = 0; i < 10; i++) {
      row2.put(fieldNames[i], Variants.of(7));
    }

    ShreddedObject row3 = Variants.object(meta);
    for (int i = 0; i < 10; i++) {
      row3.put(fieldNames[i], Variants.of(99));
    }

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2, row3), 0);

    assertThat(schema).isNotNull();
    assertThat(schema).isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.getFieldCount()).isGreaterThan(0).isLessThanOrEqualTo(300);
  }

  @Test
  public void testNestedObjectsWithinDepthLimit() {
    VariantMetadata cityMeta = Variants.metadata("city");
    ShreddedObject city = Variants.object(cityMeta);
    city.put("city", Variants.of("NYC"));

    VariantMetadata addrMeta = Variants.metadata("address");
    ShreddedObject addr = Variants.object(addrMeta);
    addr.put("address", city);

    VariantMetadata rootMeta = Variants.metadata("user");
    ShreddedObject root = Variants.object(rootMeta);
    root.put("user", addr);

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(root), 0);

    assertThat(schema).isNotNull();
    GroupType rootTv = schema.asGroupType();
    assertThat(rootTv.getName()).isEqualTo("typed_value");

    // user -> typed_value -> address -> typed_value -> city -> typed_value (STRING)
    GroupType userGroup = rootTv.getType("user").asGroupType();
    assertThat(userGroup.containsField("value")).isTrue();
    assertThat(userGroup.containsField("typed_value")).isTrue();

    GroupType addrTv = userGroup.getType("typed_value").asGroupType();
    GroupType addrGroup = addrTv.getType("address").asGroupType();
    assertThat(addrGroup.containsField("typed_value")).isTrue();

    GroupType cityTv = addrGroup.getType("typed_value").asGroupType();
    GroupType cityGroup = cityTv.getType("city").asGroupType();
    assertThat(cityGroup.containsField("typed_value")).isTrue();

    PrimitiveType cityPrimitive = cityGroup.getType("typed_value").asPrimitiveType();
    assertThat(cityPrimitive.getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
    assertThat(cityPrimitive.getLogicalTypeAnnotation())
        .isEqualTo(LogicalTypeAnnotation.stringType());
  }

  @Test
  public void testDecimalForExceedingPrecision() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    // Value 1: 30 integer digits, 0 fractional -> precision=30, scale=0, intDigits=30
    // Value 2: 1 integer digit, 20 fractional  -> precision=21, scale=20, intDigits=1
    // Combined: maxIntDigits=30, maxScale=20, raw sum=50 -> capped to precision=38,
    // scale=min(20, 38-30)=8 (integer digits get priority)
    VariantMetadata meta = Variants.metadata("val");
    ShreddedObject row1 = Variants.object(meta);
    row1.put("val", Variants.of(new BigDecimal("123456789012345678901234567890")));

    ShreddedObject row2 = Variants.object(meta);
    row2.put("val", Variants.of(new BigDecimal("1.23456789012345678901")));

    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2), 0);
    assertThat(schema).isNotNull();

    GroupType typedValue = schema.asGroupType();
    GroupType valGroup = typedValue.getType("val").asGroupType();
    PrimitiveType valPrimitive = valGroup.getType("typed_value").asPrimitiveType();

    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal =
        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
            valPrimitive.getLogicalTypeAnnotation();
    assertThat(decimal).isNotNull();
    assertThat(decimal.getPrecision()).isEqualTo(38);
    // With 30 integer digits, scale is capped to 38 - 30 = 8 (integer digits get priority)
    assertThat(decimal.getScale()).isEqualTo(8);
    assertThat(decimal.getScale()).isLessThanOrEqualTo(decimal.getPrecision());

    // Physical type should be FIXED_LEN_BYTE_ARRAY since precision > 18
    assertThat(valPrimitive.getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
  }

  @ParameterizedTest
  @CsvSource({"20, 9", "28, 12", "33, 14"})
  public void testDecimalExceedingPrecisionUsesMinimumFixedLength(
      int precision, int expectedLength) {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    // Precision > 18 shreds to FIXED_LEN_BYTE_ARRAY; the declared length must equal what the
    // writer emits (decimalRequiredBytes).
    VariantMetadata meta = Variants.metadata("val");
    ShreddedObject row = Variants.object(meta);
    row.put("val", Variants.of(new BigDecimal(BigInteger.TEN.pow(precision - 1))));

    Type schema = analyzer.analyzeAndCreateSchema(List.of(row), 0);
    assertThat(schema).isNotNull();

    GroupType valGroup = schema.asGroupType().getType("val").asGroupType();
    PrimitiveType valPrimitive = valGroup.getType("typed_value").asPrimitiveType();

    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal =
        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
            valPrimitive.getLogicalTypeAnnotation();
    assertThat(decimal.getPrecision()).isEqualTo(precision);
    assertThat(valPrimitive.getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    assertThat(valPrimitive.getTypeLength())
        .isEqualTo(expectedLength)
        .isEqualTo(TypeUtil.decimalRequiredBytes(precision));
  }

  @Test
  public void testDecimalForExactPrecision() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    // Value with exactly precision=38: 20 integer digits + 18 scale = 38
    VariantMetadata meta = Variants.metadata("val");
    ShreddedObject row = Variants.object(meta);
    row.put("val", Variants.of(new BigDecimal("12345678901234567890.123456789012345678")));

    Type schema = analyzer.analyzeAndCreateSchema(List.of(row), 0);
    assertThat(schema).isNotNull();

    GroupType typedValue = schema.asGroupType();
    GroupType valGroup = typedValue.getType("val").asGroupType();
    PrimitiveType valPrimitive = valGroup.getType("typed_value").asPrimitiveType();

    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal =
        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
            valPrimitive.getLogicalTypeAnnotation();
    assertThat(decimal.getPrecision()).isEqualTo(38);
    assertThat(decimal.getScale()).isEqualTo(18);
    // Precision 38 is the max and requires the full 16-byte FIXED width.
    assertThat(valPrimitive.getTypeLength())
        .isEqualTo(TypeUtil.decimalRequiredBytes(38))
        .isEqualTo(16);
  }

  @Test
  public void testInfrequentFieldsArePruned() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    // 100 rows: "common" in all, "rare" in only 5 (below MIN_FIELD_FREQUENCY = 0.10)
    List<VariantValue> rows = buildPruningTestRows(5, obj -> obj);

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();

    GroupType group = schema.asGroupType();
    assertThat(group.containsField("common")).isTrue();
    assertThat(group.containsField("rare")).isFalse();
  }

  @Test
  public void testEmptyArrayReturnsNull() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    // All rows are empty arrays, no element type to infer
    List<VariantValue> rows = List.of(Variants.array(), Variants.array(), Variants.array());

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNull();
  }

  @Test
  public void testRootPrimitiveProducesTypedValue() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    // root type is primitive
    List<VariantValue> rows = List.of(Variants.of("hello"), Variants.of("world"), Variants.of("x"));

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();
    assertThat(schema.getName()).isEqualTo("typed_value");
    assertThat(schema.isPrimitive()).isTrue();
    assertThat(schema.asPrimitiveType().getLogicalTypeAnnotation())
        .isEqualTo(LogicalTypeAnnotation.stringType());
  }

  @Test
  public void testRootArrayOfObjectsPrunesInfrequentFields() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    // 100 arrays: "common" in all, "rare" in only 3 (below MIN_FIELD_FREQUENCY = 0.10)
    List<VariantValue> rows =
        buildPruningTestRows(
            3,
            obj -> {
              ValueArray arr = Variants.array();
              arr.add(obj);
              return arr;
            });

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();

    GroupType listType = schema.asGroupType();
    assertThat(listType.getLogicalTypeAnnotation())
        .isInstanceOf(LogicalTypeAnnotation.ListLogicalTypeAnnotation.class);
    GroupType repeatedGroup = listType.getType(0).asGroupType();
    GroupType elementGroup = repeatedGroup.getType(0).asGroupType();
    assertThat(elementGroup.containsField("typed_value")).isTrue();
    GroupType objectFields = elementGroup.getType("typed_value").asGroupType();
    assertThat(objectFields.containsField("common")).isTrue();
    assertThat(objectFields.containsField("rare")).isFalse();
  }

  @Test
  public void testObjectWithArrayChildPrunesNestedFields() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    VariantMetadata itemMeta = Variants.metadata("name", "rare");
    VariantMetadata rootMeta = Variants.metadata("items");

    // 100 rows, "rare" appears in only 3 rows (below MIN_FIELD_FREQUENCY = 0.10)
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      ShreddedObject item = Variants.object(itemMeta);
      item.put("name", Variants.of("item_" + i));
      if (i < 3) {
        item.put("rare", Variants.of(1));
      }
      ValueArray arr = Variants.array();
      arr.add(item);
      ShreddedObject root = Variants.object(rootMeta);
      root.put("items", arr);
      rows.add(root);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();

    GroupType rootTv = schema.asGroupType();
    GroupType itemsGroup = rootTv.getType("items").asGroupType();
    assertThat(itemsGroup.containsField("typed_value")).isTrue();
    GroupType listType = itemsGroup.getType("typed_value").asGroupType();
    GroupType repeatedGroup = listType.getType(0).asGroupType();
    GroupType elementGroup = repeatedGroup.getType(0).asGroupType();
    assertThat(elementGroup.containsField("typed_value")).isTrue();
    GroupType elementFields = elementGroup.getType("typed_value").asGroupType();
    assertThat(elementFields.containsField("name")).isTrue();
    assertThat(elementFields.containsField("rare")).isFalse();
  }

  @Test
  public void testLongArrayInFewRowsSurvivesPruning() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();

    VariantMetadata itemMeta = Variants.metadata("key");

    // 2 of 100 rows have 500-element arrays with {"key": N}. Per-element counting gives
    // observationCount=1000, so key survives the 10% pruning threshold.
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      ValueArray arr = Variants.array();
      if (i < 2) {
        for (int j = 0; j < 500; j++) {
          ShreddedObject item = Variants.object(itemMeta);
          item.put("key", Variants.of(j));
          arr.add(item);
        }
      }
      rows.add(arr);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();

    GroupType listType = schema.asGroupType();
    GroupType repeatedGroup = listType.getType(0).asGroupType();
    GroupType elementGroup = repeatedGroup.getType(0).asGroupType();
    assertThat(elementGroup.containsField("typed_value")).isTrue();
    GroupType elementFields = elementGroup.getType("typed_value").asGroupType();
    assertThat(elementFields.containsField("key")).isTrue();
  }

  @Test
  public void testUuidFieldIsTrackedAndShredded() {
    VariantMetadata meta = Variants.metadata("id");
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("id", Variants.ofUUID(UUID.randomUUID()));
      rows.add(obj);
    }

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);

    assertThat(schema).isNotNull();
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.containsField("id")).isTrue();
    GroupType idGroup = typedValue.getType("id").asGroupType();
    PrimitiveType idTyped = idGroup.getType("typed_value").asPrimitiveType();
    assertThat(idTyped.getLogicalTypeAnnotation())
        .isInstanceOf(LogicalTypeAnnotation.UUIDLogicalTypeAnnotation.class);
  }

  @Test
  public void testMixedPrimitiveTypesFieldNotShredded() {
    VariantMetadata meta = Variants.metadata("mixed", "keep");
    ShreddedObject row1 = Variants.object(meta);
    row1.put("mixed", Variants.of(42));
    row1.put("keep", Variants.of(1));
    ShreddedObject row2 = Variants.object(meta);
    row2.put("mixed", Variants.of("text"));
    row2.put("keep", Variants.of(2));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2), 0);

    assertThat(schema).isNotNull().isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.containsField("mixed")).isFalse();
    assertThat(typedValue.containsField("keep")).isTrue();
  }

  @Test
  public void testIntegerWideningAdmitsField() {
    VariantMetadata meta = Variants.metadata("n");
    ShreddedObject row1 = Variants.object(meta);
    row1.put("n", Variants.of(42));
    ShreddedObject row2 = Variants.object(meta);
    row2.put("n", Variants.of(5_000_000_000L));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2), 0);

    assertThat(schema).isNotNull().isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.containsField("n")).isTrue();
    GroupType nGroup = typedValue.getType("n").asGroupType();
    assertThat(nGroup.getType("typed_value").asPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
  }

  @Test
  public void testDecimalWideningAdmitsField() {
    VariantMetadata meta = Variants.metadata("n");
    ShreddedObject row1 = Variants.object(meta);
    row1.put("n", Variants.of(new BigDecimal("1.5")));
    ShreddedObject row2 = Variants.object(meta);
    row2.put("n", Variants.of(new BigDecimal("9876543210.123")));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2), 0);

    assertThat(schema).isNotNull().isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.containsField("n")).isTrue();
    GroupType nGroup = typedValue.getType("n").asGroupType();
    assertThat(nGroup.getType("typed_value").asPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
  }

  @Test
  public void testShreddedSchemaIsOrderIndependent() {
    VariantMetadata meta = Variants.metadata("n");
    ShreddedObject small = Variants.object(meta);
    small.put("n", Variants.of(42));
    ShreddedObject large = Variants.object(meta);
    large.put("n", Variants.of(5_000_000_000L));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type orderA = analyzer.analyzeAndCreateSchema(List.of(small, large), 0);
    Type orderB = analyzer.analyzeAndCreateSchema(List.of(large, small), 0);

    assertThat(orderA).isNotNull().isEqualTo(orderB);
  }

  @Test
  public void testMixedTypesNotShreddedRegardlessOfOrder() {
    VariantMetadata meta = Variants.metadata("mixed", "keep");
    ShreddedObject intFirst = Variants.object(meta);
    intFirst.put("mixed", Variants.of(42));
    intFirst.put("keep", Variants.of(1));
    ShreddedObject strFirst = Variants.object(meta);
    strFirst.put("mixed", Variants.of("text"));
    strFirst.put("keep", Variants.of(2));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type orderA = analyzer.analyzeAndCreateSchema(List.of(intFirst, strFirst), 0);
    Type orderB = analyzer.analyzeAndCreateSchema(List.of(strFirst, intFirst), 0);

    assertThat(orderA).isNotNull().isEqualTo(orderB);
    assertThat(((GroupType) orderA).containsField("mixed")).isFalse();
    assertThat(((GroupType) orderA).containsField("keep")).isTrue();
  }

  @Test
  public void testAllNullFieldNotShredded() {
    VariantMetadata meta = Variants.metadata("n");
    ShreddedObject row1 = Variants.object(meta);
    row1.put("n", Variants.ofNull());
    ShreddedObject row2 = Variants.object(meta);
    row2.put("n", Variants.ofNull());

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2), 0);

    // A field that is null in every observed row has no type family, so nothing is shredded.
    assertThat(schema).isNull();
  }

  @Test
  public void testFieldWithNullsAndSingleTypeStillShreds() {
    VariantMetadata meta = Variants.metadata("n");
    ShreddedObject row1 = Variants.object(meta);
    row1.put("n", Variants.ofNull());
    ShreddedObject row2 = Variants.object(meta);
    row2.put("n", Variants.of(7));
    ShreddedObject row3 = Variants.object(meta);
    row3.put("n", Variants.of(9));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2, row3), 0);

    assertThat(schema).isNotNull().isInstanceOf(GroupType.class);
    assertThat(((GroupType) schema).containsField("n")).isTrue();
  }

  @Test
  public void testIntAndDecimalAtSameFieldNotShredded() {
    VariantMetadata meta = Variants.metadata("mixed", "keep");
    ShreddedObject row1 = Variants.object(meta);
    row1.put("mixed", Variants.of(42));
    row1.put("keep", Variants.of(1));
    ShreddedObject row2 = Variants.object(meta);
    row2.put("mixed", Variants.of(new BigDecimal("3.14")));
    row2.put("keep", Variants.of(2));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2), 0);

    assertThat(schema).isNotNull().isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.containsField("mixed")).isFalse();
    assertThat(typedValue.containsField("keep")).isTrue();
  }

  @Test
  public void testMixedObjectAndPrimitiveAtFieldNotShredded() {
    VariantMetadata outerMeta = Variants.metadata("mixed", "keep");
    VariantMetadata innerMeta = Variants.metadata("x");
    ShreddedObject inner = Variants.object(innerMeta);
    inner.put("x", Variants.of(1));

    ShreddedObject row1 = Variants.object(outerMeta);
    row1.put("mixed", inner);
    row1.put("keep", Variants.of(1));
    ShreddedObject row2 = Variants.object(outerMeta);
    row2.put("mixed", Variants.of("hello"));
    row2.put("keep", Variants.of(2));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2), 0);

    assertThat(schema).isNotNull().isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.containsField("mixed")).isFalse();
    assertThat(typedValue.containsField("keep")).isTrue();
  }

  @Test
  public void testArrayWithMixedElementTypesNotShredded() {
    VariantMetadata meta = Variants.metadata("arr", "keep");

    ValueArray arr1 = Variants.array();
    arr1.add(Variants.of(1));
    arr1.add(Variants.of(2));
    ShreddedObject row1 = Variants.object(meta);
    row1.put("arr", arr1);
    row1.put("keep", Variants.of(1));

    ValueArray arr2 = Variants.array();
    arr2.add(Variants.of("text"));
    ShreddedObject row2 = Variants.object(meta);
    row2.put("arr", arr2);
    row2.put("keep", Variants.of(2));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2), 0);

    assertThat(schema).isNotNull().isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.containsField("arr")).isFalse();
    assertThat(typedValue.containsField("keep")).isTrue();
  }

  @Test
  public void testMixedFloatAndDoubleNotShredded() {
    VariantMetadata meta = Variants.metadata("mixed", "keep");
    ShreddedObject row1 = Variants.object(meta);
    row1.put("mixed", Variants.of(1.5F));
    row1.put("keep", Variants.of(1));
    ShreddedObject row2 = Variants.object(meta);
    row2.put("mixed", Variants.of(2.5D));
    row2.put("keep", Variants.of(2));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2), 0);

    assertThat(schema).isNotNull().isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.containsField("mixed")).isFalse();
    assertThat(typedValue.containsField("keep")).isTrue();
  }

  @Test
  public void testMixedTimestampTzAndNanosNotShredded() {
    VariantMetadata meta = Variants.metadata("mixed", "keep");
    ShreddedObject row1 = Variants.object(meta);
    row1.put("mixed", Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00"));
    row1.put("keep", Variants.of(1));
    ShreddedObject row2 = Variants.object(meta);
    row2.put("mixed", Variants.ofIsoTimestamptzNanos("2024-11-07T12:33:54.123456789+00:00"));
    row2.put("keep", Variants.of(2));

    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(row1, row2), 0);

    assertThat(schema).isNotNull().isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.containsField("mixed")).isFalse();
    assertThat(typedValue.containsField("keep")).isTrue();
  }

  @Test
  public void testRootLevelMixedTypesReturnsNull() {
    VariantValueShreddingAnalyzer analyzer = new VariantValueShreddingAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(Variants.of(42), Variants.of("text")), 0);

    assertThat(schema).isNull();
  }

  /**
   * Builds 100 variant rows where "common" appears in every row and "rare" appears in only {@code
   * rareCount} rows (below MIN_FIELD_FREQUENCY = 0.10 when rareCount < 10).
   */
  private static List<VariantValue> buildPruningTestRows(
      int rareCount, Function<ShreddedObject, VariantValue> wrap) {
    VariantMetadata meta = Variants.metadata("common", "rare");
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("common", Variants.of(i));
      if (i < rareCount) {
        obj.put("rare", Variants.of("text"));
      }
      rows.add(wrap.apply(obj));
    }
    return rows;
  }

  /** Count typed_value group nesting depth along field "a". */
  private static int countObjectDepth(Type type) {
    int depth = 0;
    Type current = type;
    while (current != null && "typed_value".equals(current.getName()) && !current.isPrimitive()) {
      depth++;
      GroupType group = current.asGroupType();
      if (group.containsField("a")) {
        GroupType fieldGroup = group.getType("a").asGroupType();
        if (fieldGroup.containsField("typed_value")) {
          current = fieldGroup.getType("typed_value");
        } else {
          break;
        }
      } else {
        break;
      }
    }
    return depth;
  }

  /** Count nested array (LIST) levels in the schema. */
  private static int countArrayDepth(Type type) {
    int depth = 0;
    Type current = type;
    while (current != null && !current.isPrimitive()) {
      if (!"typed_value".equals(current.getName())) {
        break;
      }
      GroupType group = current.asGroupType();
      if (!(group.getLogicalTypeAnnotation()
          instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)) {
        break;
      }
      depth++;
      GroupType listGroup = group.getType(0).asGroupType();
      GroupType elementGroup = listGroup.getType(0).asGroupType();
      if (elementGroup.containsField("typed_value")) {
        current = elementGroup.getType("typed_value");
      } else {
        break;
      }
    }
    return depth;
  }
}
