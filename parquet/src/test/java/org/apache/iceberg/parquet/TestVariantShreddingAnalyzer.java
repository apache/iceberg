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
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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

public class TestVariantShreddingAnalyzer {

  private static class DirectAnalyzer extends VariantShreddingAnalyzer<VariantValue, Void> {
    @Override
    protected List<VariantValue> extractVariantValues(List<VariantValue> rows, int idx) {
      return rows;
    }

    @Override
    protected int resolveColumnIndex(Void engineSchema, String columnName) {
      throw new UnsupportedOperationException("Not used in direct tests");
    }
  }

  @Test
  public void testDepthLimitStopsObjectRecursion() {
    DirectAnalyzer analyzer = new DirectAnalyzer();

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
    DirectAnalyzer analyzer = new DirectAnalyzer();

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
    DirectAnalyzer analyzer = new DirectAnalyzer();

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

    DirectAnalyzer analyzer = new DirectAnalyzer();
    Type schema = analyzer.analyzeAndCreateSchema(List.of(obj), 0);

    assertThat(schema).isNotNull();
    assertThat(schema).isInstanceOf(GroupType.class);
    GroupType typedValue = (GroupType) schema;
    assertThat(typedValue.getFieldCount()).isLessThanOrEqualTo(300).isGreaterThan(0);
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
      row2.put(fieldNames[i], Variants.of("text"));
    }

    ShreddedObject row3 = Variants.object(meta);
    for (int i = 0; i < 10; i++) {
      row3.put(fieldNames[i], Variants.of(99));
    }

    DirectAnalyzer analyzer = new DirectAnalyzer();
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

    DirectAnalyzer analyzer = new DirectAnalyzer();
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
    DirectAnalyzer analyzer = new DirectAnalyzer();
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

  @Test
  public void testDecimalForExactPrecision() {
    DirectAnalyzer analyzer = new DirectAnalyzer();

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
  }

  @Test
  public void testInfrequentFieldsArePruned() {
    DirectAnalyzer analyzer = new DirectAnalyzer();

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
    DirectAnalyzer analyzer = new DirectAnalyzer();

    // All rows are empty arrays, no element type to infer
    List<VariantValue> rows = List.of(Variants.array(), Variants.array(), Variants.array());

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNull();
  }

  @Test
  public void testRootPrimitiveProducesTypedValue() {
    DirectAnalyzer analyzer = new DirectAnalyzer();

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
    DirectAnalyzer analyzer = new DirectAnalyzer();

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
    DirectAnalyzer analyzer = new DirectAnalyzer();

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
    DirectAnalyzer analyzer = new DirectAnalyzer();

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
      } else {
        arr.add(Variants.of("no_key"));
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
