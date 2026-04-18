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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * Analyzes variant data across buffered rows to determine an optimal shredding schema.
 *
 * <p>Determinism contract: for a given set of variant values (regardless of row arrival order),
 * this analyzer produces the same shredded schema.
 *
 * <ul>
 *   <li>Object fields use a TreeMap, so field ordering is alphabetical and deterministic.
 *   <li>Type selection picks the most common type with explicit tie-break priority (see
 *       TIE_BREAK_PRIORITY), not enum ordinal.
 *   <li>Integer types (INT8/16/32/64) and decimal types (DECIMAL4/8/16) are each promoted to the
 *       widest observed before competing with other types.
 *   <li>Fields below {@code MIN_FIELD_FREQUENCY} are pruned. Above {@code MAX_SHREDDED_FIELDS}, the
 *       most frequent are kept with alphabetical tie-breaking.
 *   <li>Recursion into nested objects/arrays stops at {@code MAX_SHREDDING_DEPTH} (default 50).
 *   <li>New struct fields are not tracked once a node reaches {@code MAX_INTERMEDIATE_FIELDS}
 *       (default 1000) to bound memory during inference.
 * </ul>
 *
 * <p>This contract holds within a single batch. Different batches with different distributions may
 * produce different layouts; cross-batch stability requires schema pinning (not yet implemented).
 *
 * <p>Subclasses implement {@link #extractVariantValues} to convert engine-specific row types into
 * {@link VariantValue} instances.
 *
 * @param <T> the engine-specific row type (e.g., Spark InternalRow, Flink RowData)
 * @param <S> the engine-specific schema type (e.g., Spark StructType, Flink RowType)
 */
public abstract class VariantShreddingAnalyzer<T, S> {
  private static final String TYPED_VALUE = "typed_value";
  private static final String VALUE = "value";
  private static final String ELEMENT = "element";
  private static final double MIN_FIELD_FREQUENCY = 0.10;
  private static final int MAX_SHREDDED_FIELDS = 300;
  private static final int MAX_SHREDDING_DEPTH = 50;
  private static final int MAX_INTERMEDIATE_FIELDS = 1000;

  protected VariantShreddingAnalyzer() {}

  /**
   * Analyzes buffered variant values to determine the optimal shredding schema.
   *
   * @param bufferedRows the buffered rows to analyze
   * @param variantFieldIndex the index of the variant field in the rows
   * @return the shredded schema type, or null if no shredding should be performed
   */
  public Type analyzeAndCreateSchema(List<T> bufferedRows, int variantFieldIndex) {
    List<VariantValue> variantValues = extractVariantValues(bufferedRows, variantFieldIndex);
    if (variantValues.isEmpty()) {
      return null;
    }

    PathNode root = buildPathTree(variantValues);
    PhysicalType rootType = root.info.getMostCommonType();
    if (rootType == null) {
      return null;
    }

    pruneInfrequentFields(root, root.info.observationCount);

    return buildTypedValue(root, rootType);
  }

  protected abstract List<VariantValue> extractVariantValues(
      List<T> bufferedRows, int variantFieldIndex);

  /**
   * Resolves a column name to its index in the engine-specific schema. Returns -1 if the column is
   * not found.
   */
  protected abstract int resolveColumnIndex(S engineSchema, String columnName);

  /**
   * Analyzes all variant columns in the schema, resolving column indices via the engine-specific
   * {@link #resolveColumnIndex} method.
   *
   * @param bufferedRows the buffered rows to analyze
   * @param icebergSchema the Iceberg table schema
   * @param engineSchema the engine-specific schema used to resolve column indices
   * @return a map from Iceberg field ID to the shredded Parquet type for each variant column
   */
  public Map<Integer, Type> analyzeVariantColumns(
      List<T> bufferedRows, Schema icebergSchema, S engineSchema) {
    Map<Integer, Type> shreddedTypes = Maps.newHashMap();
    for (NestedField col : icebergSchema.columns()) {
      if (col.type().isVariantType()) {
        int rowIndex = resolveColumnIndex(engineSchema, col.name());
        if (rowIndex >= 0) {
          Type typed = analyzeAndCreateSchema(bufferedRows, rowIndex);
          if (typed != null) {
            shreddedTypes.put(col.fieldId(), typed);
          }
        }
      }
    }

    return shreddedTypes;
  }

  private static PathNode buildPathTree(List<VariantValue> variantValues) {
    PathNode root = new PathNode(null);
    root.info = new FieldInfo();

    for (VariantValue value : variantValues) {
      traverse(root, value, 0);
    }

    return root;
  }

  private static void pruneInfrequentFields(PathNode node, int totalRows) {
    if (node.objectChildren.isEmpty() && node.arrayElement == null) {
      return;
    }

    // Remove fields below frequency threshold
    node.objectChildren
        .entrySet()
        .removeIf(
            entry -> {
              FieldInfo info = entry.getValue().info;
              return info != null
                  && ((double) info.observationCount / totalRows) < MIN_FIELD_FREQUENCY;
            });

    // Cap at MAX_SHREDDED_FIELDS, keep the most frequently observed
    if (node.objectChildren.size() > MAX_SHREDDED_FIELDS) {
      List<Map.Entry<String, PathNode>> sorted = Lists.newArrayList(node.objectChildren.entrySet());
      sorted.sort(
          (a, b) -> {
            int cmp =
                Integer.compare(
                    b.getValue().info.observationCount, a.getValue().info.observationCount);
            return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
          });
      Set<String> keep = Sets.newHashSet();
      for (int i = 0; i < MAX_SHREDDED_FIELDS; i++) {
        keep.add(sorted.get(i).getKey());
      }
      node.objectChildren.entrySet().removeIf(entry -> !keep.contains(entry.getKey()));
    }

    // Recurse into remaining object children
    for (PathNode child : node.objectChildren.values()) {
      pruneInfrequentFields(child, totalRows);
    }

    // Recurse into array elements (arrays of objects need pruning too)
    if (node.arrayElement != null) {
      pruneInfrequentFields(node.arrayElement, totalRows);
    }
  }

  private static void traverse(PathNode node, VariantValue value, int depth) {
    if (value == null || value.type() == PhysicalType.NULL) {
      return;
    }

    node.info.observe(value);

    if (value.type() == PhysicalType.OBJECT && depth < MAX_SHREDDING_DEPTH) {
      traverseObject(node, value.asObject(), depth);
    } else if (value.type() == PhysicalType.ARRAY && depth < MAX_SHREDDING_DEPTH) {
      traverseArray(node, value.asArray(), depth);
    }
  }

  private static void traverseObject(PathNode node, VariantObject obj, int depth) {
    for (String fieldName : obj.fieldNames()) {
      VariantValue fieldValue = obj.get(fieldName);
      if (fieldValue != null) {
        PathNode childNode = node.objectChildren.get(fieldName);
        if (childNode == null) {
          if (node.objectChildren.size() >= MAX_INTERMEDIATE_FIELDS) {
            continue;
          }
          childNode = new PathNode(fieldName);
          childNode.info = new FieldInfo();
          node.objectChildren.put(fieldName, childNode);
        }
        traverse(childNode, fieldValue, depth + 1);
      }
    }
  }

  private static void traverseArray(PathNode node, VariantArray array, int depth) {
    int numElements = array.numElements();
    if (node.arrayElement == null) {
      node.arrayElement = new PathNode(null);
      node.arrayElement.info = new FieldInfo();
    }
    for (int i = 0; i < numElements; i++) {
      VariantValue element = array.get(i);
      if (element != null) {
        traverse(node.arrayElement, element, depth + 1);
      }
    }
  }

  private static Type buildFieldGroup(PathNode node) {
    PhysicalType commonType = node.info.getMostCommonType();
    if (commonType == null) {
      return null;
    }

    Type typedValue = buildTypedValue(node, commonType);
    if (typedValue == null) {
      return null;
    }

    return Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named(VALUE)
        .addField(typedValue)
        .named(node.fieldName);
  }

  private static Type buildTypedValue(PathNode node, PhysicalType physicalType) {
    return switch (physicalType) {
      case ARRAY -> createArrayTypedValue(node);
      case OBJECT -> createObjectTypedValue(node);
      default -> createPrimitiveTypedValue(node.info, physicalType);
    };
  }

  private static Type createObjectTypedValue(PathNode node) {
    if (node.objectChildren.isEmpty()) {
      return null;
    }

    Types.GroupBuilder<GroupType> builder = Types.buildGroup(Type.Repetition.OPTIONAL);
    boolean hasFields = false;
    for (PathNode child : node.objectChildren.values()) {
      Type fieldType = buildFieldGroup(child);
      if (fieldType != null) {
        builder.addField(fieldType);
        hasFields = true;
      }
    }

    return hasFields ? builder.named(TYPED_VALUE) : null;
  }

  private static Type createArrayTypedValue(PathNode node) {
    PathNode elementNode = node.arrayElement;
    if (elementNode == null) {
      return null;
    }
    PhysicalType elementType = elementNode.info.getMostCommonType();
    if (elementType == null) {
      return null;
    }
    Type elementTypedValue = buildTypedValue(elementNode, elementType);
    if (elementTypedValue == null) {
      return null;
    }

    GroupType elementGroup =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .named(VALUE)
            .addField(elementTypedValue)
            .named(ELEMENT);

    return Types.optionalList().element(elementGroup).named(TYPED_VALUE);
  }

  private static class PathNode {
    private final String fieldName;
    private final Map<String, PathNode> objectChildren = Maps.newTreeMap();
    private PathNode arrayElement = null;
    private FieldInfo info = null;

    private PathNode(String fieldName) {
      this.fieldName = fieldName;
    }
  }

  /** Use DECIMAL with maximum precision and scale as the shredding type */
  private static Type createDecimalTypedValue(FieldInfo info) {
    int maxPrecision = Math.min(info.maxDecimalIntegerDigits + info.maxDecimalScale, 38);
    int maxScale = Math.min(info.maxDecimalScale, Math.max(0, 38 - info.maxDecimalIntegerDigits));

    if (maxPrecision <= 9) {
      return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
          .as(LogicalTypeAnnotation.decimalType(maxScale, maxPrecision))
          .named(TYPED_VALUE);
    } else if (maxPrecision <= 18) {
      return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
          .as(LogicalTypeAnnotation.decimalType(maxScale, maxPrecision))
          .named(TYPED_VALUE);
    } else {
      return Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
          .length(16)
          .as(LogicalTypeAnnotation.decimalType(maxScale, maxPrecision))
          .named(TYPED_VALUE);
    }
  }

  private static Type createPrimitiveTypedValue(FieldInfo info, PhysicalType primitiveType) {
    return switch (primitiveType) {
      case BOOLEAN_TRUE, BOOLEAN_FALSE ->
          Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(TYPED_VALUE);
      case INT8 ->
          Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
              .as(LogicalTypeAnnotation.intType(8, true))
              .named(TYPED_VALUE);
      case INT16 ->
          Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
              .as(LogicalTypeAnnotation.intType(16, true))
              .named(TYPED_VALUE);
      case INT32 ->
          Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
              .as(LogicalTypeAnnotation.intType(32, true))
              .named(TYPED_VALUE);
      case INT64 ->
          Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
              .as(LogicalTypeAnnotation.intType(64, true))
              .named(TYPED_VALUE);
      case FLOAT -> Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(TYPED_VALUE);
      case DOUBLE -> Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(TYPED_VALUE);
      case STRING ->
          Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
              .as(LogicalTypeAnnotation.stringType())
              .named(TYPED_VALUE);
      case BINARY -> Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(TYPED_VALUE);
      case TIME ->
          Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
              .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
              .named(TYPED_VALUE);
      case DATE ->
          Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
              .as(LogicalTypeAnnotation.dateType())
              .named(TYPED_VALUE);
      case TIMESTAMPTZ ->
          Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
              .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
              .named(TYPED_VALUE);
      case TIMESTAMPNTZ ->
          Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
              .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
              .named(TYPED_VALUE);
      case TIMESTAMPTZ_NANOS ->
          Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
              .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
              .named(TYPED_VALUE);
      case TIMESTAMPNTZ_NANOS ->
          Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
              .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
              .named(TYPED_VALUE);
      case DECIMAL4, DECIMAL8, DECIMAL16 -> createDecimalTypedValue(info);
      case UUID ->
          Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
              .length(16)
              .as(LogicalTypeAnnotation.uuidType())
              .named(TYPED_VALUE);
      default ->
          throw new UnsupportedOperationException(
              "Unknown primitive physical type: " + primitiveType);
    };
  }

  /** Tracks occurrence count and types for a single field. */
  private static class FieldInfo {
    private final Map<PhysicalType, Integer> typeCounts = Maps.newHashMap();
    private int maxDecimalScale = 0;
    private int maxDecimalIntegerDigits = 0;
    private int observationCount = 0;

    private static final Map<PhysicalType, Integer> INTEGER_PRIORITY =
        ImmutableMap.of(
            PhysicalType.INT8, 0,
            PhysicalType.INT16, 1,
            PhysicalType.INT32, 2,
            PhysicalType.INT64, 3);

    private static final Map<PhysicalType, Integer> DECIMAL_PRIORITY =
        ImmutableMap.of(
            PhysicalType.DECIMAL4, 0,
            PhysicalType.DECIMAL8, 1,
            PhysicalType.DECIMAL16, 2);

    private static final Map<PhysicalType, Integer> TIE_BREAK_PRIORITY =
        ImmutableMap.<PhysicalType, Integer>builder()
            .put(PhysicalType.BOOLEAN_TRUE, 0)
            .put(PhysicalType.INT8, 1)
            .put(PhysicalType.INT16, 2)
            .put(PhysicalType.INT32, 3)
            .put(PhysicalType.INT64, 4)
            .put(PhysicalType.FLOAT, 5)
            .put(PhysicalType.DOUBLE, 6)
            .put(PhysicalType.DECIMAL4, 7)
            .put(PhysicalType.DECIMAL8, 8)
            .put(PhysicalType.DECIMAL16, 9)
            .put(PhysicalType.DATE, 10)
            .put(PhysicalType.TIME, 11)
            .put(PhysicalType.TIMESTAMPTZ, 12)
            .put(PhysicalType.TIMESTAMPNTZ, 13)
            .put(PhysicalType.BINARY, 14)
            .put(PhysicalType.STRING, 15)
            .put(PhysicalType.TIMESTAMPTZ_NANOS, 16)
            .put(PhysicalType.TIMESTAMPNTZ_NANOS, 17)
            .put(PhysicalType.UUID, 18)
            .buildOrThrow();

    void observe(VariantValue value) {
      observationCount++;
      // Use BOOLEAN_TRUE for both TRUE/FALSE values
      PhysicalType type =
          value.type() == PhysicalType.BOOLEAN_FALSE ? PhysicalType.BOOLEAN_TRUE : value.type();

      typeCounts.compute(type, (k, v) -> (v == null) ? 1 : v + 1);

      // Track max precision and scale for decimal types
      if (type == PhysicalType.DECIMAL4
          || type == PhysicalType.DECIMAL8
          || type == PhysicalType.DECIMAL16) {
        VariantPrimitive<?> primitive = value.asPrimitive();
        Object decimalValue = primitive.get();
        if (decimalValue instanceof BigDecimal bd) {
          maxDecimalIntegerDigits = Math.max(maxDecimalIntegerDigits, bd.precision() - bd.scale());
          maxDecimalScale = Math.max(maxDecimalScale, bd.scale());
        }
      }
    }

    PhysicalType getMostCommonType() {
      Map<PhysicalType, Integer> combinedCounts = Maps.newHashMap();

      int integerTotalCount = 0;
      PhysicalType mostCapableInteger = null;

      int decimalTotalCount = 0;
      PhysicalType mostCapableDecimal = null;

      for (Map.Entry<PhysicalType, Integer> entry : typeCounts.entrySet()) {
        PhysicalType type = entry.getKey();
        int count = entry.getValue();

        if (isIntegerType(type)) {
          integerTotalCount += count;
          if (mostCapableInteger == null
              || INTEGER_PRIORITY.get(type) > INTEGER_PRIORITY.get(mostCapableInteger)) {
            mostCapableInteger = type;
          }
        } else if (isDecimalType(type)) {
          decimalTotalCount += count;
          if (mostCapableDecimal == null
              || DECIMAL_PRIORITY.get(type) > DECIMAL_PRIORITY.get(mostCapableDecimal)) {
            mostCapableDecimal = type;
          }
        } else {
          combinedCounts.put(type, count);
        }
      }

      if (mostCapableInteger != null) {
        combinedCounts.put(mostCapableInteger, integerTotalCount);
      }

      if (mostCapableDecimal != null) {
        combinedCounts.put(mostCapableDecimal, decimalTotalCount);
      }

      // Pick the most common type with tie-breaking
      return combinedCounts.entrySet().stream()
          .max(
              Map.Entry.<PhysicalType, Integer>comparingByValue()
                  .thenComparingInt(entry -> TIE_BREAK_PRIORITY.getOrDefault(entry.getKey(), -1)))
          .map(Map.Entry::getKey)
          .orElse(null);
    }

    private static boolean isIntegerType(PhysicalType type) {
      return type == PhysicalType.INT8
          || type == PhysicalType.INT16
          || type == PhysicalType.INT32
          || type == PhysicalType.INT64;
    }

    private static boolean isDecimalType(PhysicalType type) {
      return type == PhysicalType.DECIMAL4
          || type == PhysicalType.DECIMAL8
          || type == PhysicalType.DECIMAL16;
    }
  }
}
