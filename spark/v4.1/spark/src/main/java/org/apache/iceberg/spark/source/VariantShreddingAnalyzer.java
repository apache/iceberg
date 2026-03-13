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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.VariantVal;

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
 *   <li>Fields below MIN_FIELD_FREQUENCY frequency are pruned. Above MAX_SHREDDED_FIELDS fields,
 *       the most frequent are kept with alphabetical tie-breaking.
 * </ul>
 *
 * <p>This contract holds within a single batch. Different batches with different distributions may
 * produce different layouts; cross-batch stability requires schema pinning (not yet implemented).
 */
public class VariantShreddingAnalyzer {
  private static final String TYPED_VALUE = "typed_value";
  private static final String VALUE = "value";
  private static final String ELEMENT = "element";
  private static final double MIN_FIELD_FREQUENCY = 0.10;
  private static final int MAX_SHREDDED_FIELDS = 300;

  public VariantShreddingAnalyzer() {}

  /**
   * Analyzes buffered variant values to determine the optimal shredding schema.
   *
   * @param bufferedRows the buffered rows to analyze
   * @param variantFieldIndex the index of the variant field in the rows
   * @return the shredded schema type, or null if no shredding should be performed
   */
  public Type analyzeAndCreateSchema(List<InternalRow> bufferedRows, int variantFieldIndex) {
    List<VariantValue> variantValues = extractVariantValues(bufferedRows, variantFieldIndex);
    if (variantValues.isEmpty()) {
      return null;
    }

    PathNode root = buildPathTree(variantValues);
    PhysicalType rootType = root.info.getMostCommonType();
    if (rootType == null) {
      return null;
    }

    if (rootType == PhysicalType.OBJECT) {
      pruneInfrequentFields(root, variantValues.size());
    }

    return buildTypedValue(root, rootType);
  }

  private static List<VariantValue> extractVariantValues(
      List<InternalRow> bufferedRows, int variantFieldIndex) {
    List<VariantValue> values = new java.util.ArrayList<>();

    for (InternalRow row : bufferedRows) {
      if (!row.isNullAt(variantFieldIndex)) {
        VariantVal variantVal = row.getVariant(variantFieldIndex);
        if (variantVal != null) {
          VariantValue variantValue =
              VariantValue.from(
                  VariantMetadata.from(
                      ByteBuffer.wrap(variantVal.getMetadata()).order(ByteOrder.LITTLE_ENDIAN)),
                  ByteBuffer.wrap(variantVal.getValue()).order(ByteOrder.LITTLE_ENDIAN));
          values.add(variantValue);
        }
      }
    }

    return values;
  }

  private static PathNode buildPathTree(List<VariantValue> variantValues) {
    PathNode root = new PathNode(null);
    root.info = new FieldInfo();

    for (VariantValue value : variantValues) {
      traverse(root, value);
    }

    return root;
  }

  private static void pruneInfrequentFields(PathNode node, int totalRows) {
    if (node.objectChildren.isEmpty()) {
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
      List<Map.Entry<String, PathNode>> sorted =
          new java.util.ArrayList<>(node.objectChildren.entrySet());
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

    // Recurse into remaining children
    for (PathNode child : node.objectChildren.values()) {
      pruneInfrequentFields(child, totalRows);
    }
  }

  private static void traverse(PathNode node, VariantValue value) {
    if (value == null || value.type() == PhysicalType.NULL) {
      return;
    }

    node.info.observe(value);

    if (value.type() == PhysicalType.OBJECT) {
      VariantObject obj = value.asObject();
      for (String fieldName : obj.fieldNames()) {
        VariantValue fieldValue = obj.get(fieldName);
        if (fieldValue != null) {
          PathNode childNode = node.objectChildren.computeIfAbsent(fieldName, PathNode::new);
          if (childNode.info == null) {
            childNode.info = new FieldInfo();
          }
          traverse(childNode, fieldValue);
        }
      }
    } else if (value.type() == PhysicalType.ARRAY) {
      VariantArray array = value.asArray();
      int numElements = array.numElements();
      if (node.arrayElement == null) {
        node.arrayElement = new PathNode(null);
        node.arrayElement.info = new FieldInfo();
      }
      for (int i = 0; i < numElements; i++) {
        VariantValue element = array.get(i);
        if (element != null) {
          traverse(node.arrayElement, element);
        }
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
    Type typedValue;
    if (physicalType == PhysicalType.ARRAY) {
      typedValue = createArrayTypedValue(node);
    } else if (physicalType == PhysicalType.OBJECT) {
      typedValue = createObjectTypedValue(node);
    } else {
      typedValue = createPrimitiveTypedValue(node.info, physicalType);
    }

    return typedValue;
  }

  private static Type createObjectTypedValue(PathNode node) {
    if (node.objectChildren.isEmpty()) {
      return null;
    }

    Types.GroupBuilder<GroupType> builder = Types.buildGroup(Type.Repetition.OPTIONAL);
    for (PathNode child : node.objectChildren.values()) {
      Type fieldType = buildFieldGroup(child);
      if (fieldType == null) {
        continue;
      }

      builder.addField(fieldType);
    }

    return builder.named(TYPED_VALUE);
  }

  private static Type createArrayTypedValue(PathNode node) {
    PathNode elementNode = node.arrayElement;
    PhysicalType elementType = elementNode.info.getMostCommonType();
    if (elementType == null) {
      return null;
    }
    Type elementTypedValue = buildTypedValue(elementNode, elementType);

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
    int maxPrecision = info.maxDecimalIntegerDigits + info.maxDecimalScale;
    int maxScale = info.maxDecimalScale;

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
      default ->
          throw new UnsupportedOperationException(
              "Unknown primitive physical type: " + primitiveType);
    };
  }

  /** Tracks occurrence count and types for a single field. */
  private static class FieldInfo {
    private final Set<PhysicalType> observedTypes = Sets.newHashSet();
    private final Map<PhysicalType, Integer> typeCounts = Maps.newHashMap();
    private int maxDecimalScale = 0;
    private int maxDecimalIntegerDigits = 0;
    private int observationCount = 0;

    private static final Map<PhysicalType, Integer> INTEGER_PRIORITY =
        Map.of(
            PhysicalType.INT8, 0,
            PhysicalType.INT16, 1,
            PhysicalType.INT32, 2,
            PhysicalType.INT64, 3);

    private static final Map<PhysicalType, Integer> DECIMAL_PRIORITY =
        Map.of(
            PhysicalType.DECIMAL4, 0,
            PhysicalType.DECIMAL8, 1,
            PhysicalType.DECIMAL16, 2);

    private static final Map<PhysicalType, Integer> TIE_BREAK_PRIORITY =
        Map.ofEntries(
            Map.entry(PhysicalType.BOOLEAN_TRUE, 0),
            Map.entry(PhysicalType.INT8, 1),
            Map.entry(PhysicalType.INT16, 2),
            Map.entry(PhysicalType.INT32, 3),
            Map.entry(PhysicalType.INT64, 4),
            Map.entry(PhysicalType.FLOAT, 5),
            Map.entry(PhysicalType.DOUBLE, 6),
            Map.entry(PhysicalType.DECIMAL4, 7),
            Map.entry(PhysicalType.DECIMAL8, 8),
            Map.entry(PhysicalType.DECIMAL16, 9),
            Map.entry(PhysicalType.DATE, 10),
            Map.entry(PhysicalType.TIME, 11),
            Map.entry(PhysicalType.TIMESTAMPTZ, 12),
            Map.entry(PhysicalType.TIMESTAMPNTZ, 13),
            Map.entry(PhysicalType.BINARY, 14),
            Map.entry(PhysicalType.STRING, 15));

    void observe(VariantValue value) {
      observationCount++;
      // Use BOOLEAN_TRUE for both TRUE/FALSE values
      PhysicalType type =
          value.type() == PhysicalType.BOOLEAN_FALSE ? PhysicalType.BOOLEAN_TRUE : value.type();

      observedTypes.add(type);
      typeCounts.compute(type, (k, v) -> (v == null) ? 1 : v + 1);

      // Track max precision and scale for decimal types
      if (type == PhysicalType.DECIMAL4
          || type == PhysicalType.DECIMAL8
          || type == PhysicalType.DECIMAL16) {
        VariantPrimitive<?> primitive = value.asPrimitive();
        Object decimalValue = primitive.get();
        if (decimalValue instanceof BigDecimal) {
          BigDecimal bd = (BigDecimal) decimalValue;
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

    private boolean isIntegerType(PhysicalType type) {
      return type == PhysicalType.INT8
          || type == PhysicalType.INT16
          || type == PhysicalType.INT32
          || type == PhysicalType.INT64;
    }

    private boolean isDecimalType(PhysicalType type) {
      return type == PhysicalType.DECIMAL4
          || type == PhysicalType.DECIMAL8
          || type == PhysicalType.DECIMAL16;
    }
  }
}
