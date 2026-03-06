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
 * <ul>
 *   <li>shred to the most common type
 * </ul>
 */
public class VariantShreddingAnalyzer {
  private static final String TYPED_VALUE = "typed_value";
  private static final String VALUE = "value";
  private static final String ELEMENT = "element";

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
    return buildTypedValue(root, root.info.getMostCommonType());
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
    int maxPrecision = info.maxDecimalPrecision;
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
    private int maxDecimalPrecision = 0;
    private int maxDecimalScale = 0;

    void observe(VariantValue value) {
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
          maxDecimalPrecision = Math.max(maxDecimalPrecision, bd.precision());
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
          if (mostCapableInteger == null || type.ordinal() > mostCapableInteger.ordinal()) {
            mostCapableInteger = type;
          }
        } else if (isDecimalType(type)) {
          decimalTotalCount += count;
          if (mostCapableDecimal == null || type.ordinal() > mostCapableDecimal.ordinal()) {
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
                  .thenComparingInt(entry -> entry.getKey().ordinal()))
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
