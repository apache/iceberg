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
import org.apache.iceberg.parquet.ParquetVariantUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes variant data across buffered rows to determine an optimal shredding schema.
 **
 * <ul>
 *   <li>If a field appears consistently with a consistent type → create both {@code value} and
 *       {@code typed_value}
 *   <li>If a field appears with inconsistent types → only create {@code value}
 *   <li>Drop fields that occur in less than the configured threshold of sampled rows
 *   <li>Cap the maximum fields to shred
 * </ul>
 */
public class VariantShreddingAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(VariantShreddingAnalyzer.class);

  private final double minOccurrenceThreshold;
  private final int maxFields;

  /**
   * Creates a new analyzer with the specified configuration.
   *
   * @param minOccurrenceThreshold minimum occurrence threshold (e.g., 0.1 for 10%)
   * @param maxFields maximum number of fields to shred
   */
  public VariantShreddingAnalyzer(double minOccurrenceThreshold, int maxFields) {
    this.minOccurrenceThreshold = minOccurrenceThreshold;
    this.maxFields = maxFields;
  }

  /**
   * Analyzes buffered variant values to determine the optimal shredding schema.
   *
   * @param bufferedRows the buffered rows to analyze
   * @param variantFieldIndex the index of the variant field in the rows
   * @return the shredded schema type, or null if no shredding should be performed
   */
  public Type analyzeAndCreateSchema(List<InternalRow> bufferedRows, int variantFieldIndex) {
    if (bufferedRows.isEmpty()) {
      return null;
    }

    List<VariantValue> variantValues = extractVariantValues(bufferedRows, variantFieldIndex);
    if (variantValues.isEmpty()) {
      return null;
    }

    FieldStats stats = analyzeFields(variantValues);
    return buildShreddedSchema(stats, variantValues.size());
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

  private static FieldStats analyzeFields(List<VariantValue> variantValues) {
    FieldStats stats = new FieldStats();

    for (VariantValue value : variantValues) {
      if (value.type() == PhysicalType.OBJECT) {
        VariantObject obj = value.asObject();
        for (String fieldName : obj.fieldNames()) {
          VariantValue fieldValue = obj.get(fieldName);
          if (fieldValue != null) {
            stats.recordField(fieldName, fieldValue);
          }
        }
      }
    }

    return stats;
  }

  private Type buildShreddedSchema(FieldStats stats, int totalRows) {
    int minOccurrences = (int) Math.ceil(totalRows * minOccurrenceThreshold);

    // Get fields that meet the occurrence threshold
    Set<String> candidateFields = Sets.newTreeSet();
    for (Map.Entry<String, FieldInfo> entry : stats.fieldInfoMap.entrySet()) {
      String fieldName = entry.getKey();
      FieldInfo info = entry.getValue();

      if (info.occurrenceCount >= minOccurrences) {
        candidateFields.add(fieldName);
      } else {
        LOG.debug(
            "Field '{}' appears only {} times out of {} (< {}%), dropping",
            fieldName,
            info.occurrenceCount,
            totalRows,
            (int) (minOccurrenceThreshold * 100));
      }
    }

    if (candidateFields.isEmpty()) {
      return null;
    }

    // Build the typed_value struct with field count limit
    Types.GroupBuilder<GroupType> objectBuilder = Types.buildGroup(Type.Repetition.OPTIONAL);
    int fieldCount = 0;

    for (String fieldName : candidateFields) {
      FieldInfo info = stats.fieldInfoMap.get(fieldName);

      if (info.hasConsistentType()) {
        Type shreddedFieldType = createShreddedFieldType(fieldName, info);
        if (shreddedFieldType != null) {
          if (fieldCount + 2 > maxFields) {
            LOG.debug(
                "Reached maximum field limit ({}) while processing field '{}', stopping",
                maxFields,
                fieldName);
            break;
          }
          objectBuilder.addField(shreddedFieldType);
          fieldCount += 2;
        }
      } else {
        Type valueOnlyField = createValueOnlyField(fieldName);
        if (fieldCount + 1 > maxFields) {
          LOG.debug(
              "Reached maximum field limit ({}) while processing field '{}', stopping",
              maxFields,
              fieldName);
          break;
        }
        objectBuilder.addField(valueOnlyField);
        fieldCount += 1;
        LOG.debug(
            "Field '{}' has inconsistent types ({}), creating value-only field",
            fieldName,
            info.observedTypes);
      }
    }

    if (fieldCount == 0) {
      return null;
    }

    LOG.info("Created shredded schema with {} fields for {} candidate fields", fieldCount, candidateFields.size());
    return objectBuilder.named("typed_value");
  }

  private static Type createShreddedFieldType(String fieldName, FieldInfo info) {
    PhysicalType physicalType = info.getConsistentType();
    if (physicalType == null) {
      return null;
    }

    // For array types, analyze the first value to determine element type
    Type typedValue;
    if (physicalType == PhysicalType.ARRAY) {
      typedValue = createArrayTypedValue(info);
    } else if (physicalType == PhysicalType.DECIMAL4
        || physicalType == PhysicalType.DECIMAL8
        || physicalType == PhysicalType.DECIMAL16) {
      // For decimals, infer precision and scale from actual values
      typedValue = createDecimalTypedValue(info, physicalType);
    } else if (physicalType == PhysicalType.OBJECT) {
      // For nested objects, attempt recursive shredding
      typedValue = createNestedObjectTypedValue(info);
    } else {
      // Convert the physical type to a Parquet type for typed_value
      typedValue = convertPhysicalTypeToParquet(physicalType);
    }

    if (typedValue == null) {
      // If we can't create a typed_value (e.g., inconsistent decimal scales),
      // create a value-only field instead of skipping the field entirely
      return Types.buildGroup(Type.Repetition.REQUIRED)
          .optional(PrimitiveType.PrimitiveTypeName.BINARY)
          .named("value")
          .named(fieldName);
    }

    return Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .addField(typedValue)
        .named(fieldName);
  }

  private static Type createDecimalTypedValue(FieldInfo info, PhysicalType decimalType) {
    // Analyze decimal values to determine precision and scale
    // All values must have the same scale to be considered consistent
    Integer consistentScale = null;
    int maxPrecision = 0;

    for (VariantValue value : info.observedValues) {
      if (value.type() == decimalType) {
        try {
          VariantPrimitive<?> primitive = value.asPrimitive();
          Object decimalValue = primitive.get();
          if (decimalValue instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) decimalValue;
            int precision = bd.precision();
            int scale = bd.scale();
            
            // Check scale consistency
            if (consistentScale == null) {
              consistentScale = scale;
            } else if (consistentScale != scale) {
              // Different scales mean inconsistent types - no typed_value
              LOG.debug(
                  "Decimal values have inconsistent scales ({} vs {}), skipping typed_value",
                  consistentScale,
                  scale);
              return null;
            }
            
            maxPrecision = Math.max(maxPrecision, precision);
          }
        } catch (Exception e) {
          LOG.debug("Failed to analyze decimal value", e);
        }
      }
    }

    if (maxPrecision == 0 || consistentScale == null) {
      LOG.debug("Could not determine decimal precision/scale, skipping typed_value");
      return null;
    }

    // Determine the appropriate Parquet type based on precision
    PrimitiveType.PrimitiveTypeName primitiveType;
    if (maxPrecision <= 9) {
      primitiveType = PrimitiveType.PrimitiveTypeName.INT32;
    } else if (maxPrecision <= 18) {
      primitiveType = PrimitiveType.PrimitiveTypeName.INT64;
    } else {
      primitiveType = PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
    }

    return Types.optional(primitiveType)
        .as(LogicalTypeAnnotation.decimalType(consistentScale, maxPrecision))
        .named("typed_value");
  }

  private static Type createNestedObjectTypedValue(FieldInfo info) {
    // For nested objects, we can recursively analyze their fields
    // For now, we'll create a simpler representation
    // A full implementation would recursively build the object structure

    // Get a sample object to analyze its fields
    for (VariantValue value : info.observedValues) {
      if (value.type() == PhysicalType.OBJECT) {
        try {
          VariantObject obj = value.asObject();
          int numFields = obj.numFields();

          // Only shred simple nested objects (not too many fields)
          if (numFields > 0 && numFields <= 20) {
            // Analyze fields in the nested object
            Map<String, Set<PhysicalType>> nestedFieldTypes = Maps.newHashMap();

            for (String fieldName : obj.fieldNames()) {
              VariantValue fieldValue = obj.get(fieldName);
              if (fieldValue != null) {
                nestedFieldTypes
                    .computeIfAbsent(fieldName, k -> Sets.newHashSet())
                    .add(fieldValue.type());
              }
            }

            // Build nested struct with fields that have consistent types
            Types.GroupBuilder<GroupType> nestedBuilder =
                Types.buildGroup(Type.Repetition.OPTIONAL);
            int fieldCount = 0;

            for (Map.Entry<String, Set<PhysicalType>> entry : nestedFieldTypes.entrySet()) {
              String fieldName = entry.getKey();
              Set<PhysicalType> types = entry.getValue();

              // Only include fields with consistent types
              if (types.size() == 1) {
                PhysicalType fieldType = types.iterator().next();
                Type fieldParquetType = convertPhysicalTypeToParquet(fieldType);
                if (fieldParquetType != null) {
                  GroupType nestedField =
                      Types.buildGroup(Type.Repetition.REQUIRED)
                          .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                          .named("value")
                          .addField(fieldParquetType)
                          .named(fieldName);
                  nestedBuilder.addField(nestedField);
                  fieldCount++;
                }
              }
            }

            if (fieldCount > 0) {
              return nestedBuilder.named("typed_value");
            }
          }
        } catch (Exception e) {
          LOG.debug("Failed to analyze nested object", e);
        }
        break;
      }
    }

    LOG.debug("Skipping nested object - complex structure or analysis failed");
    return null;
  }

  private static Type createArrayTypedValue(FieldInfo info) {
    // Get a sample array value to analyze element types
    for (VariantValue value : info.observedValues) {
      if (value.type() == PhysicalType.ARRAY) {
        try {
          VariantArray array = value.asArray();
          int numElements = array.numElements();
          if (numElements > 0) {
            // Analyze elements to determine if they have consistent type
            Set<PhysicalType> elementTypes = Sets.newHashSet();
            for (int i = 0; i < numElements; i++) {
              elementTypes.add(array.get(i).type());
            }

            // If all elements have consistent type, create typed array
            if (elementTypes.size() == 1
                || (elementTypes.size() == 2
                    && elementTypes.contains(PhysicalType.BOOLEAN_TRUE)
                    && elementTypes.contains(PhysicalType.BOOLEAN_FALSE))) {
              PhysicalType elementType = elementTypes.iterator().next();
              if (elementType == PhysicalType.BOOLEAN_FALSE
                  || elementType == PhysicalType.BOOLEAN_TRUE) {
                elementType = PhysicalType.BOOLEAN_TRUE;
              }
              Type elementParquetType = convertPhysicalTypeToParquet(elementType);
              if (elementParquetType != null) {
                // Create list with typed element
                GroupType element =
                    Types.buildGroup(Type.Repetition.REQUIRED)
                        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .named("value")
                        .addField(elementParquetType)
                        .named("element");
                return Types.optionalList().element(element).named("typed_value");
              }
            }
          }
        } catch (Exception e) {
          LOG.debug("Failed to analyze array elements", e);
        }
        break;
      }
    }
    return null;
  }

  private static Type createValueOnlyField(String fieldName) {
    // Create a field with only the value field (no typed_value)
    return Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .named(fieldName);
  }

  private static Type convertPhysicalTypeToParquet(PhysicalType physicalType) {
    switch (physicalType) {
      case BOOLEAN_TRUE:
      case BOOLEAN_FALSE:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("typed_value");

      case INT8:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(8, true))
            .named("typed_value");

      case INT16:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(16, true))
            .named("typed_value");

      case INT32:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, true))
            .named("typed_value");

      case INT64:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("typed_value");

      case FLOAT:
        return Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named("typed_value");

      case DOUBLE:
        return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("typed_value");

      case STRING:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("typed_value");

      case BINARY:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("typed_value");

      case DATE:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.dateType())
            .named("typed_value");

      case TIMESTAMPTZ:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("typed_value");

      case TIMESTAMPNTZ:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("typed_value");

      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        // Decimals are now handled in createDecimalTypedValue()
        // This case should not be reached for consistent decimal types
        LOG.debug("Decimal type {} should be handled by createDecimalTypedValue()", physicalType);
        return null;

      case ARRAY:
        // Arrays are now handled in createArrayTypedValue()
        LOG.debug("Array type should be handled by createArrayTypedValue()");
        return null;

      case OBJECT:
        // Nested objects are now handled in createNestedObjectTypedValue()
        LOG.debug("Object type should be handled by createNestedObjectTypedValue()");
        return null;

      default:
        LOG.debug("Unknown physical type: {}", physicalType);
        return null;
    }
  }

  /** Tracks statistics about fields across multiple variant values. */
  private static class FieldStats {
    private final Map<String, FieldInfo> fieldInfoMap = Maps.newHashMap();

    void recordField(String fieldName, VariantValue value) {
      FieldInfo info = fieldInfoMap.computeIfAbsent(fieldName, k -> new FieldInfo());
      info.observe(value);
    }
  }

  /** Tracks occurrence count and type consistency for a single field. */
  private static class FieldInfo {
    private int occurrenceCount = 0;
    private final Set<PhysicalType> observedTypes = Sets.newHashSet();
    private final List<VariantValue> observedValues = new java.util.ArrayList<>();

    void observe(VariantValue value) {
      occurrenceCount++;
      observedTypes.add(value.type());
      observedValues.add(value);
    }

    boolean hasConsistentType() {
      // Handle boolean types specially - both TRUE and FALSE map to BOOLEAN
      if (observedTypes.size() == 2
          && observedTypes.contains(PhysicalType.BOOLEAN_TRUE)
          && observedTypes.contains(PhysicalType.BOOLEAN_FALSE)) {
        return true;
      }
      return observedTypes.size() == 1;
    }

    PhysicalType getConsistentType() {
      if (!hasConsistentType()) {
        return null;
      }

      // Handle boolean types
      if (observedTypes.contains(PhysicalType.BOOLEAN_TRUE)
          || observedTypes.contains(PhysicalType.BOOLEAN_FALSE)) {
        return PhysicalType.BOOLEAN_TRUE; // Use TRUE as canonical boolean type
      }

      return observedTypes.iterator().next();
    }
  }
}

