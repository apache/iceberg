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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

public abstract class ParquetVariantVisitor<R> {
  static final String METADATA = "metadata";
  static final String VALUE = "value";
  static final String TYPED_VALUE = "typed_value";

  /**
   * Handles the root variant column group.
   *
   * <p>The value and typed_value results are combined by calling {@link #value}.
   *
   * <pre>
   *   group v (VARIANT) { &lt;-- metadata result and combined value and typed_value result
   *     required binary metadata;
   *     optional binary value;
   *     optional ... typed_value;
   *   }
   * </pre>
   */
  public R variant(GroupType variant, R metadataResult, R valueResult) {
    return null;
  }

  /**
   * Handles a serialized variant metadata column.
   *
   * <pre>
   *   group v (VARIANT) {
   *     required binary metadata; &lt;-- this column
   *     optional binary value;
   *     optional ... typed_value;
   *   }
   * </pre>
   */
  public R metadata(PrimitiveType metadata) {
    return null;
  }

  /**
   * Handles a serialized variant value column.
   *
   * <pre>
   *   group variant_value_pair {
   *     optional binary value; &lt;-- this column
   *     optional ... typed_value;
   *   }
   * </pre>
   */
  public R serialized(PrimitiveType value) {
    return null;
  }

  /**
   * Handles a shredded primitive typed_value column.
   *
   * <pre>
   *   group variant_value_pair {
   *     optional binary value;
   *     optional int32 typed_value; &lt;-- this column when it is any primitive
   *   }
   * </pre>
   */
  public R primitive(PrimitiveType primitive) {
    return null;
  }

  /**
   * Handles a variant value result and typed_value result pair.
   *
   * <p>The value and typed_value pair may be nested in an object field, array element, or in the
   * root group of a variant.
   *
   * <p>This method is also called when the typed_value field is missing.
   *
   * <pre>
   *   group variant_value_pair { &lt;-- value result and typed_value result
   *     optional binary value;
   *     optional ... typed_value;
   *   }
   * </pre>
   */
  public R value(GroupType value, R valueResult, R typedResult) {
    return null;
  }

  /**
   * Handles a shredded object value result and a list of field value results.
   *
   * <p>Each field's value and typed_value results are combined by calling {@link #value}.
   *
   * <pre>
   *   group variant_value_pair {  &lt;-- value result and typed_value field results
   *     optional binary value;
   *     optional group typed_value {
   *       required group a {
   *         optional binary value;
   *         optional binary typed_value (UTF8);
   *       }
   *       ...
   *     }
   *   }
   * </pre>
   */
  public R object(GroupType object, R valueResult, List<R> fieldResults) {
    return null;
  }

  /**
   * Handles a shredded array value result and an element value result.
   *
   * <p>The element's value and typed_value results are combined by calling {@link #value}.
   *
   * <pre>
   *   group variant_value_pair {  &lt;-- value result and element result
   *     optional binary value;
   *     optional group typed_value (LIST) {
   *       repeated group list {
   *         required group element {
   *           optional binary value;
   *           optional binary typed_value (UTF8);
   *         }
   *       }
   *     }
   *   }
   * </pre>
   */
  public R array(GroupType array, R valueResult, R elementResult) {
    return null;
  }

  /** Handler called before visiting any primitive or group type. */
  public void beforeField(Type type) {}

  /** Handler called after visiting any primitive or group type. */
  public void afterField(Type type) {}

  public static <R> R visit(GroupType type, ParquetVariantVisitor<R> visitor) {
    Preconditions.checkArgument(
        ParquetSchemaUtil.hasField(type, METADATA), "Invalid variant, missing metadata: %s", type);

    Type metadataType = type.getType(METADATA);
    Preconditions.checkArgument(
        isBinary(metadataType), "Invalid variant metadata, expecting BINARY: %s", metadataType);

    R metadataResult =
        withBeforeAndAfter(
            () -> visitor.metadata(metadataType.asPrimitiveType()), metadataType, visitor);
    R valueResult = visitValue(type, visitor);

    return visitor.variant(type, metadataResult, valueResult);
  }

  private static <R> R visitValue(GroupType valueGroup, ParquetVariantVisitor<R> visitor) {
    R valueResult;
    if (ParquetSchemaUtil.hasField(valueGroup, VALUE)) {
      Type valueType = valueGroup.getType(VALUE);
      Preconditions.checkArgument(
          isBinary(valueType), "Invalid variant value, expecting BINARY: %s", valueType);

      valueResult =
          withBeforeAndAfter(
              () -> visitor.serialized(valueType.asPrimitiveType()), valueType, visitor);
    } else {
      Preconditions.checkArgument(
          ParquetSchemaUtil.hasField(valueGroup, TYPED_VALUE),
          "Invalid variant, missing both value and typed_value: %s",
          valueGroup);

      valueResult = null;
    }

    if (ParquetSchemaUtil.hasField(valueGroup, TYPED_VALUE)) {
      Type typedValueType = valueGroup.getType(TYPED_VALUE);

      if (typedValueType.isPrimitive()) {
        R typedResult =
            withBeforeAndAfter(
                () -> visitor.primitive(typedValueType.asPrimitiveType()), typedValueType, visitor);

        return visitor.value(valueGroup, valueResult, typedResult);

      } else if (typedValueType.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
        R elementResult =
            withBeforeAndAfter(
                () -> visitArray(typedValueType.asGroupType(), visitor), typedValueType, visitor);

        return visitor.array(valueGroup, valueResult, elementResult);

      } else {
        List<R> results =
            withBeforeAndAfter(
                () -> visitObjectFields(typedValueType.asGroupType(), visitor),
                typedValueType,
                visitor);

        return visitor.object(valueGroup, valueResult, results);
      }
    }

    // there was no typed_value field, but the value result must be handled
    return visitor.value(valueGroup, valueResult, null);
  }

  private static <R> R visitArray(GroupType array, ParquetVariantVisitor<R> visitor) {
    Preconditions.checkArgument(
        array.getFieldCount() == 1,
        "Invalid variant array: does not contain single repeated field: %s",
        array);

    Type repeated = array.getFields().get(0);
    Preconditions.checkArgument(
        repeated.isRepetition(Type.Repetition.REPEATED),
        "Invalid variant array: inner group is not repeated");

    // 3-level structure is required; element is always the only child of the repeated field
    return withBeforeAndAfter(
        () -> visitElement(repeated.asGroupType().getType(0), visitor), repeated, visitor);
  }

  private static <R> R visitElement(Type element, ParquetVariantVisitor<R> visitor) {
    return withBeforeAndAfter(() -> visitValue(element.asGroupType(), visitor), element, visitor);
  }

  private static <R> List<R> visitObjectFields(GroupType fields, ParquetVariantVisitor<R> visitor) {
    List<R> results = Lists.newArrayList();
    for (Type fieldType : fields.getFields()) {
      Preconditions.checkArgument(
          !fieldType.isPrimitive(), "Invalid shredded object field, not a group: %s", fieldType);
      R fieldResult =
          withBeforeAndAfter(
              () -> visitValue(fieldType.asGroupType(), visitor), fieldType, visitor);
      results.add(fieldResult);
    }

    return results;
  }

  @FunctionalInterface
  private interface Action<R> {
    R invoke();
  }

  private static <R> R withBeforeAndAfter(
      Action<R> task, Type type, ParquetVariantVisitor<?> visitor) {
    visitor.beforeField(type);
    try {
      return task.invoke();
    } finally {
      visitor.afterField(type);
    }
  }

  private static boolean isBinary(Type type) {
    return type.isPrimitive()
        && type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY;
  }
}
