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
package org.apache.iceberg.arrow;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

public class ArrowSchemaUtil {
  private static final String ORIGINAL_TYPE = "originalType";
  private static final String MAP_TYPE = "mapType";

  private ArrowSchemaUtil() {}

  /**
   * Convert Iceberg schema to Arrow Schema.
   *
   * @param schema iceberg schema
   * @return arrow schema
   */
  public static Schema convert(final org.apache.iceberg.Schema schema) {
    ImmutableList.Builder<Field> fields = ImmutableList.builder();

    for (NestedField field : schema.columns()) {
      fields.add(TypeUtil.visit(field.type(), new IcebergToArrowTypeConverter(field)));
    }

    return new Schema(fields.build());
  }

  public static Field convert(final NestedField field) {
    return TypeUtil.visit(field.type(), new IcebergToArrowTypeConverter(field));
  }

  private static class IcebergToArrowTypeConverter extends TypeUtil.SchemaVisitor<Field> {
    private final NestedField currentField;

    IcebergToArrowTypeConverter(NestedField field) {
      this.currentField = field;
    }

    @Override
    public Field schema(org.apache.iceberg.Schema schema, Field structResult) {
      return structResult;
    }

    @Override
    public Field struct(StructType struct, List<Field> fieldResults) {
      return new Field(
          currentField.name(),
          new FieldType(currentField.isOptional(), ArrowType.Struct.INSTANCE, null),
          convertChildren(struct.fields()));
    }

    @Override
    public Field field(NestedField field, Field fieldResult) {
      return fieldResult;
    }

    @Override
    public Field list(ListType list, Field elementResult) {
      return new Field(
          currentField.name(),
          new FieldType(currentField.isOptional(), ArrowType.List.INSTANCE, null),
          convertChildren(list.fields()));
    }

    @Override
    public Field map(MapType map, Field keyResult, Field valueResult) {
      Map<String, String> metadata = ImmutableMap.of(ORIGINAL_TYPE, MAP_TYPE);
      ArrowType arrowType = new ArrowType.Map(false);

      List<Field> entryFields = convertChildren(map.fields());

      Field entry =
          new Field("", new FieldType(currentField.isOptional(), arrowType, null), entryFields);
      List<Field> children = Lists.newArrayList(entry);

      return new Field(
          currentField.name(),
          new FieldType(currentField.isOptional(), arrowType, null, metadata),
          children);
    }

    private List<Field> convertChildren(Collection<NestedField> children) {
      List<Field> converted = Lists.newArrayListWithCapacity(children.size());

      for (NestedField child : children) {
        converted.add(TypeUtil.visit(child.type(), new IcebergToArrowTypeConverter(child)));
      }

      return converted;
    }

    @Override
    public Field primitive(Type.PrimitiveType primitive) {
      final ArrowType arrowType;

      switch (primitive.typeId()) {
        case BINARY:
          arrowType = ArrowType.Binary.INSTANCE;
          break;
        case FIXED:
          final Types.FixedType fixedType = (Types.FixedType) primitive;
          arrowType = new ArrowType.FixedSizeBinary(fixedType.length());
          break;
        case BOOLEAN:
          arrowType = ArrowType.Bool.INSTANCE;
          break;
        case INTEGER:
          arrowType = new ArrowType.Int(Integer.SIZE, true /* signed */);
          break;
        case LONG:
          arrowType = new ArrowType.Int(Long.SIZE, true /* signed */);
          break;
        case FLOAT:
          arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
          break;
        case DOUBLE:
          arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
          break;
        case DECIMAL:
          final Types.DecimalType decimalType = (Types.DecimalType) primitive;
          arrowType = new ArrowType.Decimal(decimalType.precision(), decimalType.scale(), 128);
          break;
        case STRING:
          arrowType = ArrowType.Utf8.INSTANCE;
          break;
        case TIME:
          arrowType = new ArrowType.Time(TimeUnit.MICROSECOND, Long.SIZE);
          break;
        case UUID:
          arrowType = new ArrowType.FixedSizeBinary(16);
          break;
        case TIMESTAMP:
          arrowType =
              new ArrowType.Timestamp(
                  TimeUnit.MICROSECOND,
                  ((Types.TimestampType) primitive).shouldAdjustToUTC() ? "UTC" : null);
          break;
        case TIMESTAMP_NANO:
          arrowType =
              new ArrowType.Timestamp(
                  TimeUnit.NANOSECOND,
                  ((Types.TimestampNanoType) primitive).shouldAdjustToUTC() ? "UTC" : null);
          break;
        case DATE:
          arrowType = new ArrowType.Date(DateUnit.DAY);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported primitive type: " + primitive);
      }

      return new Field(
          currentField.name(),
          new FieldType(currentField.isOptional(), arrowType, null),
          Lists.newArrayList());
    }
  }
}
