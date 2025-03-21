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
package org.apache.iceberg.orc;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;

/** Converts an ORC schema to Iceberg. */
class OrcToIcebergVisitor extends OrcSchemaVisitor<Optional<Types.NestedField>> {

  @Override
  public Optional<Types.NestedField> record(
      TypeDescription record, List<String> names, List<Optional<Types.NestedField>> fields) {
    boolean isOptional = ORCSchemaUtil.isOptional(record);
    Optional<Integer> icebergIdOpt = ORCSchemaUtil.icebergID(record);
    if (!icebergIdOpt.isPresent() || fields.stream().noneMatch(Optional::isPresent)) {
      return Optional.empty();
    }

    Types.StructType structType =
        Types.StructType.of(
            fields.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()));
    return Optional.of(
        Types.NestedField.builder()
            .withId(icebergIdOpt.get())
            .isOptional(isOptional)
            .withName(currentFieldName())
            .ofType(structType)
            .build());
  }

  @Override
  public Optional<Types.NestedField> list(
      TypeDescription array, Optional<Types.NestedField> element) {
    boolean isOptional = ORCSchemaUtil.isOptional(array);
    Optional<Integer> icebergIdOpt = ORCSchemaUtil.icebergID(array);

    if (!icebergIdOpt.isPresent() || !element.isPresent()) {
      return Optional.empty();
    }

    Types.NestedField foundElement = element.get();
    Types.ListType listTypeWithElem =
        ORCSchemaUtil.isOptional(array.getChildren().get(0))
            ? Types.ListType.ofOptional(foundElement.fieldId(), foundElement.type())
            : Types.ListType.ofRequired(foundElement.fieldId(), foundElement.type());

    return Optional.of(
        Types.NestedField.builder()
            .withId(icebergIdOpt.get())
            .isOptional(isOptional)
            .withName(currentFieldName())
            .ofType(listTypeWithElem)
            .build());
  }

  @Override
  public Optional<Types.NestedField> map(
      TypeDescription map, Optional<Types.NestedField> key, Optional<Types.NestedField> value) {
    boolean isOptional = ORCSchemaUtil.isOptional(map);
    Optional<Integer> icebergIdOpt = ORCSchemaUtil.icebergID(map);

    if (!icebergIdOpt.isPresent() || !key.isPresent() || !value.isPresent()) {
      return Optional.empty();
    }

    Types.NestedField foundKey = key.get();
    Types.NestedField foundValue = value.get();
    Types.MapType mapTypeWithKV =
        ORCSchemaUtil.isOptional(map.getChildren().get(1))
            ? Types.MapType.ofOptional(
                foundKey.fieldId(), foundValue.fieldId(), foundKey.type(), foundValue.type())
            : Types.MapType.ofRequired(
                foundKey.fieldId(), foundValue.fieldId(), foundKey.type(), foundValue.type());

    return Optional.of(
        Types.NestedField.builder()
            .withId(icebergIdOpt.get())
            .isOptional(isOptional)
            .withName(currentFieldName())
            .ofType(mapTypeWithKV)
            .build());
  }

  @Override
  public Optional<Types.NestedField> variant(
      TypeDescription variant,
      Optional<Types.NestedField> metadata,
      Optional<Types.NestedField> value) {
    boolean isOptional = ORCSchemaUtil.isOptional(variant);
    Optional<Integer> icebergIdOpt = ORCSchemaUtil.icebergID(variant);

    return icebergIdOpt.map(
        fieldId ->
            Types.NestedField.builder()
                .withId(fieldId)
                .isOptional(isOptional)
                .ofType(Types.VariantType.get())
                .withName(currentFieldName())
                .build());
  }

  @Override
  public Optional<Types.NestedField> primitive(TypeDescription primitive) {
    boolean isOptional = ORCSchemaUtil.isOptional(primitive);
    Optional<Integer> icebergIdOpt = ORCSchemaUtil.icebergID(primitive);

    if (!icebergIdOpt.isPresent()) {
      return Optional.empty();
    }

    Types.NestedField.Builder builder =
        Types.NestedField.builder()
            .withId(icebergIdOpt.get())
            .isOptional(isOptional)
            .withName(currentFieldName());
    switch (primitive.getCategory()) {
      case BOOLEAN:
        builder.ofType(Types.BooleanType.get());
        break;
      case BYTE:
      case SHORT:
      case INT:
        builder.ofType(Types.IntegerType.get());
        break;
      case LONG:
        convertLong(primitive, builder);
        break;
      case FLOAT:
        builder.ofType(Types.FloatType.get());
        break;
      case DOUBLE:
        builder.ofType(Types.DoubleType.get());
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
        builder.ofType(Types.StringType.get());
        break;
      case BINARY:
        convertBinary(primitive, builder);
        break;
      case DATE:
        builder.ofType(Types.DateType.get());
        break;
      case TIMESTAMP:
        String unit = primitive.getAttributeValue(ORCSchemaUtil.TIMESTAMP_UNIT);
        if (unit == null || ORCSchemaUtil.MICROS.equalsIgnoreCase(unit)) {
          builder.ofType(Types.TimestampType.withoutZone());
        } else if (unit.equalsIgnoreCase(ORCSchemaUtil.NANOS)) {
          builder.ofType(Types.TimestampNanoType.withoutZone());
        } else {
          throw new IllegalStateException("Invalid Timestamp type unit: %s" + unit);
        }

        break;
      case TIMESTAMP_INSTANT:
        String tsUnit = primitive.getAttributeValue(ORCSchemaUtil.TIMESTAMP_UNIT);
        if (tsUnit == null || ORCSchemaUtil.MICROS.equalsIgnoreCase(tsUnit)) {
          builder.ofType(Types.TimestampType.withZone());
        } else if (tsUnit.equalsIgnoreCase(ORCSchemaUtil.NANOS)) {
          builder.ofType(Types.TimestampNanoType.withZone());
        } else {
          throw new IllegalStateException("Invalid Timestamp type unit: %s" + tsUnit);
        }

        break;
      case DECIMAL:
        builder.ofType(Types.DecimalType.of(primitive.getPrecision(), primitive.getScale()));
        break;
      default:
        throw new IllegalArgumentException("Can't handle " + primitive);
    }

    return Optional.of(builder.build());
  }

  private static void convertLong(TypeDescription primitive, Types.NestedField.Builder builder) {
    String longAttributeValue =
        primitive.getAttributeValue(ORCSchemaUtil.ICEBERG_LONG_TYPE_ATTRIBUTE);
    ORCSchemaUtil.LongType longType =
        longAttributeValue == null
            ? ORCSchemaUtil.LongType.LONG
            : ORCSchemaUtil.LongType.valueOf(longAttributeValue);
    switch (longType) {
      case TIME:
        builder.ofType(Types.TimeType.get());
        break;
      case LONG:
        builder.ofType(Types.LongType.get());
        break;
      default:
        throw new IllegalStateException("Invalid Long type found in ORC type attribute");
    }
  }

  private static void convertBinary(TypeDescription binary, Types.NestedField.Builder builder) {
    String binaryAttributeValue =
        binary.getAttributeValue(ORCSchemaUtil.ICEBERG_BINARY_TYPE_ATTRIBUTE);
    ORCSchemaUtil.BinaryType binaryType =
        binaryAttributeValue == null
            ? ORCSchemaUtil.BinaryType.BINARY
            : ORCSchemaUtil.BinaryType.valueOf(binaryAttributeValue);
    switch (binaryType) {
      case UUID:
        builder.ofType(Types.UUIDType.get());
        break;
      case FIXED:
        int fixedLength =
            Integer.parseInt(binary.getAttributeValue(ORCSchemaUtil.ICEBERG_FIELD_LENGTH));
        builder.ofType(Types.FixedType.ofLength(fixedLength));
        break;
      case BINARY:
        builder.ofType(Types.BinaryType.get());
        break;
      default:
        throw new IllegalStateException("Invalid Binary type found in ORC type attribute");
    }
  }
}
