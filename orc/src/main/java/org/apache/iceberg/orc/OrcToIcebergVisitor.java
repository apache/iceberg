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
        Types.NestedField.of(icebergIdOpt.get(), isOptional, currentFieldName(), structType));
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
        Types.NestedField.of(icebergIdOpt.get(), isOptional, currentFieldName(), listTypeWithElem));
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
        Types.NestedField.of(icebergIdOpt.get(), isOptional, currentFieldName(), mapTypeWithKV));
  }

  @Override
  public Optional<Types.NestedField> primitive(TypeDescription primitive) {
    boolean isOptional = ORCSchemaUtil.isOptional(primitive);
    Optional<Integer> icebergIdOpt = ORCSchemaUtil.icebergID(primitive);

    if (!icebergIdOpt.isPresent()) {
      return Optional.empty();
    }

    final Types.NestedField foundField;
    int icebergID = icebergIdOpt.get();
    String name = currentFieldName();
    switch (primitive.getCategory()) {
      case BOOLEAN:
        foundField = Types.NestedField.of(icebergID, isOptional, name, Types.BooleanType.get());
        break;
      case BYTE:
      case SHORT:
      case INT:
        foundField = Types.NestedField.of(icebergID, isOptional, name, Types.IntegerType.get());
        break;
      case LONG:
        String longAttributeValue =
            primitive.getAttributeValue(ORCSchemaUtil.ICEBERG_LONG_TYPE_ATTRIBUTE);
        ORCSchemaUtil.LongType longType =
            longAttributeValue == null
                ? ORCSchemaUtil.LongType.LONG
                : ORCSchemaUtil.LongType.valueOf(longAttributeValue);
        switch (longType) {
          case TIME:
            foundField = Types.NestedField.of(icebergID, isOptional, name, Types.TimeType.get());
            break;
          case LONG:
            foundField = Types.NestedField.of(icebergID, isOptional, name, Types.LongType.get());
            break;
          default:
            throw new IllegalStateException("Invalid Long type found in ORC type attribute");
        }
        break;
      case FLOAT:
        foundField = Types.NestedField.of(icebergID, isOptional, name, Types.FloatType.get());
        break;
      case DOUBLE:
        foundField = Types.NestedField.of(icebergID, isOptional, name, Types.DoubleType.get());
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
        foundField = Types.NestedField.of(icebergID, isOptional, name, Types.StringType.get());
        break;
      case BINARY:
        String binaryAttributeValue =
            primitive.getAttributeValue(ORCSchemaUtil.ICEBERG_BINARY_TYPE_ATTRIBUTE);
        ORCSchemaUtil.BinaryType binaryType =
            binaryAttributeValue == null
                ? ORCSchemaUtil.BinaryType.BINARY
                : ORCSchemaUtil.BinaryType.valueOf(binaryAttributeValue);
        switch (binaryType) {
          case UUID:
            foundField = Types.NestedField.of(icebergID, isOptional, name, Types.UUIDType.get());
            break;
          case FIXED:
            int fixedLength =
                Integer.parseInt(primitive.getAttributeValue(ORCSchemaUtil.ICEBERG_FIELD_LENGTH));
            foundField =
                Types.NestedField.of(
                    icebergID, isOptional, name, Types.FixedType.ofLength(fixedLength));
            break;
          case BINARY:
            foundField = Types.NestedField.of(icebergID, isOptional, name, Types.BinaryType.get());
            break;
          default:
            throw new IllegalStateException("Invalid Binary type found in ORC type attribute");
        }
        break;
      case DATE:
        foundField = Types.NestedField.of(icebergID, isOptional, name, Types.DateType.get());
        break;
      case TIMESTAMP:
        foundField =
            Types.NestedField.of(icebergID, isOptional, name, Types.TimestampType.withoutZone());
        break;
      case TIMESTAMP_INSTANT:
        foundField =
            Types.NestedField.of(icebergID, isOptional, name, Types.TimestampType.withZone());
        break;
      case DECIMAL:
        foundField =
            Types.NestedField.of(
                icebergID,
                isOptional,
                name,
                Types.DecimalType.of(primitive.getPrecision(), primitive.getScale()));
        break;
      default:
        throw new IllegalArgumentException("Can't handle " + primitive);
    }
    return Optional.of(foundField);
  }
}
