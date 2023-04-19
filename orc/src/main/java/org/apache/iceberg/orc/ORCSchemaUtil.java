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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;

/** Utilities for mapping Iceberg to ORC schemas. */
public final class ORCSchemaUtil {

  public enum BinaryType {
    UUID,
    FIXED,
    BINARY
  }

  public enum LongType {
    TIME,
    LONG
  }

  private static class OrcField {
    private final String name;
    private final TypeDescription type;

    OrcField(String name, TypeDescription type) {
      this.name = name;
      this.type = type;
    }

    public String name() {
      return name;
    }

    public TypeDescription type() {
      return type;
    }
  }

  static final String ICEBERG_ID_ATTRIBUTE = "iceberg.id";
  static final String ICEBERG_REQUIRED_ATTRIBUTE = "iceberg.required";

  /**
   * The name of the ORC {@link TypeDescription} attribute indicating the Iceberg type corresponding
   * to an ORC binary type. The values for this attribute are denoted in {@code BinaryType}.
   */
  public static final String ICEBERG_BINARY_TYPE_ATTRIBUTE = "iceberg.binary-type";
  /**
   * The name of the ORC {@link TypeDescription} attribute indicating the Iceberg type corresponding
   * to an ORC long type. The values for this attribute are denoted in {@code LongType}.
   */
  public static final String ICEBERG_LONG_TYPE_ATTRIBUTE = "iceberg.long-type";

  static final String ICEBERG_FIELD_LENGTH = "iceberg.length";

  private static final ImmutableMultimap<Type.TypeID, TypeDescription.Category> TYPE_MAPPING =
      ImmutableMultimap.<Type.TypeID, TypeDescription.Category>builder()
          .put(Type.TypeID.BOOLEAN, TypeDescription.Category.BOOLEAN)
          .put(Type.TypeID.INTEGER, TypeDescription.Category.BYTE)
          .put(Type.TypeID.INTEGER, TypeDescription.Category.SHORT)
          .put(Type.TypeID.INTEGER, TypeDescription.Category.INT)
          .put(Type.TypeID.LONG, TypeDescription.Category.LONG)
          .put(Type.TypeID.TIME, TypeDescription.Category.LONG)
          .put(Type.TypeID.FLOAT, TypeDescription.Category.FLOAT)
          .put(Type.TypeID.DOUBLE, TypeDescription.Category.DOUBLE)
          .put(Type.TypeID.DATE, TypeDescription.Category.DATE)
          .put(Type.TypeID.STRING, TypeDescription.Category.CHAR)
          .put(Type.TypeID.STRING, TypeDescription.Category.VARCHAR)
          .put(Type.TypeID.STRING, TypeDescription.Category.STRING)
          .put(Type.TypeID.UUID, TypeDescription.Category.BINARY)
          .put(Type.TypeID.FIXED, TypeDescription.Category.BINARY)
          .put(Type.TypeID.BINARY, TypeDescription.Category.BINARY)
          .put(Type.TypeID.DECIMAL, TypeDescription.Category.DECIMAL)
          .build();

  private ORCSchemaUtil() {}

  public static TypeDescription convert(Schema schema) {
    final TypeDescription root = TypeDescription.createStruct();
    final Types.StructType schemaRoot = schema.asStruct();
    for (Types.NestedField field : schemaRoot.asStructType().fields()) {
      TypeDescription orcColumnType = convert(field.fieldId(), field.type(), field.isRequired());
      root.addField(field.name(), orcColumnType);
    }
    return root;
  }

  private static TypeDescription convert(Integer fieldId, Type type, boolean isRequired) {
    final TypeDescription orcType;

    switch (type.typeId()) {
      case BOOLEAN:
        orcType = TypeDescription.createBoolean();
        break;
      case INTEGER:
        orcType = TypeDescription.createInt();
        break;
      case TIME:
        orcType = TypeDescription.createLong();
        orcType.setAttribute(ICEBERG_LONG_TYPE_ATTRIBUTE, LongType.TIME.toString());
        break;
      case LONG:
        orcType = TypeDescription.createLong();
        orcType.setAttribute(ICEBERG_LONG_TYPE_ATTRIBUTE, LongType.LONG.toString());
        break;
      case FLOAT:
        orcType = TypeDescription.createFloat();
        break;
      case DOUBLE:
        orcType = TypeDescription.createDouble();
        break;
      case DATE:
        orcType = TypeDescription.createDate();
        break;
      case TIMESTAMP:
        Types.TimestampType tsType = (Types.TimestampType) type;
        if (tsType.shouldAdjustToUTC()) {
          orcType = TypeDescription.createTimestampInstant();
        } else {
          orcType = TypeDescription.createTimestamp();
        }
        break;
      case STRING:
        orcType = TypeDescription.createString();
        break;
      case UUID:
        orcType = TypeDescription.createBinary();
        orcType.setAttribute(ICEBERG_BINARY_TYPE_ATTRIBUTE, BinaryType.UUID.toString());
        break;
      case FIXED:
        orcType = TypeDescription.createBinary();
        orcType.setAttribute(ICEBERG_BINARY_TYPE_ATTRIBUTE, BinaryType.FIXED.toString());
        orcType.setAttribute(
            ICEBERG_FIELD_LENGTH, Integer.toString(((Types.FixedType) type).length()));
        break;
      case BINARY:
        orcType = TypeDescription.createBinary();
        orcType.setAttribute(ICEBERG_BINARY_TYPE_ATTRIBUTE, BinaryType.BINARY.toString());
        break;
      case DECIMAL:
        {
          Types.DecimalType decimal = (Types.DecimalType) type;
          orcType =
              TypeDescription.createDecimal()
                  .withScale(decimal.scale())
                  .withPrecision(decimal.precision());
          break;
        }
      case STRUCT:
        {
          orcType = TypeDescription.createStruct();
          for (Types.NestedField field : type.asStructType().fields()) {
            TypeDescription childType = convert(field.fieldId(), field.type(), field.isRequired());
            orcType.addField(field.name(), childType);
          }
          break;
        }
      case LIST:
        {
          Types.ListType list = (Types.ListType) type;
          TypeDescription elementType =
              convert(list.elementId(), list.elementType(), list.isElementRequired());
          orcType = TypeDescription.createList(elementType);
          break;
        }
      case MAP:
        {
          Types.MapType map = (Types.MapType) type;
          TypeDescription keyType = convert(map.keyId(), map.keyType(), true);
          TypeDescription valueType =
              convert(map.valueId(), map.valueType(), map.isValueRequired());
          orcType = TypeDescription.createMap(keyType, valueType);
          break;
        }
      default:
        throw new IllegalArgumentException("Unhandled type " + type.typeId());
    }

    // Set Iceberg column attributes for mapping
    orcType.setAttribute(ICEBERG_ID_ATTRIBUTE, String.valueOf(fieldId));
    orcType.setAttribute(ICEBERG_REQUIRED_ATTRIBUTE, String.valueOf(isRequired));
    return orcType;
  }

  /**
   * Convert an ORC schema to an Iceberg schema. This method handles the convertion from the
   * original Iceberg column mapping IDs if present in the ORC column attributes, otherwise, ORC
   * columns with no Iceberg IDs will be ignored and skipped in the conversion.
   *
   * @return the Iceberg schema
   * @throws IllegalArgumentException if ORC schema has no columns with Iceberg ID attributes
   */
  public static Schema convert(TypeDescription orcSchema) {
    List<TypeDescription> children = orcSchema.getChildren();
    List<String> childrenNames = orcSchema.getFieldNames();
    Preconditions.checkState(
        children.size() == childrenNames.size(),
        "Error in ORC file, children fields and names do not match.");

    OrcToIcebergVisitor schemaConverter = new OrcToIcebergVisitor();
    List<Types.NestedField> fields =
        OrcToIcebergVisitor.visitSchema(orcSchema, schemaConverter).stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

    if (fields.size() == 0) {
      throw new IllegalArgumentException("ORC schema does not contain Iceberg IDs");
    }

    return new Schema(fields);
  }

  /**
   * Converts an Iceberg schema to a corresponding ORC schema within the context of an existing ORC
   * file schema. This method also handles schema evolution from the original ORC file schema to the
   * given Iceberg schema. It builds the desired reader schema with the schema evolution rules and
   * pass that down to the ORC reader, which would then use its schema evolution to map that to the
   * writer’s schema.
   *
   * <p>Example: <code>
   * Iceberg writer                                        ORC writer
   * struct&lt;a (1): int, b (2): string&gt;                     struct&lt;a: int, b: string&gt;
   * struct&lt;a (1): struct&lt;b (2): string, c (3): date&gt;&gt;     struct&lt;a: struct&lt;b:string, c:date&gt;&gt;
   * </code> Iceberg reader ORC reader <code>
   * struct&lt;a (2): string, c (3): date&gt;                    struct&lt;b: string, c: date&gt;
   * struct&lt;aa (1): struct&lt;cc (3): date, bb (2): string&gt;&gt;  struct&lt;a: struct&lt;c:date, b:string&gt;&gt;
   * </code>
   *
   * @param schema an Iceberg schema
   * @param originalOrcSchema an existing ORC file schema
   * @return the resulting ORC schema
   */
  public static TypeDescription buildOrcProjection(
      Schema schema, TypeDescription originalOrcSchema) {
    final Map<Integer, OrcField> icebergToOrc = icebergToOrcMapping("root", originalOrcSchema);
    return buildOrcProjection(Integer.MIN_VALUE, schema.asStruct(), true, icebergToOrc);
  }

  private static TypeDescription buildOrcProjection(
      Integer fieldId, Type type, boolean isRequired, Map<Integer, OrcField> mapping) {
    final TypeDescription orcType;

    switch (type.typeId()) {
      case STRUCT:
        orcType = TypeDescription.createStruct();
        for (Types.NestedField nestedField : type.asStructType().fields()) {
          // Using suffix _r to avoid potential underlying issues in ORC reader
          // with reused column names between ORC and Iceberg;
          // e.g. renaming column c -> d and adding new column d
          String name =
              Optional.ofNullable(mapping.get(nestedField.fieldId()))
                  .map(OrcField::name)
                  .orElseGet(() -> nestedField.name() + "_r" + nestedField.fieldId());
          TypeDescription childType =
              buildOrcProjection(
                  nestedField.fieldId(),
                  nestedField.type(),
                  isRequired && nestedField.isRequired(),
                  mapping);
          orcType.addField(name, childType);
        }
        break;
      case LIST:
        Types.ListType list = (Types.ListType) type;
        TypeDescription elementType =
            buildOrcProjection(
                list.elementId(),
                list.elementType(),
                isRequired && list.isElementRequired(),
                mapping);
        orcType = TypeDescription.createList(elementType);
        break;
      case MAP:
        Types.MapType map = (Types.MapType) type;
        TypeDescription keyType =
            buildOrcProjection(map.keyId(), map.keyType(), isRequired, mapping);
        TypeDescription valueType =
            buildOrcProjection(
                map.valueId(), map.valueType(), isRequired && map.isValueRequired(), mapping);
        orcType = TypeDescription.createMap(keyType, valueType);
        break;
      default:
        if (mapping.containsKey(fieldId)) {
          TypeDescription originalType = mapping.get(fieldId).type();
          Optional<TypeDescription> promotedType = getPromotedType(type, originalType);

          if (promotedType.isPresent()) {
            orcType = promotedType.get();
          } else {
            Preconditions.checkArgument(
                isSameType(originalType, type),
                "Can not promote %s type to %s",
                originalType.getCategory(),
                type.typeId().name());
            orcType = originalType.clone();
          }
        } else {
          if (isRequired) {
            throw new IllegalArgumentException(
                String.format(
                    "Field %d of type %s is required and was not found.",
                    fieldId, type.toString()));
          }

          orcType = convert(fieldId, type, false);
        }
    }
    orcType.setAttribute(ICEBERG_ID_ATTRIBUTE, fieldId.toString());
    return orcType;
  }

  private static Map<Integer, OrcField> icebergToOrcMapping(String name, TypeDescription orcType) {
    Map<Integer, OrcField> icebergToOrc = Maps.newHashMap();
    switch (orcType.getCategory()) {
      case STRUCT:
        List<String> childrenNames = orcType.getFieldNames();
        List<TypeDescription> children = orcType.getChildren();
        for (int i = 0; i < children.size(); i++) {
          icebergToOrc.putAll(icebergToOrcMapping(childrenNames.get(i), children.get(i)));
        }
        break;
      case LIST:
        icebergToOrc.putAll(icebergToOrcMapping("element", orcType.getChildren().get(0)));
        break;
      case MAP:
        icebergToOrc.putAll(icebergToOrcMapping("key", orcType.getChildren().get(0)));
        icebergToOrc.putAll(icebergToOrcMapping("value", orcType.getChildren().get(1)));
        break;
    }

    if (orcType.getId() > 0) {
      // Only add to non-root types.
      icebergID(orcType)
          .ifPresent(integer -> icebergToOrc.put(integer, new OrcField(name, orcType)));
    }

    return icebergToOrc;
  }

  private static Optional<TypeDescription> getPromotedType(
      Type icebergType, TypeDescription originalOrcType) {
    TypeDescription promotedOrcType = null;
    if (Type.TypeID.LONG.equals(icebergType.typeId())
        && TypeDescription.Category.INT.equals(originalOrcType.getCategory())) {
      // Promote: int to long
      promotedOrcType = TypeDescription.createLong();
    } else if (Type.TypeID.DOUBLE.equals(icebergType.typeId())
        && TypeDescription.Category.FLOAT.equals(originalOrcType.getCategory())) {
      // Promote: float to double
      promotedOrcType = TypeDescription.createDouble();
    } else if (Type.TypeID.DECIMAL.equals(icebergType.typeId())
        && TypeDescription.Category.DECIMAL.equals(originalOrcType.getCategory())) {
      // Promote: decimal(P, S) to decimal(P', S) if P' > P
      Types.DecimalType newDecimal = (Types.DecimalType) icebergType;
      if (newDecimal.scale() == originalOrcType.getScale()
          && newDecimal.precision() > originalOrcType.getPrecision()) {
        promotedOrcType =
            TypeDescription.createDecimal()
                .withScale(newDecimal.scale())
                .withPrecision(newDecimal.precision());
      }
    }
    return Optional.ofNullable(promotedOrcType);
  }

  private static boolean isSameType(TypeDescription orcType, Type icebergType) {
    if (icebergType.typeId() == Type.TypeID.TIMESTAMP) {
      Types.TimestampType tsType = (Types.TimestampType) icebergType;
      return Objects.equals(
          tsType.shouldAdjustToUTC()
              ? TypeDescription.Category.TIMESTAMP_INSTANT
              : TypeDescription.Category.TIMESTAMP,
          orcType.getCategory());
    } else {
      return TYPE_MAPPING.containsEntry(icebergType.typeId(), orcType.getCategory());
    }
  }

  static Optional<Integer> icebergID(TypeDescription orcType) {
    return Optional.ofNullable(orcType.getAttributeValue(ICEBERG_ID_ATTRIBUTE))
        .map(Integer::parseInt);
  }

  public static int fieldId(TypeDescription orcType) {
    String idStr = orcType.getAttributeValue(ICEBERG_ID_ATTRIBUTE);
    Preconditions.checkNotNull(idStr, "Missing expected '%s' property", ICEBERG_ID_ATTRIBUTE);
    return Integer.parseInt(idStr);
  }

  static boolean isOptional(TypeDescription orcType) {
    String isRequiredStr = orcType.getAttributeValue(ICEBERG_REQUIRED_ATTRIBUTE);
    if (isRequiredStr != null) {
      return !Boolean.parseBoolean(isRequiredStr);
    }
    return true;
  }

  static TypeDescription removeIds(TypeDescription type) {
    return OrcSchemaVisitor.visit(type, new RemoveIds());
  }

  static boolean hasIds(TypeDescription orcSchema) {
    return OrcSchemaVisitor.visit(orcSchema, new HasIds());
  }

  static TypeDescription applyNameMapping(TypeDescription orcSchema, NameMapping nameMapping) {
    return OrcSchemaVisitor.visit(orcSchema, new ApplyNameMapping(nameMapping));
  }

  /**
   * Generates mapping from field IDs to ORC qualified names. See {@link IdToOrcName} for details.
   */
  public static Map<Integer, String> idToOrcName(Schema schema) {
    return TypeUtil.visit(schema, new IdToOrcName());
  }
}
