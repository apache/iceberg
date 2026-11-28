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
package org.apache.iceberg.udf;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

/**
 * Concrete implementations of {@link UdfType}: {@link PrimitiveType} for primitive and
 * semi-structured types and {@link ListType}, {@link MapType}, {@link StructType} for nested types.
 * {@link NestedField} represents a named field inside a {@link StructType}.
 */
public class UdfTypes {

  private UdfTypes() {}

  /**
   * A UDF primitive or semi-structured type, encoded as a type string (e.g., {@code int}, {@code
   * string}, {@code decimal(9, 2)}, {@code variant}).
   *
   * <p>The type string must be a recognized Iceberg primitive or semi-structured type as understood
   * by {@link Types#fromTypeName(String)}. The input is canonicalized to Iceberg's standard form
   * (lowercase, normalized whitespace), so {@code PrimitiveType.of("INT")} and {@code
   * PrimitiveType.of("Decimal( 9 , 2 )")} produce {@code int} and {@code decimal(9, 2)}
   * respectively.
   */
  public static final class PrimitiveType implements UdfType {

    private final String typeString;

    public static PrimitiveType of(String typeString) {
      Preconditions.checkArgument(typeString != null, "Invalid primitive type: null");
      // Validate against Iceberg's primitive/semi-structured type vocabulary and use the parsed
      // type's canonical toString() so callers don't have to worry about casing or whitespace.
      String canonical = Types.fromTypeName(typeString).toString();
      return new PrimitiveType(canonical);
    }

    private PrimitiveType(String typeString) {
      this.typeString = typeString;
    }

    @Override
    public TypeID typeId() {
      return TypeID.PRIMITIVE;
    }

    @Override
    public PrimitiveType asPrimitiveType() {
      return this;
    }

    /** The primitive or semi-structured type string. */
    public String typeString() {
      return typeString;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof PrimitiveType)) {
        return false;
      }

      return Objects.equals(typeString, ((PrimitiveType) o).typeString);
    }

    @Override
    public int hashCode() {
      return Objects.hash(PrimitiveType.class, typeString);
    }

    @Override
    public String toString() {
      return typeString;
    }
  }

  /** A UDF list type with an element type. */
  public static final class ListType implements UdfType {

    private final UdfType elementType;

    public static ListType of(UdfType elementType) {
      Preconditions.checkArgument(elementType != null, "Invalid element type: null");
      return new ListType(elementType);
    }

    private ListType(UdfType elementType) {
      this.elementType = elementType;
    }

    @Override
    public TypeID typeId() {
      return TypeID.LIST;
    }

    @Override
    public ListType asListType() {
      return this;
    }

    public UdfType elementType() {
      return elementType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof ListType)) {
        return false;
      }

      return Objects.equals(elementType, ((ListType) o).elementType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ListType.class, elementType);
    }

    @Override
    public String toString() {
      return String.format("list<%s>", elementType);
    }
  }

  /** A UDF map type with key and value types. */
  public static final class MapType implements UdfType {

    private final UdfType keyType;
    private final UdfType valueType;

    public static MapType of(UdfType keyType, UdfType valueType) {
      Preconditions.checkArgument(keyType != null, "Invalid key type: null");
      Preconditions.checkArgument(valueType != null, "Invalid value type: null");
      return new MapType(keyType, valueType);
    }

    private MapType(UdfType keyType, UdfType valueType) {
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    public TypeID typeId() {
      return TypeID.MAP;
    }

    @Override
    public MapType asMapType() {
      return this;
    }

    public UdfType keyType() {
      return keyType;
    }

    public UdfType valueType() {
      return valueType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof MapType)) {
        return false;
      }

      MapType that = (MapType) o;
      return Objects.equals(keyType, that.keyType) && Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(MapType.class, keyType, valueType);
    }

    @Override
    public String toString() {
      return String.format("map<%s,%s>", keyType, valueType);
    }
  }

  /**
   * A UDF struct type with an ordered list of named fields. Based on Iceberg struct types but
   * intentionally omits field IDs and element nullability.
   */
  public static final class StructType implements UdfType {

    private final List<NestedField> fields;

    public static StructType of(NestedField... fields) {
      Preconditions.checkArgument(fields != null, "Invalid fields: null");
      return of(Arrays.asList(fields));
    }

    public static StructType of(List<NestedField> fields) {
      Preconditions.checkArgument(fields != null, "Invalid fields: null");
      return new StructType(ImmutableList.copyOf(fields));
    }

    private StructType(List<NestedField> fields) {
      this.fields = fields;
    }

    @Override
    public TypeID typeId() {
      return TypeID.STRUCT;
    }

    @Override
    public StructType asStructType() {
      return this;
    }

    public List<NestedField> fields() {
      return fields;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof StructType)) {
        return false;
      }

      return Objects.equals(fields, ((StructType) o).fields);
    }

    @Override
    public int hashCode() {
      return Objects.hash(StructType.class, fields);
    }

    @Override
    public String toString() {
      return fields.stream()
          .map(NestedField::toString)
          .collect(Collectors.joining(",", "struct<", ">"));
    }
  }

  /** A field within a {@link StructType}, with a name and a type. */
  public static final class NestedField {

    private final String name;
    private final UdfType type;

    public static NestedField of(String name, UdfType type) {
      Preconditions.checkArgument(name != null, "Invalid field name: null");
      Preconditions.checkArgument(type != null, "Invalid field type: null");
      return new NestedField(name, type);
    }

    private NestedField(String name, UdfType type) {
      this.name = name;
      this.type = type;
    }

    public String name() {
      return name;
    }

    public UdfType type() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof NestedField)) {
        return false;
      }

      NestedField that = (NestedField) o;
      return Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }

    @Override
    public String toString() {
      return String.format("%s:%s", name, type);
    }
  }
}
