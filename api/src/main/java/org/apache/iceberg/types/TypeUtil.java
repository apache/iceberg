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

package org.apache.iceberg.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;

public class TypeUtil {

  private TypeUtil() {}

  public static Schema select(Schema schema, Set<Integer> fieldIds) {
    Preconditions.checkNotNull(schema, "Schema cannot be null");
    Preconditions.checkNotNull(fieldIds, "Field ids cannot be null");

    Type result = visit(schema, new PruneColumns(fieldIds));
    if (schema.asStruct() == result) {
      return schema;
    } else if (result != null) {
      if (schema.getAliases() != null) {
        return new Schema(result.asNestedType().fields(), schema.getAliases());
      } else {
        return new Schema(result.asNestedType().fields());
      }
    }

    return new Schema(ImmutableList.of(), schema.getAliases());
  }

  public static Set<Integer> getProjectedIds(Schema schema) {
    return visit(schema, new GetProjectedIds());
  }

  public static Set<Integer> getProjectedIds(Type schema) {
    if (schema.isPrimitiveType()) {
      return ImmutableSet.of();
    }
    return ImmutableSet.copyOf(visit(schema, new GetProjectedIds()));
  }

  public static Schema selectNot(Schema schema, Set<Integer> fieldIds) {
    Set<Integer> projectedIds = getProjectedIds(schema);
    projectedIds.removeAll(fieldIds);
    return select(schema, projectedIds);
  }

  public static Schema join(Schema left, Schema right) {
    List<Types.NestedField> joinedColumns = Lists.newArrayList();
    joinedColumns.addAll(left.columns());
    joinedColumns.addAll(right.columns());
    return new Schema(joinedColumns);
  }

  public static Map<String, Integer> indexByName(Types.StructType struct) {
    return visit(struct, new IndexByName());
  }

  public static Map<String, Integer> indexByLowerCaseName(Types.StructType struct) {
    Map<String, Integer> indexByLowerCaseName = Maps.newHashMap();
    indexByName(struct).forEach((name, integer) ->
        indexByLowerCaseName.put(name.toLowerCase(Locale.ROOT), integer));
    return indexByLowerCaseName;
  }

  public static Map<Integer, Types.NestedField> indexById(Types.StructType struct) {
    return visit(struct, new IndexById());
  }

  /**
   * Assigns fresh ids from the {@link NextID nextId function} for all fields in a type.
   *
   * @param type a type
   * @param nextId an id assignment function
   * @return an structurally identical type with new ids assigned by the nextId function
   */
  public static Type assignFreshIds(Type type, NextID nextId) {
    return TypeUtil.visit(type, new AssignFreshIds(nextId));
  }

  /**
   * Assigns fresh ids from the {@link NextID nextId function} for all fields in a schema.
   *
   * @param schema a schema
   * @param nextId an id assignment function
   * @return an structurally identical schema with new ids assigned by the nextId function
   */
  public static Schema assignFreshIds(Schema schema, NextID nextId) {
    return new Schema(TypeUtil
        .visit(schema.asStruct(), new AssignFreshIds(nextId))
        .asNestedType()
        .fields());
  }

  /**
   * Reassigns ids in a schema from another schema.
   * <p>
   * Ids are determined by field names. If a field in the schema cannot be found in the source
   * schema, this will throw IllegalArgumentException.
   * <p>
   * This will not alter a schema's structure, nullability, or types.
   *
   * @param schema the schema to have ids reassigned
   * @param idSourceSchema the schema from which field ids will be used
   * @return an structurally identical schema with field ids matching the source schema
   * @throws IllegalArgumentException if a field cannot be found (by name) in the source schema
   */
  public static Schema reassignIds(Schema schema, Schema idSourceSchema) {
    Types.StructType struct = visit(schema, new ReassignIds(idSourceSchema)).asStructType();
    return new Schema(struct.fields());
  }

  public static Type find(Schema schema, Predicate<Type> predicate) {
    return visit(schema, new FindTypeVisitor(predicate));
  }

  public static boolean isPromotionAllowed(Type from, Type.PrimitiveType to) {
    // Warning! Before changing this function, make sure that the type change doesn't introduce
    // compatibility problems in partitioning.
    if (from.equals(to)) {
      return true;
    }

    switch (from.typeId()) {
      case INTEGER:
        return to == Types.LongType.get();

      case FLOAT:
        return to == Types.DoubleType.get();

      case DECIMAL:
        Types.DecimalType fromDecimal = (Types.DecimalType) from;
        if (to.typeId() != Type.TypeID.DECIMAL) {
          return false;
        }

        Types.DecimalType toDecimal = (Types.DecimalType) to;
        return fromDecimal.scale() == toDecimal.scale() &&
            fromDecimal.precision() <= toDecimal.precision();
    }

    return false;
  }

  /**
   * Interface for passing a function that assigns column IDs.
   */
  public interface NextID {
    int get();
  }

  public static class SchemaVisitor<T> {
    private final Deque<String> fieldNames = Lists.newLinkedList();
    private final Deque<Integer> fieldIds = Lists.newLinkedList();

    public T schema(Schema schema, T structResult) {
      return null;
    }

    public T struct(Types.StructType struct, List<T> fieldResults) {
      return null;
    }

    public T field(Types.NestedField field, T fieldResult) {
      return null;
    }

    public T list(Types.ListType list, T elementResult) {
      return null;
    }

    public T map(Types.MapType map, T keyResult, T valueResult) {
      return null;
    }

    public T primitive(Type.PrimitiveType primitive) {
      return null;
    }

    protected Deque<String> fieldNames() {
      return fieldNames;
    }

    protected Deque<Integer> fieldIds() {
      return fieldIds;
    }
  }

  public static <T> T visit(Schema schema, SchemaVisitor<T> visitor) {
    return visitor.schema(schema, visit(schema.asStruct(), visitor));
  }

  public static <T> T visit(Type type, SchemaVisitor<T> visitor) {
    switch (type.typeId()) {
      case STRUCT:
        Types.StructType struct = type.asNestedType().asStructType();
        List<T> results = Lists.newArrayListWithExpectedSize(struct.fields().size());
        for (Types.NestedField field : struct.fields()) {
          visitor.fieldIds.push(field.fieldId());
          visitor.fieldNames.push(field.name());
          T result;
          try {
            result = visit(field.type(), visitor);
          } finally {
            visitor.fieldIds.pop();
            visitor.fieldNames.pop();
          }
          results.add(visitor.field(field, result));
        }
        return visitor.struct(struct, results);

      case LIST:
        Types.ListType list = type.asNestedType().asListType();
        T elementResult;

        visitor.fieldIds.push(list.elementId());
        try {
          elementResult = visit(list.elementType(), visitor);
        } finally {
          visitor.fieldIds.pop();
        }

        return visitor.list(list, elementResult);

      case MAP:
        Types.MapType map = type.asNestedType().asMapType();
        T keyResult;
        T valueResult;

        visitor.fieldIds.push(map.keyId());
        try {
          keyResult = visit(map.keyType(), visitor);
        } finally {
          visitor.fieldIds.pop();
        }

        visitor.fieldIds.push(map.valueId());
        try {
          valueResult = visit(map.valueType(), visitor);
        } finally {
          visitor.fieldIds.pop();
        }

        return visitor.map(map, keyResult, valueResult);

      default:
        return visitor.primitive(type.asPrimitiveType());
    }
  }

  public static class CustomOrderSchemaVisitor<T> {
    public T schema(Schema schema, Supplier<T> structResult) {
      return null;
    }

    public T struct(Types.StructType struct, Iterable<T> fieldResults) {
      return null;
    }

    public T field(Types.NestedField field, Supplier<T> fieldResult) {
      return null;
    }

    public T list(Types.ListType list, Supplier<T> elementResult) {
      return null;
    }

    public T map(Types.MapType map, Supplier<T> keyResult, Supplier<T> valueResult) {
      return null;
    }

    public T primitive(Type.PrimitiveType primitive) {
      return null;
    }
  }

  private static class VisitFuture<T> implements Supplier<T> {
    private final Type type;
    private final CustomOrderSchemaVisitor<T> visitor;

    private VisitFuture(Type type, CustomOrderSchemaVisitor<T> visitor) {
      this.type = type;
      this.visitor = visitor;
    }

    @Override
    public T get() {
      return visit(type, visitor);
    }
  }

  private static class VisitFieldFuture<T> implements Supplier<T> {
    private final Types.NestedField field;
    private final CustomOrderSchemaVisitor<T> visitor;

    private VisitFieldFuture(Types.NestedField field, CustomOrderSchemaVisitor<T> visitor) {
      this.field = field;
      this.visitor = visitor;
    }

    @Override
    public T get() {
      return visitor.field(field, new VisitFuture<>(field.type(), visitor));
    }
  }

  public static <T> T visit(Schema schema, CustomOrderSchemaVisitor<T> visitor) {
    return visitor.schema(schema, new VisitFuture<>(schema.asStruct(), visitor));
  }

  /**
   * Used to traverse types with traversals other than pre-order.
   * <p>
   * This passes a {@link Supplier} to each {@link CustomOrderSchemaVisitor visitor} method that
   * returns the result of traversing child types. Structs are passed an {@link Iterable} that
   * traverses child fields during iteration.
   * <p>
   * An example use is assigning column IDs, which should be done with a post-order traversal.
   *
   * @param type a type to traverse with a visitor
   * @param visitor a custom order visitor
   * @param <T> the type returned by the visitor
   * @return the result of traversing the given type with the visitor
   */
  public static <T> T visit(Type type, CustomOrderSchemaVisitor<T> visitor) {
    switch (type.typeId()) {
      case STRUCT:
        Types.StructType struct = type.asNestedType().asStructType();
        List<VisitFieldFuture<T>> results = Lists
            .newArrayListWithExpectedSize(struct.fields().size());
        for (Types.NestedField field : struct.fields()) {
          results.add(
              new VisitFieldFuture<>(field, visitor));
        }

        return visitor.struct(struct, Iterables.transform(results, VisitFieldFuture::get));

      case LIST:
        Types.ListType list = type.asNestedType().asListType();
        return visitor.list(list, new VisitFuture<>(list.elementType(), visitor));

      case MAP:
        Types.MapType map = type.asNestedType().asMapType();
        return visitor.map(map,
            new VisitFuture<>(map.keyType(), visitor),
            new VisitFuture<>(map.valueType(), visitor));

      default:
        return visitor.primitive(type.asPrimitiveType());
    }
  }

  public static class WrappedStruct extends WrappedType {
    private Iterable<WrappedType> fields;

    public WrappedStruct(Type type, Iterable<WrappedType> fields) {
      super(type);
      this.fields = fields;
    }

    public Iterable<WrappedType> getFields() {
      return fields;
    }
  }

  public static class WrappedMap extends WrappedType {
    private WrappedType key;
    private WrappedType value;

    public WrappedMap(Type type, WrappedType key, WrappedType value) {
      super(type);
      this.key = key;
      this.value = value;
    }

    public WrappedType getKey() {
      return key;
    }

    public WrappedType getValue() {
      return value;
    }
  }

  public static class WrappedList extends WrappedType {

    private WrappedType list;

    public WrappedList(Type type, WrappedType list) {
      super(type);
      this.list = list;
    }

    public WrappedType getList() {
      return list;
    }
  }

  /**
   * POJO used to model an object describing a type alongside with a String representation of its name
   * and a String representation of the nested structure of fields.
   * The nested structure of fields is flattened and we use a dot delimited order for indicating precedence.
   */
  public static class WrappedType {

    private String predecessor;
    private String name;
    private Type type;

    public WrappedType(Type type) {
      this.type = type;
    }

    public WrappedType setPredecessor(String predecessor) {
      this.predecessor = predecessor;
      return this;
    }

    public WrappedType setName(String name) {
      this.name = name;
      return this;
    }

    public String getPredecessor() {
      return predecessor;
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
    }
  }

  /**
   * Visitor for traversing two Iceberg schemas
   *
   * @param <T> the Java class returned by the visitor
   */
  public static class CompanionSchemaVisitor<T extends WrappedType> {
    protected LinkedList<String> fields = Lists.newLinkedList();
    protected LinkedList<Integer> fieldIds = Lists.newLinkedList();

    public static <T extends WrappedType> List<T> schema(Schema schema, Schema companion, CompanionSchemaVisitor<T> visitor) {
      return visitFields(schema.asStruct(), companion.asStruct(), visitor)
          .stream()
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }

    public static <T extends WrappedType> List<T> visit(Type left, Type right, CompanionSchemaVisitor<T> visitor,
        Ownership ownership) {
      switch (left.typeId()) {
        case LIST:
          Types.ListType listType = left.asListType();
          if(right != null) {
            // Handle case of commons list elements
            return Collections.emptyList();
          }
          return ImmutableList.of(visitor.list(listType,
              visit(listType.elementType(), null, visitor, ownership).get(0),
              ownership));
        case STRUCT:
          if (right != null) {
            return visitor.struct(left.asStructType(), right.asStructType(),
                visitFields(left.asStructType(), right.asStructType(), visitor));
          }
          return ImmutableList.of(visitor.struct(left.asStructType(), ownership,
              visitFields(left.asStructType(), visitor, ownership)));
        case MAP:
          if(right != null) {
            // Handle case of commons map keys
            return Collections.emptyList();
          }
          Types.MapType map = left.asNestedType().asMapType();
          T keyResult;
          T valueResult;
          visitor.fieldIds.push(map.keyId());
          try {
            keyResult = visit(map.keyType(),  null, visitor, ownership).get(0);
          } finally {
            visitor.fieldIds.pop();
          }
          visitor.fieldIds.push(map.valueId());
          try {
            valueResult = visit(map.valueType(), null, visitor, ownership).get(0);
          } finally {
            visitor.fieldIds.pop();
          }
          return ImmutableList.of(visitor.map(map, keyResult, valueResult, ownership));
        default:
          List<T> primitives = Lists.newArrayList();
          primitives.add(visitor.primitive(left.asPrimitiveType(), ownership));
          if (right != null) {
            // No way of telling whether it's the left-side or right-side element we're visiting in the case of a
            // common primitive field.
            // This makes the visitor.primitive API rather vague, so need more explicit API contract for this one.
            primitives.add(visitor.primitive(right.asPrimitiveType(), ownership));
          }
          return primitives;
      }
    }

    private static <T extends WrappedType> List<T> visitField(
        Types.NestedField head,
        Types.NestedField companion,
        CompanionSchemaVisitor<T> visitor,
        Ownership ownership) {
      visitor.fields.push(head.name());
      try {
        return visit(head.type(), (companion != null) ? companion.type() : null, visitor, ownership);
      } finally {
        visitor.fields.pop();
      }
    }

    private static <T extends WrappedType> List<T> visitFields(
        Types.StructType head,
        CompanionSchemaVisitor<T> visitor,
        Ownership ownership) {
      List<T> results = Lists.newArrayListWithExpectedSize(head.fields().size());
      for (Types.NestedField field : head.fields()) {
        results.addAll(visitField(field, null, visitor, ownership));
      }
      return results;
    }

    private static <T extends WrappedType> List<T> visitFields(
        Types.StructType head,
        Types.StructType companion,
        CompanionSchemaVisitor<T> visitor) {

      // Used to compute the intersection of fields based on fields matching type id, field name and required option.
      Comparator<Types.NestedField> matchingFieldComparator = Comparator.comparing(Types.NestedField::name)
          .thenComparing(Types.NestedField::isOptional)
          .thenComparing(t -> t.type().typeId());

      TreeSet<Types.NestedField> l = Sets.newTreeSet(matchingFieldComparator);
      l.addAll(head.fields());
      TreeSet<Types.NestedField> r = Sets.newTreeSet(matchingFieldComparator);
      r.addAll(companion.fields());

      HashSet<Types.NestedField> intersection = Sets.intersection(l, r).copyInto(new HashSet<>());

      // Collect the intersection ids so we can later filter out intersection elements from the respective original
      // collections leaving out left-side and right-side only fields left to visit.
      List<Integer> collectLeftSideIntersectIds = Lists.newArrayListWithCapacity(intersection.size());
      List<Integer> collectRightSideIntersectIds = Lists.newArrayListWithCapacity(intersection.size());
      List<T> results = Lists.newArrayListWithExpectedSize(l.size() + r.size() - intersection.size());

      // Having established the intersection fields we will proceed to look-up each in their respective original
      // collections (complexity/cost is very high, needs improvement) finding matching elements and visiting them as
      for (Types.NestedField nestedField : intersection) {
        Types.NestedField leftSideIntersect =
            l.stream().filter(f -> f.type().typeId().equals(nestedField.type().typeId()))
                .filter(f -> f.name().equalsIgnoreCase(nestedField.name()))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Expected a left-side match."));

        Types.NestedField rightSideIntersect =
            r.stream().filter(f -> f.type().typeId().equals(nestedField.type().typeId()))
                .filter(f -> f.name().equalsIgnoreCase(nestedField.name()))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Expected a right-side match."));

        results.addAll(visitField(leftSideIntersect, rightSideIntersect, visitor, Ownership.BOTH));

        l.remove(leftSideIntersect);
        r.remove(rightSideIntersect);

        collectLeftSideIntersectIds.add(leftSideIntersect.fieldId());
        collectRightSideIntersectIds.add(rightSideIntersect.fieldId());
      }

      List<Types.NestedField> leftSideOnlyFields = head.fields();
      if(!collectLeftSideIntersectIds.isEmpty()) {
        leftSideOnlyFields =
            head.fields()
                .stream()
                .filter(f -> !collectLeftSideIntersectIds.contains(f.fieldId()))
                .collect(Collectors.toList());
      }

      List<Types.NestedField> rightSideOnlyFields = companion.fields();
      if(!collectRightSideIntersectIds.isEmpty()) {
        rightSideOnlyFields = companion.fields()
            .stream()
            .filter(f -> !collectRightSideIntersectIds.contains(f.fieldId()))
            .collect(Collectors.toList());
      }

      for (Types.NestedField nestedField : leftSideOnlyFields) {
        results.addAll(visitField(nestedField, null, visitor, Ownership.L));
      }

      for (Types.NestedField nestedField : rightSideOnlyFields) {
        results.addAll(visitField(nestedField, null, visitor, Ownership.R));
      }

      return results
          .stream()
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }

    public T schema(Type left, Type right, T result) {return null;}

    public T struct(Types.StructType type, Ownership ownership, Iterable<T> fields) {
      return null;
    }

    public List<T> struct(Types.StructType typeLeft, Types.StructType typeRight, Iterable<T> fields) {
      return null;
    }

    public T list(Types.ListType list, T element, Ownership ownership) {
      return null;
    }

    public T map(Types.MapType map, T key, T value, Ownership ownership) {
      return null;
    }

    public T field(Types.NestedField field, T fieldResult) {
      return null;
    }

    public T primitive(Type.PrimitiveType type, Ownership ownership) {
      return null;
    }

    protected String predecessor() {
      return String.join(".", currentPath(1));
    }

    protected String fieldName() {
      if (!fields.isEmpty()) {
        return fields.iterator().next();
      }
      throw new IllegalArgumentException("Failed to resolve field name from null fields.");
    }

    private List<String> currentPath(int descendent) {
      LinkedList<String> local = new LinkedList<>(fields);
      while (descendent > 0) {
        descendent--;
        local.removeFirst();
      }
      List<String> path = Lists.newArrayList();
      local.descendingIterator().forEachRemaining(path::add);
      return path;
    }

    public enum Ownership {
      L("LEFT"),
      R("RIGHT"),
      BOTH("BOTH");

      private String label;

      Ownership(String label) {
        this.label = label;
      }

      public String getLabel() {
        return label;
      }
    }
  }

  static int decimalMaxPrecision(int numBytes) {
    Preconditions.checkArgument(numBytes >= 0 && numBytes < 24,
        "Unsupported decimal length: %s", numBytes);
    return MAX_PRECISION[numBytes];
  }

  public static int decimalRequriedBytes(int precision) {
    Preconditions.checkArgument(precision >= 0 && precision < 40,
        "Unsupported decimal precision: %s", precision);
    return REQUIRED_LENGTH[precision];
  }

  private static final int[] MAX_PRECISION = new int[24];
  private static final int[] REQUIRED_LENGTH = new int[40];

  static {
    // for each length, calculate the max precision
    for (int len = 0; len < MAX_PRECISION.length; len += 1) {
      MAX_PRECISION[len] = (int) Math.floor(Math.log10(Math.pow(2, 8 * len - 1) - 1));
    }

    // for each precision, find the first length that can hold it
    for (int precision = 0; precision < REQUIRED_LENGTH.length; precision += 1) {
      REQUIRED_LENGTH[precision] = -1;
      for (int len = 0; len < MAX_PRECISION.length; len += 1) {
        // find the first length that can hold the precision
        if (precision <= MAX_PRECISION[len]) {
          REQUIRED_LENGTH[precision] = len;
          break;
        }
      }
      if (REQUIRED_LENGTH[precision] < 0) {
        throw new IllegalStateException(
            "Could not find required length for precision " + precision);
      }
    }
  }
}
