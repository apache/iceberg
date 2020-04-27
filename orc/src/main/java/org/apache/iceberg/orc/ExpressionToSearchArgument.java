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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Bound;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.io.sarg.PredicateLeaf;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.storage.serde2.io.HiveDecimalWritable;

public class ExpressionToSearchArgument extends ExpressionVisitors.BoundVisitor<ExpressionToSearchArgument.Action> {

  static SearchArgument visit(Expression expr, TypeDescription readSchema) {
    Map<Integer, String> idToColumnName = TypeUtil.visit(ORCSchemaUtil.convert(readSchema), new IdToQuotedColumnName());
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    ExpressionVisitors.visit(expr, new ExpressionToSearchArgument(builder, idToColumnName)).invoke();
    return builder.build();
  }

  // Currently every predicate in ORC requires a PredicateLeaf.Type field which is not available for these Iceberg types
  private static final Set<TypeID> UNSUPPORTED_TYPES = ImmutableSet.of(
      TypeID.BINARY,
      TypeID.FIXED,
      TypeID.UUID,
      TypeID.STRUCT,
      TypeID.MAP,
      TypeID.LIST
  );

  private SearchArgument.Builder builder;
  private Map<Integer, String> idToColumnName;

  private ExpressionToSearchArgument(SearchArgument.Builder builder, Map<Integer, String> idToColumnName) {
    this.builder = builder;
    this.idToColumnName = idToColumnName;
  }

  @Override
  public Action alwaysTrue() {
    return () -> this.builder.literal(TruthValue.YES);
  }

  @Override
  public Action alwaysFalse() {
    return () -> this.builder.literal(TruthValue.NO);
  }

  @Override
  public Action not(Action child) {
    return () -> {
      this.builder.startNot();
      child.invoke();
      this.builder.end();
    };
  }

  @Override
  public Action and(Action leftChild, Action rightChild) {
    return () -> {
      this.builder.startAnd();
      leftChild.invoke();
      rightChild.invoke();
      this.builder.end();
    };
  }

  @Override
  public Action or(Action leftChild, Action rightChild) {
    return () -> {
      this.builder.startOr();
      leftChild.invoke();
      rightChild.invoke();
      this.builder.end();
    };
  }

  @Override
  public <T> Action isNull(Bound<T> expr) {
    return () -> this.builder.isNull(idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()));
  }

  @Override
  public <T> Action notNull(Bound<T> expr) {
    return () -> this.builder.startNot()
        .isNull(idToColumnName.get(expr.ref().fieldId()),
            type(expr.ref().type()))
        .end();
  }

  @Override
  public <T> Action lt(Bound<T> expr, Literal<T> lit) {
    return () -> this.builder.lessThan(idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()),
        literal(expr.ref().type(), lit.value()));
  }

  @Override
  public <T> Action ltEq(Bound<T> expr, Literal<T> lit) {
    return () -> this.builder.lessThanEquals(idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()),
        literal(expr.ref().type(), lit.value()));
  }

  @Override
  public <T> Action gt(Bound<T> expr, Literal<T> lit) {
    return () -> this.builder.startNot()
          .lessThanEquals(idToColumnName.get(expr.ref().fieldId()),
              type(expr.ref().type()),
              literal(expr.ref().type(), lit.value()))
          .end();
  }

  @Override
  public <T> Action gtEq(Bound<T> expr, Literal<T> lit) {
    return () -> this.builder.startNot()
          .lessThan(idToColumnName.get(expr.ref().fieldId()),
              type(expr.ref().type()),
              literal(expr.ref().type(), lit.value()))
          .end();
  }

  @Override
  public <T> Action eq(Bound<T> expr, Literal<T> lit) {
    return () -> this.builder.equals(idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()),
        literal(expr.ref().type(), lit.value()));
  }

  @Override
  public <T> Action notEq(Bound<T> expr, Literal<T> lit) {
    return () -> this.builder.startNot()
          .equals(idToColumnName.get(expr.ref().fieldId()),
              type(expr.ref().type()),
              literal(expr.ref().type(), lit.value()))
          .end();
  }

  @Override
  public <T> Action in(Bound<T> expr, Set<T> literalSet) {
    return () -> this.builder.in(
        idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()),
        literalSet.stream().map(lit -> literal(expr.ref().type(), lit)).toArray(Object[]::new));
  }

  @Override
  public <T> Action notIn(Bound<T> expr, Set<T> literalSet) {
    return () -> this.builder.startNot()
          .in(idToColumnName.get(expr.ref().fieldId()), type(expr.ref().type()),
              literalSet.stream().map(lit -> literal(expr.ref().type(), lit)).toArray(Object[]::new))
          .end();
  }

  @Override
  public <T> Action startsWith(Bound<T> expr, Literal<T> lit) {
    // Cannot push down STARTS_WITH operator to ORC, so return TruthValue.YES_NO_NULL which signifies
    // that this predicate cannot help with filtering
    return () -> this.builder.literal(TruthValue.YES_NO_NULL);
  }

  @Override
  public <T> Action predicate(BoundPredicate<T> pred) {
    if (UNSUPPORTED_TYPES.contains(pred.ref().type().typeId())) {
      // Cannot push down predicates for types which cannot be represented in PredicateLeaf.Type, so return
      // TruthValue.YES_NO_NULL which signifies that this predicate cannot help with filtering
      return () -> this.builder.literal(TruthValue.YES_NO_NULL);
    } else {
      return super.predicate(pred);
    }
  }

  @FunctionalInterface
  interface Action {
    void invoke();
  }

  private PredicateLeaf.Type type(Type icebergType) {
    switch (icebergType.typeId()) {
      case BOOLEAN:
        return PredicateLeaf.Type.BOOLEAN;
      case INTEGER:
      case LONG:
      case TIME:
        return PredicateLeaf.Type.LONG;
      case FLOAT:
      case DOUBLE:
        return PredicateLeaf.Type.FLOAT;
      case DATE:
        return PredicateLeaf.Type.DATE;
      case TIMESTAMP:
        return PredicateLeaf.Type.TIMESTAMP;
      case STRING:
        return PredicateLeaf.Type.STRING;
      case DECIMAL:
        return PredicateLeaf.Type.DECIMAL;
      default:
        throw new UnsupportedOperationException("Type " + icebergType + " not supported in ORC SearchArguments");
    }
  }

  private <T> Object literal(Type icebergType, T icebergLiteral) {
    switch (icebergType.typeId()) {
      case BOOLEAN:
      case LONG:
      case TIME:
      case DOUBLE:
        return icebergLiteral;
      case INTEGER:
        return ((Integer) icebergLiteral).longValue();
      case FLOAT:
        return ((Float) icebergLiteral).doubleValue();
      case STRING:
        return icebergLiteral.toString();
      case DATE:
        return Date.valueOf(LocalDate.ofEpochDay((Integer) icebergLiteral));
      case TIMESTAMP:
        long microsFromEpoch = (Long) icebergLiteral;
        return Timestamp.from(Instant.ofEpochSecond(Math.floorDiv(microsFromEpoch, 1_000_000),
            (microsFromEpoch % 1_000_000) * 1_000));
      case DECIMAL:
        return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) icebergLiteral, false));
      default:
        throw new UnsupportedOperationException("Type " + icebergType + " not supported in ORC SearchArguments");
    }
  }

  /**
   * Generates mapping from field IDs to fully qualified column names compatible with ORC convention for a given
   * {@link Schema}
   *
   * This visitor also enclose column names in backticks i.e. ` so that ORC can correctly parse column names with
   * special characters. A comparison of ORC convention with Iceberg convention is provided below
   * <pre>
   *                                      Iceberg           ORC
   * field                                field             field
   * struct -> field                      struct.field      struct.field
   * list -> element                      list.element      list._elem
   * list -> struct element -> field      list.field        list._elem.field
   * map -> key                           map.key           map._key
   * map -> value                         map.value         map._value
   * map -> struct key -> field           map.key.field     map._key.field
   * map -> struct value -> field         map.field         map._value.field
   * </pre>
   */
  static class IdToQuotedColumnName extends TypeUtil.CustomOrderSchemaVisitor<Map<Integer, String>> {
    private static final Joiner DOT = Joiner.on(".");

    private final Deque<String> fieldNames = Lists.newLinkedList();
    private final Map<Integer, String> idToName = Maps.newHashMap();

    @Override
    public Map<Integer, String> schema(Schema schema, Supplier<Map<Integer, String>> structResult) {
      return structResult.get();
    }

    @Override
    public Map<Integer, String> struct(Types.StructType struct, Iterable<Map<Integer, String>> fieldResults) {
      // iterate through the fields to generate column names for each one, use size to avoid errorprone failure
      Lists.newArrayList(fieldResults).size();
      return idToName;
    }

    @Override
    public Map<Integer, String> field(Types.NestedField field, Supplier<Map<Integer, String>> fieldResult) {
      withName(field.name(), fieldResult::get);
      addField(field.name(), field.fieldId());
      return null;
    }

    @Override
    public Map<Integer, String> list(Types.ListType list, Supplier<Map<Integer, String>> elementResult) {
      withName("_elem", elementResult::get);
      addField("_elem", list.elementId());
      return null;
    }

    @Override
    public Map<Integer, String> map(Types.MapType map,
        Supplier<Map<Integer, String>> keyResult,
        Supplier<Map<Integer, String>> valueResult) {
      withName("_key", keyResult::get);
      withName("_value", valueResult::get);
      addField("_key", map.keyId());
      addField("_value", map.valueId());
      return null;
    }

    private <T> T withName(String name, Callable<T> callable) {
      fieldNames.push(name);
      try {
        return callable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        fieldNames.pop();
      }
    }

    private void addField(String name, int fieldId) {
      withName(name, () -> {
        return idToName.put(fieldId, DOT.join(Iterables.transform(fieldNames::descendingIterator, this::quoteName)));
      });
    }

    private String quoteName(String name) {
      String escapedName = name.replace("`", "``"); // if the column name contains ` then escape it with another `
      return "`" + escapedName + "`";
    }
  }
}
