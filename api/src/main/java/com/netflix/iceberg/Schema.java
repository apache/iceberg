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

package com.netflix.iceberg;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Sets;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.TypeUtil;
import com.netflix.iceberg.types.Types;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The schema of a data table.
 */
public class Schema implements Serializable {
  private static final Joiner NEWLINE = Joiner.on('\n');
  private static final String ALL_COLUMNS = "*";

  private final Types.StructType struct;
  private transient BiMap<String, Integer> aliasToId = null;
  private transient Map<Integer, Types.NestedField> idToField = null;
  private transient BiMap<String, Integer> nameToId = null;
  private transient BiMap<String, Integer> lowerCaseNameToId = null;

  public Schema(List<Types.NestedField> columns, Map<String, Integer> aliases) {
    this.struct = Types.StructType.of(columns);
    this.aliasToId = aliases != null ? ImmutableBiMap.copyOf(aliases) : null;
  }

  public Schema(List<Types.NestedField> columns) {
    this.struct = Types.StructType.of(columns);
  }

  private Map<Integer, Types.NestedField> lazyIdToField() {
    if (idToField == null) {
      this.idToField = TypeUtil.indexById(struct);
    }
    return idToField;
  }

  private BiMap<String, Integer> lazyNameToId() {
    if (nameToId == null) {
      this.nameToId = ImmutableBiMap.copyOf(TypeUtil.indexByName(struct));
    }
    return nameToId;
  }

  private BiMap<String, Integer> lazyLowerCaseNameToId() {
    if (lowerCaseNameToId == null) {
      this.lowerCaseNameToId = ImmutableBiMap.copyOf(TypeUtil.indexByLowerCaseName(struct));
    }
    return lowerCaseNameToId;
  }

  public Schema(Types.NestedField... columns) {
    this(Arrays.asList(columns));
  }

  /**
   * Returns an alias map for this schema, if set.
   * <p>
   * Alias maps are created when translating an external schema, like an Avro Schema, to this
   * format. The original column names can be provided in a Map when constructing this Schema.
   *
   * @return a Map of column aliases to field ids
   */
  public Map<String, Integer> getAliases() {
    return aliasToId;
  }

  /**
   * Returns the underlying {@link Types.StructType struct type} for this schema.
   *
   * @return the StructType version of this schema.
   */
  public Types.StructType asStruct() {
    return struct;
  }

  /**
   * @return a List of the {@link Types.NestedField columns} in this Schema.
   */
  public List<Types.NestedField> columns() {
    return struct.fields();
  }

  public Type findType(String name) {
    Preconditions.checkArgument(!name.isEmpty(), "Invalid column name: (empty)");
    return findType(lazyNameToId().get(name));
  }

  /**
   * Returns the {@link Type} of a sub-field identified by the field id.
   *
   * @param id a field id
   * @return a Type for the sub-field or null if it is not found
   */
  public Type findType(int id) {
    Types.NestedField field = lazyIdToField().get(id);
    if (field != null) {
      return field.type();
    }
    return null;
  }

  /**
   * Returns the sub-field identified by the field id as a {@link Types.NestedField}.
   *
   * @param id a field id
   * @return the sub-field or null if it is not found
   */
  public Types.NestedField findField(int id) {
    return lazyIdToField().get(id);
  }

  /**
   * Returns a sub-field field by name as a {@link Types.NestedField}.
   * <p>
   * The result may be a nested field.
   *
   * @param name a String name
   * @return a Type for the sub-field or null if it is not found
   */
  public Types.NestedField findField(String name) {
    Preconditions.checkArgument(!name.isEmpty(), "Invalid column name: (empty)");
    Integer id = lazyNameToId().get(name);
    if (id != null) {
      return lazyIdToField().get(id);
    }
    return null;
  }

  /**
   * Returns the full column name for the given id.
   *
   * @param id a field id
   * @return the full column name in this schema that resolves to the id
   */
  public String findColumnName(int id) {
    return lazyNameToId().inverse().get(id);
  }

  /**
   * Returns the column id for the given column alias. Column aliases are set
   * by conversions from Parquet or Avro to this Schema type.
   *
   * @param alias a full column name in the unconverted data schema
   * @return the column id in this schema, or null if the column wasn't found
   */
  public Integer aliasToId(String alias) {
    if (aliasToId != null) {
      return aliasToId.get(alias);
    }
    return null;
  }

  /**
   * Returns the column id for the given column alias. Column aliases are set
   * by conversions from Parquet or Avro to this Schema type.
   *
   * @param fieldId a column id in this schema
   * @return the full column name in the unconverted data schema, or null if one wasn't found
   */
  public String idToAlias(Integer fieldId) {
    if (aliasToId != null) {
      return aliasToId.inverse().get(fieldId);
    }
    return null;
  }

  /**
   * Creates a projection schema for a subset of columns, selected by name.
   * <p>
   * Names that identify nested fields will select part or all of the field's top-level column.
   *
   * @param names String names for selected columns
   * @return a projection schema from this schema, by name
   */
  public Schema select(String... names) {
    return select(Arrays.asList(names));
  }

  /**
   * Creates a projection schema for a subset of columns, selected by name.
   * <p>
   * Names that identify nested fields will select part or all of the field's top-level column.
   *
   * @param names a List of String names for selected columns
   * @return a projection schema from this schema, by name
   */
  public Schema select(Collection<String> names) {
    return select(names, true);
  }

  /**
   * Creates a projection schema for a subset of columns, selected by name, and with the specified case sensitivity.
   * <p>
   * Names that identify nested fields will select part or all of the field's top-level column.
   *
   * @param names a List of String names for selected columns
   * @return a projection schema from this schema, by name
   */
  public Schema select(Collection<String> names, boolean caseSensitive) {
    if (names.contains(ALL_COLUMNS)) {
      return this;
    }

    Set<Integer> selected = Sets.newHashSet();
    for (String name : names) {
      Integer id;
      if (caseSensitive) {
        id = lazyNameToId().get(name);
      } else {
        id = lazyLowerCaseNameToId().get(name.toLowerCase(Locale.ROOT));
      }

      if (id != null) {
        selected.add(id);
      }
    }

    return TypeUtil.select(this, selected);
  }

  @Override
  public String toString() {
    return String.format("table {\n%s\n}",
        NEWLINE.join(struct.fields().stream()
            .map(f -> "  " + f + (f.doc() == null ? "" : " COMMENT '" + f.doc() + "'"))
            .collect(Collectors.toList())));
  }
}
