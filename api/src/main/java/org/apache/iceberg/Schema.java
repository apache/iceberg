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

package org.apache.iceberg;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.BiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableBiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
 * The schema of a data table.
 * <p>
 * Schema ID will only be populated when reading from/writing to table metadata,
 * otherwise it will be default to 0.
 */
public class Schema implements Serializable {
  private static final Joiner NEWLINE = Joiner.on('\n');
  private static final String ALL_COLUMNS = "*";
  private static final int DEFAULT_SCHEMA_ID = 0;

  private final StructType struct;
  private final int schemaId;
  private transient BiMap<String, Integer> aliasToId = null;
  private transient Map<Integer, NestedField> idToField = null;
  private transient Map<String, Integer> nameToId = null;
  private transient Map<String, Integer> lowerCaseNameToId = null;
  private transient Map<Integer, Accessor<StructLike>> idToAccessor = null;
  private transient Map<Integer, String> idToName = null;

  public Schema(List<NestedField> columns, Map<String, Integer> aliases) {
    this.schemaId = DEFAULT_SCHEMA_ID;
    this.struct = StructType.of(columns);
    this.aliasToId = aliases != null ? ImmutableBiMap.copyOf(aliases) : null;

    // validate the schema through IndexByName visitor
    lazyIdToName();
  }

  public Schema(List<NestedField> columns) {
    this(DEFAULT_SCHEMA_ID, columns);
  }

  public Schema(int schemaId, List<NestedField> columns) {
    this.schemaId = schemaId;
    this.struct = StructType.of(columns);
    lazyIdToName();
  }

  public Schema(NestedField... columns) {
    this(DEFAULT_SCHEMA_ID, Arrays.asList(columns));
  }

  public Schema(int schemaId, NestedField... columns) {
    this(schemaId, Arrays.asList(columns));
  }

  private Map<Integer, NestedField> lazyIdToField() {
    if (idToField == null) {
      this.idToField = TypeUtil.indexById(struct);
    }
    return idToField;
  }

  private Map<String, Integer> lazyNameToId() {
    if (nameToId == null) {
      this.nameToId = ImmutableMap.copyOf(TypeUtil.indexByName(struct));
    }
    return nameToId;
  }

  private Map<Integer, String> lazyIdToName() {
    if (idToName == null) {
      this.idToName = ImmutableMap.copyOf(TypeUtil.indexNameById(struct));
    }
    return idToName;
  }

  private Map<String, Integer> lazyLowerCaseNameToId() {
    if (lowerCaseNameToId == null) {
      this.lowerCaseNameToId = ImmutableMap.copyOf(TypeUtil.indexByLowerCaseName(struct));
    }
    return lowerCaseNameToId;
  }

  private Map<Integer, Accessor<StructLike>> lazyIdToAccessor() {
    if (idToAccessor == null) {
      idToAccessor = Accessors.forSchema(this);
    }
    return idToAccessor;
  }

  /**
   * Returns the schema ID for this schema.
   * <p>
   * Note that schema ID will only be populated when reading from/writing to table metadata,
   * otherwise it will be default to 0.
   */
  public int schemaId() {
    return this.schemaId;
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
   * Returns the underlying {@link StructType struct type} for this schema.
   *
   * @return the StructType version of this schema.
   */
  public StructType asStruct() {
    return struct;
  }

  /**
   * Returns a List of the {@link NestedField columns} in this Schema.
   */
  public List<NestedField> columns() {
    return struct.fields();
  }

  /**
   * Returns the {@link Type} of a sub-field identified by the field name.
   *
   * @param name a field name
   * @return a Type for the sub-field or null if it is not found
   */
  public Type findType(String name) {
    Preconditions.checkArgument(!name.isEmpty(), "Invalid column name: (empty)");
    Integer id = lazyNameToId().get(name);
    if (id != null) {  // name is found
      return findType(id);
    }

    // name could not be found
    return null;
  }

  /**
   * Returns the {@link Type} of a sub-field identified by the field id.
   *
   * @param id a field id
   * @return a Type for the sub-field or null if it is not found
   */
  public Type findType(int id) {
    NestedField field = lazyIdToField().get(id);
    if (field != null) {
      return field.type();
    }
    return null;
  }

  /**
   * Returns the sub-field identified by the field id as a {@link NestedField}.
   *
   * @param id a field id
   * @return the sub-field or null if it is not found
   */
  public NestedField findField(int id) {
    return lazyIdToField().get(id);
  }

  /**
   * Returns a sub-field by name as a {@link NestedField}.
   * <p>
   * The result may be a top-level or a nested field.
   *
   * @param name a String name
   * @return a Type for the sub-field or null if it is not found
   */
  public NestedField findField(String name) {
    Preconditions.checkArgument(!name.isEmpty(), "Invalid column name: (empty)");
    Integer id = lazyNameToId().get(name);
    if (id != null) {
      return lazyIdToField().get(id);
    }
    return null;
  }

  /**
   * Returns a sub-field by name as a {@link NestedField}.
   * <p>
   * The result may be a top-level or a nested field.
   *
   * @param name a String name
   * @return the sub-field or null if it is not found
   */
  public NestedField caseInsensitiveFindField(String name) {
    Preconditions.checkArgument(!name.isEmpty(), "Invalid column name: (empty)");
    Integer id = lazyLowerCaseNameToId().get(name.toLowerCase(Locale.ROOT));
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
    return lazyIdToName().get(id);
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
   * Returns the full column name in the unconverted data schema for the given column id.
   * Column aliases are set by conversions from Parquet or Avro to this Schema type.
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
   * Returns an accessor for retrieving the data from {@link StructLike}.
   * <p>
   * Accessors do not retrieve data contained in lists or maps.
   *
   * @param id a column id in this schema
   * @return an {@link Accessor} to retrieve values from a {@link StructLike} row
   */
  public Accessor<StructLike> accessorForField(int id) {
    return lazyIdToAccessor().get(id);
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
    return internalSelect(names, true);
  }

  /**
   * Creates a projection schema for a subset of columns, selected by case insensitive names
   * <p>
   * Names that identify nested fields will select part or all of the field's top-level column.
   *
   * @param names a List of String names for selected columns
   * @return a projection schema from this schema, by names
   */
  public Schema caseInsensitiveSelect(String... names) {
    return caseInsensitiveSelect(Arrays.asList(names));
  }

  /**
   * Creates a projection schema for a subset of columns, selected by case insensitive names
   * <p>
   * Names that identify nested fields will select part or all of the field's top-level column.
   *
   * @param names a List of String names for selected columns
   * @return a projection schema from this schema, by names
   */
  public Schema caseInsensitiveSelect(Collection<String> names) {
    return internalSelect(names, false);
  }

  private Schema internalSelect(Collection<String> names, boolean caseSensitive) {
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
            .map(f -> "  " + f)
            .collect(Collectors.toList())));
  }
}
