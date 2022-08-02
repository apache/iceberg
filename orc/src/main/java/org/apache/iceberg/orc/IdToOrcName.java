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

import java.util.Deque;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Generates mapping from field IDs to ORC qualified names.
 *
 * <p>This visitor also enclose column names in backticks i.e. ` so that ORC can correctly parse
 * column names with special characters. A comparison of ORC convention with Iceberg convention is
 * provided below
 *
 * <pre>{@code
 *                                      Iceberg           ORC
 * field                                field             field
 * struct -> field                      struct.field      struct.field
 * list -> element                      list.element      list._elem
 * list -> struct element -> field      list.field        list._elem.field
 * map -> key                           map.key           map._key
 * map -> value                         map.value         map._value
 * map -> struct key -> field           map.key.field     map._key.field
 * map -> struct value -> field         map.field         map._value.field
 * }</pre>
 */
class IdToOrcName extends TypeUtil.SchemaVisitor<Map<Integer, String>> {
  private static final Joiner DOT = Joiner.on(".");

  private final Deque<String> fieldNames = Lists.newLinkedList();
  private final Map<Integer, String> idToName = Maps.newHashMap();

  @Override
  public void beforeField(Types.NestedField field) {
    fieldNames.push(field.name());
  }

  @Override
  public void afterField(Types.NestedField field) {
    fieldNames.pop();
  }

  @Override
  public void beforeListElement(Types.NestedField elementField) {
    fieldNames.push("_elem");
  }

  @Override
  public void afterListElement(Types.NestedField elementField) {
    fieldNames.pop();
  }

  @Override
  public void beforeMapKey(Types.NestedField keyField) {
    fieldNames.push("_key");
  }

  @Override
  public void afterMapKey(Types.NestedField keyField) {
    fieldNames.pop();
  }

  @Override
  public void beforeMapValue(Types.NestedField valueField) {
    fieldNames.push("_value");
  }

  @Override
  public void afterMapValue(Types.NestedField valueField) {
    fieldNames.pop();
  }

  @Override
  public Map<Integer, String> schema(Schema schema, Map<Integer, String> structResult) {
    return structResult;
  }

  @Override
  public Map<Integer, String> struct(
      Types.StructType struct, List<Map<Integer, String>> fieldResults) {
    return idToName;
  }

  @Override
  public Map<Integer, String> field(Types.NestedField field, Map<Integer, String> fieldResult) {
    addField(field.name(), field.fieldId());
    return idToName;
  }

  @Override
  public Map<Integer, String> list(Types.ListType list, Map<Integer, String> elementResult) {
    addField("_elem", list.elementId());
    return idToName;
  }

  @Override
  public Map<Integer, String> map(
      Types.MapType map, Map<Integer, String> keyResult, Map<Integer, String> valueResult) {
    addField("_key", map.keyId());
    addField("_value", map.valueId());
    return idToName;
  }

  @Override
  public Map<Integer, String> primitive(Type.PrimitiveType primitive) {
    return idToName;
  }

  private void addField(String name, int fieldId) {
    List<String> fullName = Lists.newArrayList(fieldNames.descendingIterator());
    fullName.add(name);
    idToName.put(fieldId, DOT.join(Iterables.transform(fullName, this::quoteName)));
  }

  private String quoteName(String name) {
    String escapedName =
        name.replace("`", "``"); // if the column name contains ` then escape it with another `
    return "`" + escapedName + "`";
  }
}
