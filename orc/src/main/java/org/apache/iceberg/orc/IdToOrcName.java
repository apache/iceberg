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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Generates mapping from field IDs to ORC qualified names.
 * <p>
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
class IdToOrcName extends TypeUtil.CustomOrderSchemaVisitor<Map<Integer, String>> {
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
