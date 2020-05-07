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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;

public class IndexByName extends TypeUtil.SchemaVisitor<Map<String, Integer>> {
  private static final Joiner DOT = Joiner.on(".");

  private final Deque<String> fieldNames = Lists.newLinkedList();
  private final Map<String, Integer> nameToId = Maps.newHashMap();

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
    // only add "element" to the name if the element is not a struct, so that names are more natural
    // for example, locations.latitude instead of locations.element.latitude
    if (!elementField.type().isStructType()) {
      beforeField(elementField);
    }
  }

  @Override
  public void afterListElement(Types.NestedField elementField) {
    // only remove "element" if it was added
    if (!elementField.type().isStructType()) {
      afterField(elementField);
    }
  }

  @Override
  public void beforeMapKey(Types.NestedField keyField) {
    beforeField(keyField);
  }

  @Override
  public void afterMapKey(Types.NestedField keyField) {
    afterField(keyField);
  }

  @Override
  public void beforeMapValue(Types.NestedField valueField) {
    // only add "value" to the name if the value is not a struct, so that names are more natural
    if (!valueField.type().isStructType()) {
      beforeField(valueField);
    }
  }

  @Override
  public void afterMapValue(Types.NestedField valueField) {
    // only remove "value" if it was added
    if (!valueField.type().isStructType()) {
      afterField(valueField);
    }
  }

  @Override
  public Map<String, Integer> schema(Schema schema, Map<String, Integer> structResult) {
    return nameToId;
  }

  @Override
  public Map<String, Integer> struct(Types.StructType struct, List<Map<String, Integer>> fieldResults) {
    return nameToId;
  }

  @Override
  public Map<String, Integer> field(Types.NestedField field, Map<String, Integer> fieldResult) {
    addField(field.name(), field.fieldId());
    return nameToId;
  }

  @Override
  public Map<String, Integer> list(Types.ListType list, Map<String, Integer> elementResult) {
    addField("element", list.elementId());
    return nameToId;
  }

  @Override
  public Map<String, Integer> map(Types.MapType map, Map<String, Integer> keyResult, Map<String, Integer> valueResult) {
    addField("key", map.keyId());
    addField("value", map.valueId());
    return nameToId;
  }

  @Override
  public Map<String, Integer> primitive(Type.PrimitiveType primitive) {
    return nameToId;
  }

  private void addField(String name, int fieldId) {
    String fullName = name;
    if (!fieldNames.isEmpty()) {
      fullName = DOT.join(DOT.join(fieldNames.descendingIterator()), name);
    }

    Integer existingFieldId = nameToId.put(fullName, fieldId);
    if (existingFieldId != null && !"element".equals(name) && !"value".equals(name)) {
      throw new ValidationException(
          "Invalid schema: multiple fields for name %s: %s and %s", fullName, existingFieldId, fieldId);
    }
  }
}
