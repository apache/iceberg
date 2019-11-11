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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;

public class IndexByName extends TypeUtil.CustomOrderSchemaVisitor<Map<String, Integer>> {
  private static final Joiner DOT = Joiner.on(".");

  private final Deque<String> fieldNames = Lists.newLinkedList();
  private final Map<String, Integer> nameToId = Maps.newHashMap();

  @Override
  public Map<String, Integer> schema(Schema schema, Supplier<Map<String, Integer>> structResult) {
    return structResult.get();
  }

  @Override
  public Map<String, Integer> struct(Types.StructType struct, Iterable<Map<String, Integer>> fieldResults) {
    // iterate through the fields to update the index for each one, use size to avoid errorprone failure
    Lists.newArrayList(fieldResults).size();
    return nameToId;
  }

  @Override
  public Map<String, Integer> field(Types.NestedField field, Supplier<Map<String, Integer>> fieldResult) {
    withName(field.name(), fieldResult::get);
    addField(field.name(), field.fieldId());
    return null;
  }

  @Override
  public Map<String, Integer> list(Types.ListType list, Supplier<Map<String, Integer>> elementResult) {
    // add element
    for (Types.NestedField field : list.fields()) {
      addField(field.name(), field.fieldId());
    }

    if (list.elementType().isStructType()) {
      // return to avoid errorprone failure
      return elementResult.get();
    }

    withName("element", elementResult::get);

    return null;
  }

  @Override
  public Map<String, Integer> map(Types.MapType map,
                                  Supplier<Map<String, Integer>> keyResult,
                                  Supplier<Map<String, Integer>> valueResult) {
    withName("key", keyResult::get);

    // add key and value
    for (Types.NestedField field : map.fields()) {
      addField(field.name(), field.fieldId());
    }

    if (map.valueType().isStructType()) {
      // return to avoid errorprone failure
      return valueResult.get();
    }

    withName("value", valueResult::get);

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
