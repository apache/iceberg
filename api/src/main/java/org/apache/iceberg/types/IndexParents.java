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

import java.util.Deque;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class IndexParents extends TypeUtil.SchemaVisitor<Map<Integer, Integer>> {
  private final Map<Integer, Integer> idToParent = Maps.newHashMap();
  private final Deque<Integer> idStack = Lists.newLinkedList();

  @Override
  public void beforeField(Types.NestedField field) {
    idStack.push(field.fieldId());
  }

  @Override
  public void afterField(Types.NestedField field) {
    idStack.pop();
  }

  @Override
  public Map<Integer, Integer> schema(Schema schema, Map<Integer, Integer> structResult) {
    return idToParent;
  }

  @Override
  public Map<Integer, Integer> struct(
      Types.StructType struct, List<Map<Integer, Integer>> fieldResults) {
    for (Types.NestedField field : struct.fields()) {
      Integer parentId = idStack.peek();
      if (parentId != null) {
        // fields in the root struct are not added
        idToParent.put(field.fieldId(), parentId);
      }
    }
    return idToParent;
  }

  @Override
  public Map<Integer, Integer> field(Types.NestedField field, Map<Integer, Integer> fieldResult) {
    return idToParent;
  }

  @Override
  public Map<Integer, Integer> list(Types.ListType list, Map<Integer, Integer> element) {
    idToParent.put(list.elementId(), idStack.peek());
    return idToParent;
  }

  @Override
  public Map<Integer, Integer> map(
      Types.MapType map, Map<Integer, Integer> key, Map<Integer, Integer> value) {
    idToParent.put(map.keyId(), idStack.peek());
    idToParent.put(map.valueId(), idStack.peek());
    return idToParent;
  }

  @Override
  public Map<Integer, Integer> variant(Types.VariantType variant) {
    return idToParent;
  }

  @Override
  public Map<Integer, Integer> primitive(Type.PrimitiveType primitive) {
    return idToParent;
  }
}
