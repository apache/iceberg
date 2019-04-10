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
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;

public class IndexByName extends TypeUtil.SchemaVisitor<Map<String, Integer>> {
  private static final Joiner DOT = Joiner.on(".");

  private final Map<String, Integer> nameToId = Maps.newHashMap();

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
    return null;
  }

  @Override
  public Map<String, Integer> list(Types.ListType list, Map<String, Integer> elementResult) {
    for (Types.NestedField field : list.fields()) {
      addField(field.name(), field.fieldId());
    }
    return null;
  }

  @Override
  public Map<String, Integer> map(Types.MapType map, Map<String, Integer> keyResult, Map<String, Integer> valueResult) {
    for (Types.NestedField field : map.fields()) {
      addField(field.name(), field.fieldId());
    }
    return null;
  }

  private void addField(String name, int fieldId) {
    String fullName = name;
    if (!fieldNames().isEmpty()) {
      fullName = DOT.join(DOT.join(fieldNames().descendingIterator()), name);
    }
    nameToId.put(fullName, fieldId);
  }
}
