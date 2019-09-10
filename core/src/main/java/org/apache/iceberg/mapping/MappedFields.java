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

package org.apache.iceberg.mapping;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MappedFields {

  public static MappedFields of(MappedField... fields) {
    return new MappedFields(ImmutableList.copyOf(fields));
  }

  public static MappedFields of(List<MappedField> fields) {
    return new MappedFields(fields);
  }

  private final List<MappedField> fields;
  private final Map<String, Integer> nameToId;
  private final Map<Integer, MappedField> idToField;

  private MappedFields(List<MappedField> fields) {
    this.fields = ImmutableList.copyOf(fields);
    this.nameToId = indexIds(fields);
    this.idToField = indexFields(fields);
  }

  public MappedField field(int id) {
    return idToField.get(id);
  }

  public Integer id(String name) {
    return nameToId.get(name);
  }

  public int size() {
    return fields.size();
  }

  private static Map<String, Integer> indexIds(List<MappedField> fields) {
    ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
    fields.forEach(field ->
        field.names().forEach(name -> {
          Integer id = field.id();
          if (id != null) {
            builder.put(name, id);
          }
        }));
    return builder.build();
  }

  private static Map<Integer, MappedField> indexFields(List<MappedField> fields) {
    ImmutableMap.Builder<Integer, MappedField> builder = ImmutableMap.builder();
    fields.forEach(field -> {
      Integer id = field.id();
      if (id != null) {
        builder.put(id, field);
      }
    });
    return builder.build();
  }

  public List<MappedField> fields() {
    return fields;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    return fields.equals(((MappedFields) other).fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  @Override
  public String toString() {
    return "[ " + Joiner.on(", ").join(fields) + " ]";
  }
}
