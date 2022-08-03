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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class MappedFields implements Serializable {

  public static MappedFields of(MappedField... fields) {
    return new MappedFields(ImmutableList.copyOf(fields));
  }

  public static MappedFields of(List<MappedField> fields) {
    return new MappedFields(fields);
  }

  private final List<MappedField> fields;
  private transient Map<String, Integer> nameToId;
  private transient Map<Integer, MappedField> idToField;

  private MappedFields(List<MappedField> fields) {
    this.fields = ImmutableList.copyOf(fields);
    lazyNameToId();
    lazyIdToField();
  }

  public MappedField field(int id) {
    return lazyIdToField().get(id);
  }

  public Integer id(String name) {
    return lazyNameToId().get(name);
  }

  public int size() {
    return fields.size();
  }

  private static Map<String, Integer> indexIds(List<MappedField> fields) {
    ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
    fields.forEach(
        field ->
            field
                .names()
                .forEach(
                    name -> {
                      Integer id = field.id();
                      if (id != null) {
                        builder.put(name, id);
                      }
                    }));
    return builder.build();
  }

  private static Map<Integer, MappedField> indexFields(List<MappedField> fields) {
    ImmutableMap.Builder<Integer, MappedField> builder = ImmutableMap.builder();
    fields.forEach(
        field -> {
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

  private Map<String, Integer> lazyNameToId() {
    if (nameToId == null) {
      this.nameToId = indexIds(fields);
    }
    return nameToId;
  }

  private Map<Integer, MappedField> lazyIdToField() {
    if (idToField == null) {
      this.idToField = indexFields(fields);
    }
    return idToField;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof MappedFields)) {
      return false;
    }

    return fields.equals(((MappedFields) other).fields);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fields);
  }

  @Override
  public String toString() {
    return "[ " + Joiner.on(", ").join(fields) + " ]";
  }
}
