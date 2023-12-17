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
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/** Represents a mapping from external schema names to Iceberg type IDs. */
public class NameMapping implements Serializable {
  private static final Joiner DOT = Joiner.on('.');
  private static final NameMapping EMPTY = NameMapping.of();

  public static NameMapping empty() {
    return EMPTY;
  }

  public static NameMapping of(MappedField... fields) {
    return new NameMapping(MappedFields.of(ImmutableList.copyOf(fields)));
  }

  public static NameMapping of(List<MappedField> fields) {
    return new NameMapping(MappedFields.of(fields));
  }

  public static NameMapping of(MappedFields fields) {
    return new NameMapping(fields);
  }

  private final MappedFields mapping;
  private transient Map<Integer, MappedField> fieldsById;
  private transient Map<String, MappedField> fieldsByName;

  NameMapping(MappedFields mapping) {
    this.mapping = mapping;
    lazyFieldsById();
    lazyFieldsByName();
  }

  public MappedField find(int id) {
    return lazyFieldsById().get(id);
  }

  public MappedField find(String... names) {
    return lazyFieldsByName().get(DOT.join(names));
  }

  public MappedField find(List<String> names) {
    return lazyFieldsByName().get(DOT.join(names));
  }

  public MappedFields asMappedFields() {
    return mapping;
  }

  private Map<Integer, MappedField> lazyFieldsById() {
    if (fieldsById == null) {
      this.fieldsById = MappingUtil.indexById(mapping);
    }
    return fieldsById;
  }

  private Map<String, MappedField> lazyFieldsByName() {
    if (fieldsByName == null) {
      this.fieldsByName = MappingUtil.indexByName(mapping);
    }
    return fieldsByName;
  }

  @Override
  public String toString() {
    if (mapping.fields().isEmpty()) {
      return "[]";
    } else {
      return "[\n  " + Joiner.on("\n  ").join(mapping.fields()) + "\n]";
    }
  }
}
