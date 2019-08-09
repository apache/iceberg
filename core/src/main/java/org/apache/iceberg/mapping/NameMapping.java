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
import java.util.List;
import java.util.Map;

/**
 * Represents a mapping from external schema names to Iceberg type IDs.
 */
public class NameMapping {
  private static final Joiner DOT = Joiner.on('.');

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
  private final Map<Integer, MappedField> fieldsById;
  private final Map<String, MappedField> fieldsByName;

  NameMapping(MappedFields mapping) {
    this.mapping = mapping;
    this.fieldsById = MappingUtil.indexById(mapping);
    this.fieldsByName = MappingUtil.indexByName(mapping);
  }

  public MappedField find(int id) {
    return fieldsById.get(id);
  }

  public MappedField find(String... names) {
    return fieldsByName.get(DOT.join(names));
  }

  public MappedField find(List<String> names) {
    return fieldsByName.get(DOT.join(names));
  }

  public MappedFields asMappedFields() {
    return mapping;
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
