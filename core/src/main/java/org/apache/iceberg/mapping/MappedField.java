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
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/** An immutable mapping between a field ID and a set of names. */
public class MappedField implements Serializable {

  public static MappedField of(Integer id, String name) {
    return new MappedField(id, ImmutableSet.of(name), null);
  }

  public static MappedField of(Integer id, Iterable<String> names) {
    return new MappedField(id, names, null);
  }

  public static MappedField of(Integer id, String name, MappedFields nestedMapping) {
    return new MappedField(id, ImmutableSet.of(name), nestedMapping);
  }

  public static MappedField of(Integer id, Iterable<String> names, MappedFields nestedMapping) {
    return new MappedField(id, names, nestedMapping);
  }

  private final Set<String> names;
  private Integer id;
  private MappedFields nestedMapping;

  private MappedField(Integer id, Iterable<String> names, MappedFields nested) {
    this.id = id;
    this.names = ImmutableSet.copyOf(names);
    this.nestedMapping = nested;
  }

  public Integer id() {
    return id;
  }

  public Set<String> names() {
    return names;
  }

  public MappedFields nestedMapping() {
    return nestedMapping;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof MappedField)) {
      return false;
    }

    MappedField that = (MappedField) other;
    return names.equals(that.names)
        && Objects.equals(id, that.id)
        && Objects.equals(nestedMapping, that.nestedMapping);
  }

  @Override
  public int hashCode() {
    return Objects.hash(names, id, nestedMapping);
  }

  @Override
  public String toString() {
    return "(["
        + Joiner.on(", ").join(names)
        + "] -> "
        + (id != null ? id : "?")
        + (nestedMapping != null ? ", " + nestedMapping + ")" : ")");
  }
}
