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
package org.apache.iceberg.connect.events;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/** Element representing a table identifier, with namespace and name. */
public class TableReference implements IndexedRecord {

  private String catalog;
  private List<String> namespace;
  private String name;
  private final Schema avroSchema;

  static final int CATALOG = 10_600;
  static final int NAMESPACE = 10_601;
  static final int NAME = 10_603;

  public static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(CATALOG, "catalog", StringType.get()),
          NestedField.required(
              NAMESPACE, "namespace", ListType.ofRequired(NAMESPACE + 1, StringType.get())),
          NestedField.required(NAME, "name", StringType.get()));
  private static final Schema AVRO_SCHEMA = AvroUtil.convert(ICEBERG_SCHEMA, TableReference.class);

  public static TableReference of(String catalog, TableIdentifier tableIdentifier) {
    return new TableReference(
        catalog, Arrays.asList(tableIdentifier.namespace().levels()), tableIdentifier.name());
  }

  // Used by Avro reflection to instantiate this class when reading events
  public TableReference(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public TableReference(String catalog, List<String> namespace, String name) {
    Preconditions.checkNotNull(catalog, "Catalog cannot be null");
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Name cannot be null");
    this.catalog = catalog;
    this.namespace = namespace;
    this.name = name;
    this.avroSchema = AVRO_SCHEMA;
  }

  public String catalog() {
    return catalog;
  }

  public TableIdentifier identifier() {
    Namespace icebergNamespace = Namespace.of(namespace.toArray(new String[0]));
    return TableIdentifier.of(icebergNamespace, name);
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case CATALOG:
        this.catalog = v == null ? null : v.toString();
        return;
      case NAMESPACE:
        this.namespace =
            v == null
                ? null
                : ((List<Utf8>) v).stream().map(Utf8::toString).collect(Collectors.toList());
        return;
      case NAME:
        this.name = v == null ? null : v.toString();
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case CATALOG:
        return catalog;
      case NAMESPACE:
        return namespace;
      case NAME:
        return name;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableReference that = (TableReference) o;
    return Objects.equals(catalog, that.catalog)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalog, namespace, name);
  }
}
