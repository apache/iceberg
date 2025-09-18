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
package org.apache.iceberg.flink.sink.dynamic;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.iceberg.flink.CatalogLoader;

@Internal
class DynamicRecordInternalType extends TypeInformation<DynamicRecordInternal> {

  private final CatalogLoader catalogLoader;
  private final boolean writeSchemaAndSpec;
  private final int cacheSize;

  DynamicRecordInternalType(
      CatalogLoader catalogLoader, boolean writeSchemaAndSpec, int cacheSize) {
    this.catalogLoader = catalogLoader;
    this.writeSchemaAndSpec = writeSchemaAndSpec;
    this.cacheSize = cacheSize;
  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 0;
  }

  @Override
  public int getTotalFields() {
    return 1;
  }

  @Override
  public Class<DynamicRecordInternal> getTypeClass() {
    return DynamicRecordInternal.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<DynamicRecordInternal> createSerializer(SerializerConfig serializerConfig) {
    return new DynamicRecordInternalSerializer(
        new TableSerializerCache(catalogLoader, cacheSize), writeSchemaAndSpec);
  }

  @Override
  public TypeSerializer<DynamicRecordInternal> createSerializer(ExecutionConfig executionConfig) {
    return new DynamicRecordInternalSerializer(
        new TableSerializerCache(catalogLoader, cacheSize), writeSchemaAndSpec);
  }

  @Override
  public String toString() {
    return getClass().getName();
  }

  @Override
  public boolean equals(Object o) {
    return canEqual(o);
  }

  @Override
  public int hashCode() {
    return getClass().getName().hashCode();
  }

  @Override
  public boolean canEqual(Object o) {
    return o instanceof DynamicRecordInternalType;
  }
}
