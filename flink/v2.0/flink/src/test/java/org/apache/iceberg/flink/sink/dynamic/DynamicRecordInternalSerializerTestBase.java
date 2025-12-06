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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Collections;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Test base for DynamicRecordInternalSerializer which allows to instantiate different serializer
 * version, e.g. with writing the schema itself or just the schema id.
 */
abstract class DynamicRecordInternalSerializerTestBase
    extends SerializerTestBase<DynamicRecordInternal> {

  static final String TABLE = "myTable";
  static final String BRANCH = "myBranch";

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION = new HadoopCatalogExtension("db", TABLE);

  static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "number", Types.FloatType.get()));

  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).bucket("id", 10).build();

  private boolean writeFullSchemaAndSpec;

  DynamicRecordInternalSerializerTestBase(boolean writeFullSchemaAndSpec) {
    this.writeFullSchemaAndSpec = writeFullSchemaAndSpec;
  }

  @Override
  protected TypeSerializer<DynamicRecordInternal> createSerializer() {
    return new DynamicRecordInternalSerializer(
        new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 1), writeFullSchemaAndSpec);
  }

  @BeforeEach
  void before() {
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.parse(TABLE), SCHEMA, SPEC);
  }

  @Override
  protected DynamicRecordInternal[] getTestData() {
    GenericRowData rowData = new GenericRowData(3);
    rowData.setField(0, 123L);
    rowData.setField(1, StringData.fromString("test"));
    rowData.setField(2, 1.23f);

    return new DynamicRecordInternal[] {
      new DynamicRecordInternal(
          TABLE, BRANCH, SCHEMA, rowData, SPEC, 42, false, Collections.emptySet())
    };
  }

  @Override
  protected Class<DynamicRecordInternal> getTypeClass() {
    return DynamicRecordInternal.class;
  }

  @Override
  protected int getLength() {
    return -1;
  }
}
