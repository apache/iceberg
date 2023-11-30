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
package org.apache.iceberg.flink.sink.shuffle;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;

public abstract class TestSortKeySerializerBase extends SerializerTestBase<SortKey> {

  protected abstract Schema schema();

  protected abstract SortOrder sortOrder();

  protected abstract GenericRowData rowData();

  @Override
  protected TypeSerializer createSerializer() {
    return new SortKeySerializer(schema(), sortOrder());
  }

  @Override
  protected int getLength() {
    return -1;
  }

  @Override
  protected Class<SortKey> getTypeClass() {
    return SortKey.class;
  }

  @Override
  protected SortKey[] getTestData() {
    return new SortKey[] {sortKey()};
  }

  private SortKey sortKey() {
    RowDataWrapper rowDataWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(schema()), schema().asStruct());
    SortKey sortKey = new SortKey(schema(), sortOrder());
    sortKey.wrap(rowDataWrapper.wrap(rowData()));
    return sortKey;
  }
}
