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

import org.apache.flink.api.common.typeutils.TypeInformationTestBase;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Types;

public class TestStatisticsOrRecordTypeInformation
    extends TypeInformationTestBase<StatisticsOrRecordTypeInformation> {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(2, "uuid", Types.UUIDType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));
  private static final RowType ROW_TYPE = FlinkSchemaUtil.convert(SCHEMA);
  private static final SortOrder SORT_ORDER1 = SortOrder.builderFor(SCHEMA).asc("ts").build();
  private static final SortOrder SORT_ORDER2 = SortOrder.builderFor(SCHEMA).asc("data").build();

  @Override
  protected StatisticsOrRecordTypeInformation[] getTestData() {
    return new StatisticsOrRecordTypeInformation[] {
      new StatisticsOrRecordTypeInformation(ROW_TYPE, SCHEMA, SORT_ORDER1),
      new StatisticsOrRecordTypeInformation(ROW_TYPE, SCHEMA, SORT_ORDER2),
    };
  }
}
