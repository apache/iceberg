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

import java.util.Objects;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;

public class StatisticsOrRecordTypeInformation extends TypeInformation<StatisticsOrRecord> {

  private final TypeInformation<RowData> rowTypeInformation;
  private final SortOrder sortOrder;
  private final GlobalStatisticsSerializer globalStatisticsSerializer;

  public StatisticsOrRecordTypeInformation(
      RowType flinkRowType, Schema schema, SortOrder sortOrder) {
    this.sortOrder = sortOrder;
    this.rowTypeInformation = FlinkCompatibilityUtil.toTypeInfo(flinkRowType);
    this.globalStatisticsSerializer =
        new GlobalStatisticsSerializer(new SortKeySerializer(schema, sortOrder));
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
    return 1;
  }

  @Override
  public int getTotalFields() {
    return 1;
  }

  @Override
  public Class<StatisticsOrRecord> getTypeClass() {
    return StatisticsOrRecord.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<StatisticsOrRecord> createSerializer(SerializerConfig config) {
    TypeSerializer<RowData> recordSerializer = rowTypeInformation.createSerializer(config);
    return new StatisticsOrRecordSerializer(globalStatisticsSerializer, recordSerializer);
  }

  @Override
  public String toString() {
    return "StatisticsOrRecord";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      StatisticsOrRecordTypeInformation that = (StatisticsOrRecordTypeInformation) o;
      return that.sortOrder.equals(sortOrder)
          && that.rowTypeInformation.equals(rowTypeInformation)
          && that.globalStatisticsSerializer.equals(globalStatisticsSerializer);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowTypeInformation, sortOrder, globalStatisticsSerializer);
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof StatisticsOrRecordTypeInformation;
  }
}
