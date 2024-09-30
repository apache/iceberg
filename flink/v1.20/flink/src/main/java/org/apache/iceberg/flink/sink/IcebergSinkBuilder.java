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
package org.apache.iceberg.flink.sink;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
/*
 This class is for internal purpose of transition between the previous implementation of Flink's sink (FlinkSink)
 and the new one implementation based on Flink v2 sink's API (IcebergSink)
*/
public abstract class IcebergSinkBuilder<T extends IcebergSinkBuilder<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkBuilder.class);

  public abstract T tableSchema(TableSchema newTableSchema);

  public abstract T tableLoader(TableLoader newTableLoader);

  public abstract T equalityFieldColumns(List<String> columns);

  public abstract T overwrite(boolean newOverwrite);

  public abstract T setAll(Map<String, String> properties);

  public abstract T flinkConf(ReadableConfig config);

  public abstract T table(Table newTable);

  public abstract T writeParallelism(int newWriteParallelism);

  public abstract T distributionMode(DistributionMode mode);

  public abstract T toBranch(String branch);

  public abstract T upsert(boolean enabled);

  public abstract DataStreamSink<?> append();

  protected abstract SerializableTable getTable();

  protected abstract List<String> getEqualityFieldColumns();

  @VisibleForTesting
  final List<Integer> checkAndGetEqualityFieldIds() {
    List<Integer> equalityFieldIds = Lists.newArrayList(getTable().schema().identifierFieldIds());
    if (getEqualityFieldColumns() != null && !getEqualityFieldColumns().isEmpty()) {
      Set<Integer> equalityFieldSet =
          Sets.newHashSetWithExpectedSize(getEqualityFieldColumns().size());
      for (String column : getEqualityFieldColumns()) {
        org.apache.iceberg.types.Types.NestedField field = getTable().schema().findField(column);
        org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkNotNull(
            field,
            "Missing required equality field column '%s' in table schema %s",
            column,
            getTable().schema());
        equalityFieldSet.add(field.fieldId());
      }

      if (!equalityFieldSet.equals(getTable().schema().identifierFieldIds())) {
        LOG.warn(
            "The configured equality field column IDs {} are not matched with the schema identifier field IDs"
                + " {}, use job specified equality field columns as the equality fields by default.",
            equalityFieldSet,
            getTable().schema().identifierFieldIds());
      }
      equalityFieldIds = Lists.newArrayList(equalityFieldSet);
    }
    return equalityFieldIds;
  }

  public static IcebergSinkBuilder<?> forRow(
      DataStream<Row> input, TableSchema tableSchema, boolean useV2Sink) {
    if (useV2Sink) {
      return IcebergSink.forRow(input, tableSchema);
    } else {
      return FlinkSink.forRow(input, tableSchema);
    }
  }

  public static IcebergSinkBuilder<?> forRowData(DataStream<RowData> input, boolean useV2Sink) {
    if (useV2Sink) {
      return IcebergSink.forRowData(input);
    } else {
      return FlinkSink.forRowData(input);
    }
  }
}
