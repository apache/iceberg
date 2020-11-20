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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FlinkSource {
  private FlinkSource() {
  }

  /**
   * Initialize a {@link Builder} to read the data from iceberg table. Equivalent to {@link TableScan}.
   * See more options in {@link ScanContext}.
   * <p>
   * The Source can be read static data in bounded mode. It can also continuously check the arrival of new data and
   * read records incrementally.
   * <ul>
   *   <li>Without startSnapshotId: Bounded</li>
   *   <li>With startSnapshotId and with endSnapshotId: Bounded</li>
   *   <li>With startSnapshotId (-1 means unbounded preceding) and Without endSnapshotId: Unbounded</li>
   * </ul>
   * <p>
   *
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRowData() {
    return new Builder();
  }

  /**
   * Source builder to build {@link DataStream}.
   */
  public static class Builder {
    private StreamExecutionEnvironment env;
    private Table table;
    private TableLoader tableLoader;
    private TableSchema projectedSchema;
    private ScanContext.Builder ctxtBuilder = ScanContext.builder();

    public Builder tableLoader(TableLoader newLoader) {
      this.tableLoader = newLoader;
      return this;
    }

    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    public Builder env(StreamExecutionEnvironment newEnv) {
      this.env = newEnv;
      return this;
    }

    public Builder filters(List<Expression> filters) {
      ctxtBuilder.filterExpression(filters);
      return this;
    }

    public Builder project(TableSchema schema) {
      this.projectedSchema = schema;
      return this;
    }

    public Builder limit(long newLimit) {
      ctxtBuilder.limit(newLimit);
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      ctxtBuilder.fromProperties(properties);
      return this;
    }

    public Builder caseSensitive(boolean caseSensitive) {
      ctxtBuilder.caseSensitive(caseSensitive);
      return this;
    }

    public Builder snapshotId(Long snapshotId) {
      ctxtBuilder.useSnapshotId(snapshotId);
      return this;
    }

    public Builder startSnapshotId(Long startSnapshotId) {
      ctxtBuilder.startSnapshotId(startSnapshotId);
      return this;
    }

    public Builder endSnapshotId(Long endSnapshotId) {
      ctxtBuilder.endSnapshotId(endSnapshotId);
      return this;
    }

    public Builder asOfTimestamp(Long asOfTimestamp) {
      ctxtBuilder.asOfTimestamp(asOfTimestamp);
      return this;
    }

    public Builder splitSize(Long splitSize) {
      ctxtBuilder.splitSize(splitSize);
      return this;
    }

    public Builder splitLookback(Integer splitLookback) {
      ctxtBuilder.splitLookback(splitLookback);
      return this;
    }

    public Builder splitOpenFileCost(Long splitOpenFileCost) {
      ctxtBuilder.splitOpenFileCost(splitOpenFileCost);
      return this;
    }

    public Builder streaming(boolean streaming) {
      ctxtBuilder.streaming(streaming);
      return this;
    }

    public Builder nameMapping(String nameMapping) {
      ctxtBuilder.nameMapping(nameMapping);
      return this;
    }

    public FlinkInputFormat buildFormat() {
      Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

      Schema icebergSchema;
      FileIO io;
      EncryptionManager encryption;
      if (table == null) {
        // load required fields by table loader.
        tableLoader.open();
        try (TableLoader loader = tableLoader) {
          table = loader.loadTable();
          icebergSchema = table.schema();
          io = table.io();
          encryption = table.encryption();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      } else {
        icebergSchema = table.schema();
        io = table.io();
        encryption = table.encryption();
      }

      if (projectedSchema == null) {
        ctxtBuilder.projectedSchema(icebergSchema);
      } else {
        ctxtBuilder.projectedSchema(FlinkSchemaUtil.convert(icebergSchema, projectedSchema));
      }

      return new FlinkInputFormat(tableLoader, icebergSchema, io, encryption, ctxtBuilder.build());
    }

    public DataStream<RowData> build() {
      Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
      FlinkInputFormat format = buildFormat();

      ScanContext ctxt = ctxtBuilder.build();
      TypeInformation<RowData> typeInfo = RowDataTypeInfo.of(FlinkSchemaUtil.convert(ctxt.projectedSchema()));

      if (isBounded(ctxt)) {
        return env.createInput(format, typeInfo);
      } else {
        OneInputStreamOperatorFactory<FlinkInputSplit, RowData> factory = StreamingReaderOperator.factory(format);
        StreamingMonitorFunction function = new StreamingMonitorFunction(tableLoader,
            ctxt.projectedSchema(), ctxt.filterExpressions(), ctxt);

        String monitorFunctionName = String.format("Iceberg table (%s) monitor", table);
        String readerOperatorName = String.format("Iceberg table (%s) reader", table);

        return env.addSource(function, monitorFunctionName)
            .transform(readerOperatorName, typeInfo, factory);
      }
    }
  }

  private static boolean isBounded(ScanContext context) {
    return context.startSnapshotId() == null || context.endSnapshotId() != null;
  }

  public static boolean isBounded(Map<String, String> properties) {
    return isBounded(ScanContext.builder().fromProperties(properties).build());
  }
}
