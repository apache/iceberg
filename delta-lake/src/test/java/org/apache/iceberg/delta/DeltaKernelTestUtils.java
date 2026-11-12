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
package org.apache.iceberg.delta;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;

final class DeltaKernelTestUtils {
  private static final int DECIMAL_PRECISION = 10;
  private static final int DECIMAL_SCALE = 2;

  private DeltaKernelTestUtils() {}

  static void createBasicDeltaTable(Configuration hadoopConf, String tableLocation)
      throws IOException {
    Engine engine = DefaultEngine.create(hadoopConf);
    DecimalType decimalType = new DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE);
    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER)
            .add("status", StringType.STRING)
            .add("amount", decimalType);

    Transaction txn =
        Table.forPath(engine, tableLocation)
            .createTransactionBuilder(
                engine, "Delta-Iceberg-Converter-Tests", Operation.CREATE_TABLE)
            .withSchema(engine, schema)
            .build(engine);
    Row txnState = txn.getTransactionState(engine);

    FilteredColumnarBatch batch = createSampleBatch(schema, decimalType);
    Iterator<FilteredColumnarBatch> batchIter = Collections.singletonList(batch).iterator();
    CloseableIterator<FilteredColumnarBatch> logicalData =
        new CloseableIterator<>() {
          @Override
          public boolean hasNext() {
            return batchIter.hasNext();
          }

          @Override
          public FilteredColumnarBatch next() {
            return batchIter.next();
          }

          @Override
          public void close() {}
        };

    DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, Map.of());
    try (CloseableIterator<FilteredColumnarBatch> physicalData =
            Transaction.transformLogicalData(engine, txnState, logicalData, Map.of());
        CloseableIterator<DataFileStatus> dataFiles =
            engine
                .getParquetHandler()
                .writeParquetFiles(
                    writeContext.getTargetDirectory(),
                    physicalData,
                    writeContext.getStatisticsColumns());
        CloseableIterator<Row> dataActions =
            Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);
        CloseableIterable<Row> actionsIterable = CloseableIterable.inMemoryIterable(dataActions)) {
      txn.commit(engine, actionsIterable);
    }
  }

  private static FilteredColumnarBatch createSampleBatch(
      StructType schema, DecimalType decimalType) {
    List<Integer> ids = List.of(1, 2);
    List<String> statuses = List.of("active", "inactive");
    List<BigDecimal> amounts = List.of(new BigDecimal("12.34"), new BigDecimal("56.78"));

    ColumnVector[] vectors =
        new ColumnVector[] {
          new ColumnVector() {
            @Override
            public DataType getDataType() {
              return IntegerType.INTEGER;
            }

            @Override
            public int getSize() {
              return ids.size();
            }

            @Override
            public void close() {}

            @Override
            public boolean isNullAt(int rowId) {
              return false;
            }

            @Override
            public int getInt(int rowId) {
              return ids.get(rowId);
            }
          },
          new ColumnVector() {
            @Override
            public DataType getDataType() {
              return StringType.STRING;
            }

            @Override
            public int getSize() {
              return statuses.size();
            }

            @Override
            public void close() {}

            @Override
            public boolean isNullAt(int rowId) {
              return false;
            }

            @Override
            public String getString(int rowId) {
              return statuses.get(rowId);
            }
          },
          new ColumnVector() {
            @Override
            public DataType getDataType() {
              return decimalType;
            }

            @Override
            public int getSize() {
              return amounts.size();
            }

            @Override
            public void close() {}

            @Override
            public boolean isNullAt(int rowId) {
              return false;
            }

            @Override
            public BigDecimal getDecimal(int rowId) {
              return amounts.get(rowId);
            }
          }
        };

    ColumnarBatch batch =
        new ColumnarBatch() {
          @Override
          public StructType getSchema() {
            return schema;
          }

          @Override
          public ColumnVector getColumnVector(int ordinal) {
            return vectors[ordinal];
          }

          @Override
          public int getSize() {
            return ids.size();
          }
        };

    return new FilteredColumnarBatch(batch, Optional.empty());
  }
}
