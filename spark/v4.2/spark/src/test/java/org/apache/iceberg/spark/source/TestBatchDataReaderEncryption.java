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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.EncryptedTableReadTestBase;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.ImmutableParquetBatchReadConf;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

class TestBatchDataReaderEncryption extends EncryptedTableReadTestBase {

  @Parameters(name = "format = {0}, formatVersion = {1}")
  protected static List<Object[]> parameters() {
    return List.of(new Object[] {FileFormat.PARQUET, 2}, new Object[] {FileFormat.PARQUET, 3});
  }

  @Override
  protected StructLikeSet read(Table encryptedTable) throws IOException {
    StructType sparkType = SparkSchemaUtil.convert(encryptedTable.schema());
    ParquetBatchReadConf conf = ImmutableParquetBatchReadConf.builder().batchSize(2).build();
    StructLikeSet actual = StructLikeSet.create(encryptedTable.schema().asStruct());
    try (CloseableIterable<CombinedScanTask> tasks = encryptedTable.newScan().planTasks()) {
      for (CombinedScanTask task : tasks) {
        try (BatchDataReader reader =
            new BatchDataReader(
                encryptedTable,
                encryptedTable.io(),
                task,
                encryptedTable.schema(),
                false /* case sensitive */,
                conf,
                null /* ORC conf */,
                false /* cache delete files on executors */)) {
          while (reader.next()) {
            Iterator<InternalRow> rows = reader.get().rowIterator();
            while (rows.hasNext()) {
              actual.add(
                  new InternalRowWrapper(sparkType, encryptedTable.schema().asStruct())
                      .wrap(rows.next().copy()));
            }
          }
        }
      }
    }

    return actual;
  }
}
