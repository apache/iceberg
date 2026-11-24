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
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.EncryptedTableReadTestBase;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.spark.sql.types.StructType;

class TestRowDataReaderEncryption extends EncryptedTableReadTestBase {

  @Override
  protected StructLikeSet read(Table encryptedTable) throws IOException {
    StructType sparkType = SparkSchemaUtil.convert(encryptedTable.schema());
    StructLikeSet actual = StructLikeSet.create(encryptedTable.schema().asStruct());
    try (CloseableIterable<CombinedScanTask> tasks = encryptedTable.newScan().planTasks()) {
      for (CombinedScanTask task : tasks) {
        try (RowDataReader reader =
            new RowDataReader(
                encryptedTable,
                encryptedTable.io(),
                task,
                encryptedTable.schema(),
                false /* case sensitive */,
                false /* cache delete files on executors */)) {
          while (reader.next()) {
            actual.add(
                new InternalRowWrapper(sparkType, encryptedTable.schema().asStruct())
                    .wrap(reader.get().copy()));
          }
        }
      }
    }

    return actual;
  }
}
