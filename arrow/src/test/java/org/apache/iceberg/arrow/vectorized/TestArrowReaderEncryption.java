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
package org.apache.iceberg.arrow.vectorized;

import java.io.IOException;
import java.util.List;
import org.apache.arrow.vector.util.Text;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.EncryptedTableReadTestBase;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;

class TestArrowReaderEncryption extends EncryptedTableReadTestBase {

  @Parameters(name = "format = {0}, formatVersion = {1}")
  protected static List<Object[]> parameters() {
    return List.of(new Object[] {FileFormat.PARQUET, 2}, new Object[] {FileFormat.PARQUET, 3});
  }

  @Override
  protected boolean supportsDeletes() {
    return false;
  }

  @Override
  @TestTemplate
  @Disabled("ArrowReader fails to read non-encrypted files from an encrypted table")
  protected void readMixedPlaintextAndEncryptedDataFiles() {}

  @Override
  protected StructLikeSet read(Table encryptedTable) throws IOException {
    Schema schema = encryptedTable.schema();
    StructLikeSet actual = StructLikeSet.create(schema.asStruct());
    try (VectorizedTableScanIterable batches =
        new VectorizedTableScanIterable(encryptedTable.newScan(), 2, false)) {
      for (ColumnarBatch batch : batches) {
        for (int row = 0; row < batch.numRows(); row += 1) {
          Record record = GenericRecord.create(schema);
          for (int pos = 0; pos < batch.numCols(); pos += 1) {
            record.set(pos, value(batch.column(pos), row));
          }

          actual.add(record);
        }
      }
    }

    return actual;
  }

  private static Object value(ColumnVector vector, int row) {
    if (vector.isNullAt(row)) {
      return null;
    }

    Object value = vector.getFieldVector().getObject(row);
    return value instanceof Text ? value.toString() : value;
  }
}
