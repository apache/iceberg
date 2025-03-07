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
package org.apache.iceberg.spark.data.vectorized;

import org.apache.comet.parquet.MetadataColumnReader;
import org.apache.comet.parquet.Native;
import org.apache.comet.parquet.TypeUtil;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

class CometDeleteColumnReader<T> extends CometColumnReader {
  CometDeleteColumnReader(Types.NestedField field) {
    super(field);
    setDelegate(new DeleteColumnReader());
  }

  CometDeleteColumnReader(boolean[] isDeleted) {
    super(MetadataColumns.IS_DELETED);
    setDelegate(new DeleteColumnReader(isDeleted));
  }

  @Override
  public void setBatchSize(int batchSize) {
    super.setBatchSize(batchSize);
    delegate().setBatchSize(batchSize);
    setInitialized(true);
  }

  private static class DeleteColumnReader extends MetadataColumnReader {
    private boolean[] isDeleted;

    DeleteColumnReader() {
      super(
          DataTypes.BooleanType,
          TypeUtil.convertToParquet(
              new StructField("_deleted", DataTypes.BooleanType, false, Metadata.empty())),
          false /* useDecimal128 = false */,
          false /* isConstant = false */);
      this.isDeleted = new boolean[0];
    }

    DeleteColumnReader(boolean[] isDeleted) {
      this();
      this.isDeleted = isDeleted;
    }

    @Override
    public void readBatch(int total) {
      Native.resetBatch(nativeHandle);
      // set isDeleted on the native side to be consumed by native execution
      Native.setIsDeleted(nativeHandle, isDeleted);

      super.readBatch(total);
    }
  }
}
