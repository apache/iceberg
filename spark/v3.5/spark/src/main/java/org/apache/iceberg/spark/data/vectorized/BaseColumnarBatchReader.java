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

import java.util.List;
import java.util.Properties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.BaseBatchReader;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.spark.sql.catalyst.InternalRow;

/** A base ColumnBatchReader class that contains common functionality */
@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class BaseColumnarBatchReader<T> extends BaseBatchReader<T> {
  protected boolean hasIsDeletedColumn;
  protected DeleteFilter<InternalRow> deletes = null;
  protected long rowStartPosInBatch = 0;

  protected Schema schema;

  public BaseColumnarBatchReader() {}

  public void initialize(
      List<VectorizedReader<?>> readers, Schema schema1, Properties properties) {}

  public void setDeleteFilter(DeleteFilter<InternalRow> deleteFilter) {
    this.deletes = deleteFilter;
  }
}
