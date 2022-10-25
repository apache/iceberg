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
package org.apache.iceberg.parquet;

import java.util.Map;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/** Interface for vectorized Iceberg readers. */
public interface VectorizedReader<T> {

  /**
   * Reads a batch of type @param &lt;T&gt; and of size numRows
   *
   * @param reuse container for the last batch to be reused for next batch
   * @param numRows number of rows to read
   * @return batch of records of type @param &lt;T&gt;
   */
  T read(T reuse, int numRows);

  void setBatchSize(int batchSize);

  /**
   * Sets the row group information to be used with this reader
   *
   * @param pages row group information for all the columns
   * @param metadata map of {@link ColumnPath} -&gt; {@link ColumnChunkMetaData} for the row group
   */
  void setRowGroupInfo(PageReadStore pages, Map<ColumnPath, ColumnChunkMetaData> metadata);

  /** Release any resources allocated. */
  void close();
}
