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

import java.util.List;
import java.util.Map;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/** A base BatchReader class that contains common functionality */
@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class BaseBatchReader<T> implements VectorizedReader<T> {
  protected VectorizedReader[] readers;

  public BaseBatchReader() {}

  public void initialize(List<VectorizedReader<?>> readerList) {}

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData, long rowPosition) {
    for (VectorizedReader reader : readers) {
      if (reader != null) {
        reader.setRowGroupInfo(pageStore, metaData, rowPosition);
      }
    }
  }

  @Override
  public void close() {
    for (VectorizedReader<?> reader : readers) {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Override
  public void setBatchSize(int batchSize) {
    for (VectorizedReader reader : readers) {
      if (reader != null) {
        reader.setBatchSize(batchSize);
      }
    }
  }
}
