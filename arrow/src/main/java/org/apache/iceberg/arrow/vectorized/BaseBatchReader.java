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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/** A base BatchReader class that contains common functionality */
@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class BaseBatchReader<T> implements VectorizedReader<T> {
  protected final VectorizedArrowReader[] readers;
  protected final VectorHolder[] vectorHolders;

  protected BaseBatchReader(List<VectorizedReader<?>> readers) {
    this.readers =
        readers.stream()
            .map(VectorizedArrowReader.class::cast)
            .toArray(VectorizedArrowReader[]::new);
    this.vectorHolders = new VectorHolder[readers.size()];
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData) {
    for (VectorizedArrowReader reader : readers) {
      if (reader != null) {
        reader.setRowGroupInfo(pageStore, metaData);
      }
    }
  }

  protected void closeVectors() {
    for (int i = 0; i < vectorHolders.length; i++) {
      if (vectorHolders[i] != null) {
        // Release any resources used by the vector
        if (vectorHolders[i].vector() != null) {
          vectorHolders[i].vector().close();
        }
        vectorHolders[i] = null;
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
    closeVectors();
  }

  @Override
  public void setBatchSize(int batchSize) {
    for (VectorizedArrowReader reader : readers) {
      if (reader != null) {
        reader.setBatchSize(batchSize);
      }
    }
  }
}
