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
package org.apache.iceberg.parquet.metadata;

import java.util.List;
import java.util.Objects;

public final class BlockMetadata {
  private final long rowCount;
  private final List<ColumnChunkMetadata> columns;

  public BlockMetadata(long rowCount, List<ColumnChunkMetadata> columns) {
    this.rowCount = rowCount;
    this.columns = columns;
  }

  public long rowCount() {
    return rowCount;
  }

  public List<ColumnChunkMetadata> columns() {
    return columns;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    BlockMetadata that = (BlockMetadata) obj;
    return this.rowCount == that.rowCount && Objects.equals(this.columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowCount, columns);
  }

  @Override
  public String toString() {
    return "BlockMetadata[" + "rowCount=" + rowCount + ", " + "columns=" + columns + ']';
  }
}
