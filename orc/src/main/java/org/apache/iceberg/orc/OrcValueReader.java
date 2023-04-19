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
package org.apache.iceberg.orc;

import org.apache.orc.storage.ql.exec.vector.ColumnVector;

public interface OrcValueReader<T> {
  default T read(ColumnVector vector, int row) {
    int rowIndex = vector.isRepeating ? 0 : row;
    if (!vector.noNulls && vector.isNull[rowIndex]) {
      return null;
    } else {
      return nonNullRead(vector, rowIndex);
    }
  }

  T nonNullRead(ColumnVector vector, int row);

  default void setBatchContext(long batchOffsetInFile) {}
}
