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

import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.iceberg.arrow.vectorized.VectorHolder.ConstantVectorHolder;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.vectorized.ColumnVector;

class ColumnVectorBuilder {
  private boolean[] isDeleted;
  private int[] rowIdMapping;

  public ColumnVectorBuilder withDeletedRows(int[] rowIdMappingArray, boolean[] isDeletedArray) {
    this.rowIdMapping = rowIdMappingArray;
    this.isDeleted = isDeletedArray;
    return this;
  }

  public ColumnVector build(VectorHolder holder, int numRows) {
    if (holder.isDummy()) {
      if (holder instanceof VectorHolder.DeletedVectorHolder) {
        return new DeletedColumnVector(Types.BooleanType.get(), isDeleted);
      } else if (holder instanceof ConstantVectorHolder) {
        ConstantVectorHolder<?> constantHolder = (ConstantVectorHolder<?>) holder;
        Type icebergType = constantHolder.icebergType();
        Object value = constantHolder.getConstant();
        return new ConstantColumnVector(icebergType, numRows, value);
      } else {
        throw new IllegalStateException("Unknown dummy vector holder: " + holder);
      }
    } else if (rowIdMapping != null) {
      return new ColumnVectorWithFilter(holder, rowIdMapping);
    } else {
      return new IcebergArrowColumnVector(holder);
    }
  }
}
