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
package org.apache.iceberg.arrow;

import java.math.BigDecimal;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.arrow.vectorized.ArrowVectorAccessor;
import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** This converts dictionary encoded arrow vectors to a correctly typed arrow vector. */
public class DictEncodedArrowConverter {

  private DictEncodedArrowConverter() {}

  public static FieldVector toArrowVector(
      VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(null != vectorHolder, "VectorHolder cannot be null");
    Preconditions.checkArgument(null != accessor, "ArrowVectorAccessor cannot be null");
    // TODO: add conversions for other types (https://github.com/apache/iceberg/issues/2484)
    if (vectorHolder.isDictionaryEncoded()) {
      if (Type.TypeID.DECIMAL.equals(vectorHolder.icebergType().typeId())) {
        int precision = ((Types.DecimalType) vectorHolder.icebergType()).precision();
        int scale = ((Types.DecimalType) vectorHolder.icebergType()).scale();

        DecimalVector decimalVector =
            new DecimalVector(
                vectorHolder.vector().getName(),
                ArrowSchemaUtil.convert(vectorHolder.icebergField()).getFieldType(),
                vectorHolder.vector().getAllocator());

        for (int i = 0; i < vectorHolder.vector().getValueCount(); i++) {
          BigDecimal decimal = (BigDecimal) accessor.getDecimal(i, precision, scale);
          decimalVector.setSafe(i, decimal);
        }
        decimalVector.setValueCount(vectorHolder.vector().getValueCount());
        return decimalVector;
      }
    }

    return vectorHolder.vector();
  }
}
