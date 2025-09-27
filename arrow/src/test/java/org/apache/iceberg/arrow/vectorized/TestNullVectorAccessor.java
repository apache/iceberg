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

import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestNullVectorAccessor {

  @Test
  public void testNullVectorYieldsNullAccessor() {
    Types.NestedField field =
        Types.NestedField.optional(1, "gm_dp_identifier", Types.LongType.get());

    ArrowVectorAccessor<?, String, ?, ?> acc =
        GenericArrowVectorAccessorFactory.getPlainVectorAccessor(/*vector=*/null, field);

    ColumnVector cv = new ColumnVector(field, acc, /*nullability*/ null);

    // Should not throw and must report nulls
    Assert.assertTrue(cv.isNullAt(0));
    Assert.assertTrue(cv.isNullAt(42));

    // Accessor should gracefully return nulls
    Assert.assertNull(acc.getLong(0));
    Assert.assertNull(acc.getUTF8String(0));
  }
}
