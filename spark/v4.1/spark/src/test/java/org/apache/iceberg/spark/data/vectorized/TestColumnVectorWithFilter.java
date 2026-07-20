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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.junit.jupiter.api.Test;

public class TestColumnVectorWithFilter {

  @Test
  public void testGeospatialAccessorsUseMappedRowId() {
    ColumnVector delegate = mock(ColumnVector.class);
    when(delegate.dataType()).thenReturn(DataTypes.BinaryType);

    ColumnVector filtered = new ColumnVectorWithFilter(delegate, new int[] {2, 0});
    filtered.getGeometry(0);
    filtered.getGeography(1);

    verify(delegate).getGeometry(2);
    verify(delegate).getGeography(0);
  }
}
