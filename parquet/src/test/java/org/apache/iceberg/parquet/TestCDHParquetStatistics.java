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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.parquet.column.statistics.Statistics;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Parquet 1.5.0-Stats which cannot be evaluated like later versions of Parquet stats.
 * They are intercepted by the minMaxUndefined function which always returns ROWS_MAY_MATCH
 */
public class TestCDHParquetStatistics {

  @Test
  public void testCDHParquetStatistcs() {
    Statistics cdhBinaryColumnStats = mock(Statistics.class);
    when(cdhBinaryColumnStats.isEmpty()).thenReturn(false);
    when(cdhBinaryColumnStats.hasNonNullValue()).thenReturn(true);
    when(cdhBinaryColumnStats.getMaxBytes()).thenReturn(null);
    when(cdhBinaryColumnStats.getMinBytes()).thenReturn(null);
    when(cdhBinaryColumnStats.getNumNulls()).thenReturn(0L);
    Assert.assertTrue(ParquetMetricsRowGroupFilter.minMaxUndefined(cdhBinaryColumnStats));
  }
}
