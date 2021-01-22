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

import org.apache.parquet.column.statistics.Statistics;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for Parquet 1.5.0-Stats which cannot be evaluated like later versions of Parquet stats. They are intercepted
 * by the hasNonNullButNoMinMax function which always returns ROWS_MAY_MATCH
 */
public class TestCDHParquetStatistics {

  @Test
  public void testCDHParquetStatistcs() {
    Statistics cdhBinaryColumnStats = mock(Statistics.class);
    when(cdhBinaryColumnStats.getMaxBytes()).thenReturn(null);
    when(cdhBinaryColumnStats.getMinBytes()).thenReturn(null);
    when(cdhBinaryColumnStats.getNumNulls()).thenReturn(0L);
    Assert.assertTrue(ParquetMetricsRowGroupFilter.hasNonNullButNoMinMax(cdhBinaryColumnStats, 50L));
  }

  @Test
  public void testCDHParquetStatisticsNullNotSet() {
    Statistics cdhBinaryColumnStats = mock(Statistics.class);
    when(cdhBinaryColumnStats.getMaxBytes()).thenReturn(null);
    when(cdhBinaryColumnStats.getMinBytes()).thenReturn(null);
    when(cdhBinaryColumnStats.getNumNulls()).thenReturn(-1L);
    Assert.assertTrue(ParquetMetricsRowGroupFilter.hasNonNullButNoMinMax(cdhBinaryColumnStats, 50L));
  }

  @Test
  public void testCDHParquetStatistcsAllNull() {
    Statistics cdhBinaryColumnStats = mock(Statistics.class);
    when(cdhBinaryColumnStats.getMaxBytes()).thenReturn(null);
    when(cdhBinaryColumnStats.getMinBytes()).thenReturn(null);
    when(cdhBinaryColumnStats.getNumNulls()).thenReturn(50L);
    Assert.assertFalse(ParquetMetricsRowGroupFilter.hasNonNullButNoMinMax(cdhBinaryColumnStats, 50L));
  }

  @Test
  public void testNonCDHParquetStatistics() {
    Statistics normalBinaryColumnStats = mock(Statistics.class);
    when(normalBinaryColumnStats.getMaxBytes()).thenReturn(new byte[2]);
    when(normalBinaryColumnStats.getMinBytes()).thenReturn(new byte[2]);
    when(normalBinaryColumnStats.getNumNulls()).thenReturn(0L);
    Assert.assertFalse(ParquetMetricsRowGroupFilter.hasNonNullButNoMinMax(normalBinaryColumnStats, 50L));
  }

  @Test
  public void testNonCDHParquetStatisticsNullNotSet() {
    Statistics normalBinaryColumnStats = mock(Statistics.class);
    when(normalBinaryColumnStats.getMaxBytes()).thenReturn(new byte[2]);
    when(normalBinaryColumnStats.getMinBytes()).thenReturn(new byte[2]);
    when(normalBinaryColumnStats.getNumNulls()).thenReturn(-1L);
    Assert.assertFalse(ParquetMetricsRowGroupFilter.hasNonNullButNoMinMax(normalBinaryColumnStats, 50L));
  }

  @Test
  public void testNonCDHParquetStatisticsAllNull() {
    Statistics normalBinaryColumnStats = mock(Statistics.class);
    when(normalBinaryColumnStats.getMaxBytes()).thenReturn(new byte[2]);
    when(normalBinaryColumnStats.getMinBytes()).thenReturn(new byte[2]);
    when(normalBinaryColumnStats.getNumNulls()).thenReturn(50L);
    Assert.assertFalse(ParquetMetricsRowGroupFilter.hasNonNullButNoMinMax(normalBinaryColumnStats, 50L));
  }
}
