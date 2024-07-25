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
package org.apache.iceberg.spark;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSparkReadConf {

  private SparkSession spark;
  private Table table;

  @Before
  public void before() {
    RuntimeConfig conf = Mockito.mock(RuntimeConfig.class);
    Mockito.when(conf.get(Mockito.anyString(), Mockito.anyString()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    spark = Mockito.mock(SparkSession.class);
    Mockito.when(spark.conf()).thenReturn(conf);
    table = Mockito.mock(Table.class);
    Mockito.when(table.name()).thenReturn("cat.db.tbl");
  }

  @Test
  public void splitSizeReadOption() {
    long splitSizeOption = 12345;
    Map<String, String> options =
        ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, Long.toString(splitSizeOption));

    SparkReadConf readConf = new SparkReadConf(spark, table, options);
    Assert.assertEquals(splitSizeOption, readConf.splitSize());
    Assert.assertEquals(splitSizeOption, readConf.splitSizeOption().longValue());
  }

  @Test
  public void splitSizeTableProp() {
    long splitSizeTableProp = 7654;
    Mockito.when(table.properties())
        .thenReturn(
            ImmutableMap.of(TableProperties.SPLIT_SIZE, String.valueOf(splitSizeTableProp)));

    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    Assert.assertEquals(splitSizeTableProp, readConf.splitSize());
    Assert.assertEquals(null, readConf.splitSizeOption());
  }

  @Test
  public void splitSizeMaxPartitionBytes() {
    long maxPartitionBytes = 67890000;
    Mockito.when(spark.conf().get(SQLConf.FILES_MAX_PARTITION_BYTES().key(), null))
        .thenReturn(String.valueOf(maxPartitionBytes));

    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    Assert.assertEquals(maxPartitionBytes, readConf.splitSize());
    Assert.assertEquals(null, readConf.splitSizeOption());
  }

  @Test
  public void splitSizeDefault() {
    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    Assert.assertEquals(TableProperties.SPLIT_SIZE_DEFAULT, readConf.splitSize());
    Assert.assertEquals(null, readConf.splitSizeOption());
  }

  @Test
  public void splitSizeReadOptionAndTableProp() {
    long splitSizeOption = 12345;
    Map<String, String> options =
        ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, Long.toString(splitSizeOption));

    long splitSizeTableProp = 7654;
    Mockito.when(table.properties())
        .thenReturn(
            ImmutableMap.of(TableProperties.SPLIT_SIZE, String.valueOf(splitSizeTableProp)));

    SparkReadConf readConf = new SparkReadConf(spark, table, options);
    Assert.assertEquals(splitSizeOption, readConf.splitSize());
    Assert.assertEquals(splitSizeOption, readConf.splitSizeOption().longValue());
  }

  @Test
  public void splitSizeTablePropAndMaxPartitionBytes() {
    long splitSizeTableProp = 7654;
    Mockito.when(table.properties())
        .thenReturn(
            ImmutableMap.of(TableProperties.SPLIT_SIZE, String.valueOf(splitSizeTableProp)));

    long maxPartitionBytes = 67890000;
    Mockito.when(spark.conf().get(SQLConf.FILES_MAX_PARTITION_BYTES().key(), null))
        .thenReturn(String.valueOf(maxPartitionBytes));

    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    Assert.assertEquals(splitSizeTableProp, readConf.splitSize());
    Assert.assertEquals(null, readConf.splitSizeOption());
  }
}
