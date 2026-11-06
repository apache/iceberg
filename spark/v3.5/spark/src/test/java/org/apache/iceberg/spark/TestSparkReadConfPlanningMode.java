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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TestSparkReadConfPlanningMode {

  @ParameterizedTest
  @CsvSource({"0,AUTO", "128m,LOCAL", "256m,AUTO"})
  void dataPlanningModeRespectsDriverMaxResultSize(String maxResultSize, PlanningMode expected) {
    assertThat(readConf(maxResultSize).dataPlanningMode()).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({"0,AUTO", "128m,LOCAL", "256m,AUTO"})
  void deletePlanningModeRespectsDriverMaxResultSize(String maxResultSize, PlanningMode expected) {
    assertThat(readConf(maxResultSize).deletePlanningMode()).isEqualTo(expected);
  }

  private SparkReadConf readConf(String maxResultSize) {
    SparkSession spark = mock(SparkSession.class);
    SparkContext context = mock(SparkContext.class);
    Table table = mock(Table.class);
    SparkConf sparkConf = new SparkConf(false).set("spark.driver.maxResultSize", maxResultSize);

    when(spark.sparkContext()).thenReturn(context);
    when(context.conf()).thenReturn(sparkConf);
    when(spark.conf()).thenReturn(mock(RuntimeConfig.class));
    when(table.properties()).thenReturn(ImmutableMap.of());

    return new SparkReadConf(spark, table, ImmutableMap.of());
  }
}
