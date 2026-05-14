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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkReadConf extends TestBaseWithCatalog {

  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE TABLE %s (id BIGINT, data STRING) USING iceberg", tableName);
  }

  @AfterEach
  public void after() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testSplitParallelismDefault() {
    Table table = validationCatalog.loadTable(tableIdent);
    SparkReadConf conf = new SparkReadConf(spark, table, CaseInsensitiveStringMap.empty());
    assertThat(conf.splitParallelism()).isEqualTo(conf.parallelism());
  }

  @TestTemplate
  public void testSplitParallelismSessionConf() {
    Table table = validationCatalog.loadTable(tableIdent);
    withSQLConf(
        ImmutableMap.of(
            SQLConf.SHUFFLE_PARTITIONS().key(),
            "999",
            SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_PARALLELISM,
            "42"),
        () -> {
          SparkReadConf conf = new SparkReadConf(spark, table, CaseInsensitiveStringMap.empty());
          assertThat(conf.splitParallelism()).isEqualTo(42);
        });
  }

  @TestTemplate
  public void testSplitParallelismRejectsZero() {
    Table table = validationCatalog.loadTable(tableIdent);
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_PARALLELISM, "0"),
        () -> {
          SparkReadConf conf = new SparkReadConf(spark, table, CaseInsensitiveStringMap.empty());
          assertThatIllegalArgumentException()
              .isThrownBy(conf::splitParallelism)
              .withMessageContaining("Split parallelism must be > 0");
        });
  }

  @TestTemplate
  public void testSplitParallelismRejectsNegative() {
    Table table = validationCatalog.loadTable(tableIdent);
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_PARALLELISM, "-5"),
        () -> {
          SparkReadConf conf = new SparkReadConf(spark, table, CaseInsensitiveStringMap.empty());
          assertThatIllegalArgumentException()
              .isThrownBy(conf::splitParallelism)
              .withMessageContaining("Split parallelism must be > 0");
        });
  }
}
