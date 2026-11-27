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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.optimizer.NestedDynamicPruning;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestNestedDynamicPruning extends ExtensionsTestBase {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    spark.catalog().dropTempView("dim");
  }

  @TestTemplate
  public void testExtensionsInstallTheRule() {
    sql("SELECT 1");

    assertThat(NestedDynamicPruning.installed(spark)).isTrue();
  }

  @TestTemplate
  public void testSelfJoinsDoNotNestPruningSubqueries() {
    sql("CREATE TABLE %s (id BIGINT, key BIGINT) USING iceberg PARTITIONED BY (key)", tableName);
    sql("INSERT INTO %s SELECT id, id %% 10 FROM range(0, 1000)", tableName);

    // without the rule, the plan of this query holds 121 pruning filters
    assertSelfJoinsPruneWithoutNesting(
        5, "Joins on the partition column should prune without nesting");
  }

  @TestTemplate
  public void testSelfJoinedDataColumnsDoNotNestPruningSubqueries() {
    // only scans of Vortex tables offer data columns to runtime filtering
    sql(
        "CREATE TABLE %s (id BIGINT, key BIGINT) USING iceberg "
            + "TBLPROPERTIES ('write.format.default' = 'vortex')",
        tableName);
    sql("INSERT INTO %s SELECT id, id %% 10 FROM range(0, 1000)", tableName);

    assertSelfJoinsPruneWithoutNesting(3, "Joins on the data column should prune without nesting");
  }

  private void assertSelfJoinsPruneWithoutNesting(int levels, String description) {
    // files keep the optimizer from folding the dimension into a constant
    String dimLocation = temp.resolve("dim").toString();
    spark
        .range(1, 3)
        .selectExpr("id AS key", "IF(id = 1, 'one', 'two') AS name")
        .write()
        .parquet(dimLocation);
    spark.read().parquet(dimLocation).createOrReplaceTempView("dim");

    // each level joins the level below with itself, which nests pruning subqueries without the
    // rule
    StringBuilder query =
        new StringBuilder(
            String.format(
                "WITH l1 AS (SELECT f.id, f.key FROM %s f JOIN dim d ON f.key = d.key AND d.name = 'one')",
                tableName));
    for (int level = 2; level <= levels; level += 1) {
      query.append(
          String.format(
              ", l%d AS (SELECT a.id, a.key FROM l%d a JOIN l%d b ON a.key = b.key AND a.id = b.id)",
              level, level - 1, level - 1));
    }

    query.append(String.format(" SELECT count(*), sum(id) FROM l%d", levels));

    List<Row> plan = spark.sql("EXPLAIN EXTENDED " + query).collectAsList();
    int pruningFilters =
        StringUtils.countMatches(plan.get(0).getString(0), "dynamicpruningexpression");
    assertThat(pruningFilters).as(description).isBetween(1, 20);

    String pruning = SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED().key();
    spark.conf().set(pruning, "false");
    List<Object[]> expected;
    try {
      expected = sql(query.toString());
    } finally {
      spark.conf().unset(pruning);
    }

    assertEquals("Should have expected rows", expected, sql(query.toString()));
  }
}
