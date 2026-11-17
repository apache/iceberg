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
package org.apache.iceberg.spark.sql;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests whole-stage codegen of outer joins whose generated class grows large enough for the join's
 * functions to be placed into nested classes (SPARK-60403).
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestOuterJoinCodegen extends TestBaseWithCatalog {

  // Writing the two structs of the join output generates enough code to move the join's functions
  // into nested classes. Whole-stage codegen skips plans with more fields than
  // spark.sql.codegen.maxFields, so the tests raise that limit.
  private static final int NUM_FIELDS = 5000;

  // the left table has ids 0 to 9 and the right table has ids 5 to 14
  private static final int LEFT_END = 10;
  private static final int RIGHT_START = 5;
  private static final int RIGHT_END = 15;

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName("l"));
    sql("DROP TABLE IF EXISTS %s", tableName("r"));
  }

  @TestTemplate
  public void testOuterJoinsCallFunctionsThroughNestedClasses() {
    createTable(tableName("l"), 0, LEFT_END);
    createTable(tableName("r"), RIGHT_START, RIGHT_END);

    checkJoin("MERGE(r)", "FULL OUTER", RIGHT_END, "findNextJoinRows", "consumeFullOuterJoinRow");
    checkJoin("SHUFFLE_HASH(r)", "FULL OUTER", RIGHT_END, "consumeOuterJoinRow");
    // a left outer join that builds its hash table from the left side
    checkJoin("SHUFFLE_HASH(l)", "LEFT OUTER", LEFT_END, "consumeOuterJoinRow");
  }

  private void checkJoin(String hint, String joinType, int endId, String... functions) {
    withSQLConf(
        ImmutableMap.of(
            SQLConf.WHOLESTAGE_MAX_NUM_FIELDS().key(),
            String.valueOf(Integer.MAX_VALUE),
            SQLConf.CODEGEN_FALLBACK().key(),
            "false"),
        () -> {
          Dataset<Row> df =
              spark.sql(
                  String.format(
                      "SELECT /*+ %s */ l.id, l.s, r.id, r.s FROM %s l %s JOIN %s r ON l.id = r.id",
                      hint, tableName("l"), joinType, tableName("r")));
          List<Row> rows = df.collectAsList();

          assertCallsThroughNestedClasses(df.queryExecution().executedPlan(), functions);

          List<Object[]> expected =
              IntStream.range(0, endId)
                  .mapToObj(
                      id -> {
                        boolean left = id < LEFT_END;
                        boolean right = id >= RIGHT_START;
                        return row(
                            left ? id : null,
                            left ? lastFieldValue(id) : null,
                            right ? id : null,
                            right ? lastFieldValue(id) : null);
                      })
                  .collect(Collectors.toList());
          List<Object[]> actual =
              rows.stream()
                  .map(r -> row(r.get(0), lastField(r, 1), r.get(2), lastField(r, 3)))
                  .sorted(Comparator.comparing(TestOuterJoinCodegen::joinKey))
                  .collect(Collectors.toList());
          assertEquals(hint + " " + joinType + " join", expected, actual);
        });
  }

  private void createTable(String name, int startId, int endId) {
    String type =
        IntStream.range(0, NUM_FIELDS)
            .mapToObj(i -> "f" + i + ": INT")
            .collect(Collectors.joining(", ", "STRUCT<", ">"));
    String value =
        IntStream.range(0, NUM_FIELDS)
            .mapToObj(i -> String.format("'f%d', CAST(id + %d AS INT)", i, i))
            .collect(Collectors.joining(", ", "named_struct(", ")"));

    sql("CREATE TABLE %s (id INT, s %s) USING iceberg", name, type);
    sql(
        "INSERT INTO %s SELECT CAST(id AS INT), %s FROM range(%d, %d)",
        name, value, startId, endId);
  }

  // the id of the left row, or of the right row when there is no left row
  private static Integer joinKey(Object[] values) {
    return (Integer) (values[0] != null ? values[0] : values[2]);
  }

  private static Integer lastField(Row row, int pos) {
    return row.isNullAt(pos) ? null : row.getStruct(pos).getInt(NUM_FIELDS - 1);
  }

  private static int lastFieldValue(int id) {
    return id + NUM_FIELDS - 1;
  }
}
