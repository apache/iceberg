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

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.NullOrder.NULLS_LAST;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.hour;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestSpark3Util extends SparkTestBase {
  @Test
  public void testDescribeSortOrder() {
    Schema schema =
        new Schema(
            required(1, "data", Types.StringType.get()),
            required(2, "time", Types.TimestampType.withoutZone()));

    Assert.assertEquals(
        "Sort order isn't correct.",
        "data DESC NULLS FIRST",
        Spark3Util.describe(buildSortOrder("Identity", schema, 1)));
    Assert.assertEquals(
        "Sort order isn't correct.",
        "bucket(1, data) DESC NULLS FIRST",
        Spark3Util.describe(buildSortOrder("bucket[1]", schema, 1)));
    Assert.assertEquals(
        "Sort order isn't correct.",
        "truncate(data, 3) DESC NULLS FIRST",
        Spark3Util.describe(buildSortOrder("truncate[3]", schema, 1)));
    Assert.assertEquals(
        "Sort order isn't correct.",
        "years(time) DESC NULLS FIRST",
        Spark3Util.describe(buildSortOrder("year", schema, 2)));
    Assert.assertEquals(
        "Sort order isn't correct.",
        "months(time) DESC NULLS FIRST",
        Spark3Util.describe(buildSortOrder("month", schema, 2)));
    Assert.assertEquals(
        "Sort order isn't correct.",
        "days(time) DESC NULLS FIRST",
        Spark3Util.describe(buildSortOrder("day", schema, 2)));
    Assert.assertEquals(
        "Sort order isn't correct.",
        "hours(time) DESC NULLS FIRST",
        Spark3Util.describe(buildSortOrder("hour", schema, 2)));
    Assert.assertEquals(
        "Sort order isn't correct.",
        "unknown(data) DESC NULLS FIRST",
        Spark3Util.describe(buildSortOrder("unknown", schema, 1)));

    // multiple sort orders
    SortOrder multiOrder =
        SortOrder.builderFor(schema).asc("time", NULLS_FIRST).asc("data", NULLS_LAST).build();
    Assert.assertEquals(
        "Sort order isn't correct.",
        "time ASC NULLS FIRST, data ASC NULLS LAST",
        Spark3Util.describe(multiOrder));
  }

  @Test
  public void testDescribeSchema() {
    Schema schema =
        new Schema(
            required(1, "data", Types.ListType.ofRequired(2, Types.StringType.get())),
            optional(
                3,
                "pairs",
                Types.MapType.ofOptional(4, 5, Types.StringType.get(), Types.LongType.get())),
            required(6, "time", Types.TimestampType.withoutZone()));

    Assert.assertEquals(
        "Schema description isn't correct.",
        "struct<data: list<string> not null,pairs: map<string, bigint>,time: timestamp not null>",
        Spark3Util.describe(schema));
  }

  @Test
  public void testLoadIcebergTable() throws Exception {
    spark.conf().set("spark.sql.catalog.hive", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.hive.type", "hive");
    spark.conf().set("spark.sql.catalog.hive.default-namespace", "default");

    String tableFullName = "hive.default.tbl";
    sql("CREATE TABLE %s (c1 bigint, c2 string, c3 string) USING iceberg", tableFullName);

    Table table = Spark3Util.loadIcebergTable(spark, tableFullName);
    Assert.assertTrue(table.name().equals(tableFullName));
  }

  @Test
  public void testLoadIcebergCatalog() throws Exception {
    spark.conf().set("spark.sql.catalog.test_cat", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.test_cat.type", "hive");
    Catalog catalog = Spark3Util.loadIcebergCatalog(spark, "test_cat");
    Assert.assertTrue(
        "Should retrieve underlying catalog class", catalog instanceof CachingCatalog);
  }

  @Test
  public void testDescribeExpression() {
    Expression refExpression = equal("id", 1);
    Assertions.assertThat(Spark3Util.describe(refExpression)).isEqualTo("id = 1");

    Expression yearExpression = greaterThan(year("ts"), 10);
    Assertions.assertThat(Spark3Util.describe(yearExpression)).isEqualTo("year(ts) > 10");

    Expression monthExpression = greaterThanOrEqual(month("ts"), 10);
    Assertions.assertThat(Spark3Util.describe(monthExpression)).isEqualTo("month(ts) >= 10");

    Expression dayExpression = lessThan(day("ts"), 10);
    Assertions.assertThat(Spark3Util.describe(dayExpression)).isEqualTo("day(ts) < 10");

    Expression hourExpression = lessThanOrEqual(hour("ts"), 10);
    Assertions.assertThat(Spark3Util.describe(hourExpression)).isEqualTo("hour(ts) <= 10");

    Expression bucketExpression = in(bucket("id", 5), 3);
    Assertions.assertThat(Spark3Util.describe(bucketExpression)).isEqualTo("bucket[5](id) IN (3)");

    Expression truncateExpression = notIn(truncate("name", 3), "abc");
    Assertions.assertThat(Spark3Util.describe(truncateExpression))
        .isEqualTo("truncate[3](name) NOT IN ('abc')");

    Expression andExpression = and(refExpression, yearExpression);
    Assertions.assertThat(Spark3Util.describe(andExpression))
        .isEqualTo("(id = 1 AND year(ts) > 10)");
  }

  private SortOrder buildSortOrder(String transform, Schema schema, int sourceId) {
    String jsonString =
        "{\n"
            + "  \"order-id\" : 10,\n"
            + "  \"fields\" : [ {\n"
            + "    \"transform\" : \""
            + transform
            + "\",\n"
            + "    \"source-id\" : "
            + sourceId
            + ",\n"
            + "    \"direction\" : \"desc\",\n"
            + "    \"null-order\" : \"nulls-first\"\n"
            + "  } ]\n"
            + "}";

    return SortOrderParser.fromJson(schema, jsonString);
  }
}
