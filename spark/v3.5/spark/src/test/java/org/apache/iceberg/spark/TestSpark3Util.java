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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSpark3Util extends TestBase {

  @TempDir private java.nio.file.Path narrowScanTempDir;

  @Test
  public void testNarrowScanRootsPrunesPrefixColumn() throws Exception {
    createHiveLikeDirs("id=1/name=a", "id=2/name=b", "id=3/name=a");

    Path root = new Path(narrowScanTempDir.toUri());
    Pair<List<Path>, Map<String, String>> narrowed =
        Spark3Util.narrowScanRoots(spark, root, ImmutableMap.of("id", "2"));

    assertThat(narrowed.first()).hasSize(1);
    assertThat(narrowed.first().get(0).getName()).isEqualTo("id=2");
    assertThat(narrowed.second()).isEmpty();
  }

  @Test
  public void testNarrowScanRootsPrunesCompositePrefix() throws Exception {
    createHiveLikeDirs("id=1/name=a", "id=2/name=a", "id=2/name=b");

    Path root = new Path(narrowScanTempDir.toUri());
    Pair<List<Path>, Map<String, String>> narrowed =
        Spark3Util.narrowScanRoots(
            spark, root, ImmutableMap.of("id", "2", "name", "a"));

    assertThat(narrowed.first()).hasSize(1);
    assertThat(narrowed.first().get(0).toString()).endsWith("id=2/name=a");
    assertThat(narrowed.second()).isEmpty();
  }

  @Test
  public void testNarrowScanRootsKeepsNonPrefixFilterAsResidual() throws Exception {
    createHiveLikeDirs("id=1/name=a", "id=2/name=a");

    Path root = new Path(narrowScanTempDir.toUri());
    // 'name' is not the first on-disk partition column, so nothing can be pushed down.
    Pair<List<Path>, Map<String, String>> narrowed =
        Spark3Util.narrowScanRoots(spark, root, ImmutableMap.of("name", "a"));

    assertThat(narrowed.first()).hasSize(1);
    assertThat(narrowed.first().get(0).toString()).isEqualTo(root.toString());
    assertThat(narrowed.second()).containsExactlyEntriesOf(ImmutableMap.of("name", "a"));
  }

  @Test
  public void testNarrowScanRootsMissingValueFallsBackToRoot() throws Exception {
    createHiveLikeDirs("id=1/name=a", "id=2/name=a");

    Path root = new Path(narrowScanTempDir.toUri());
    Pair<List<Path>, Map<String, String>> narrowed =
        Spark3Util.narrowScanRoots(spark, root, ImmutableMap.of("id", "99"));

    // No id=99 dir exists; fall back so downstream can produce the usual "no matching partitions"
    // diagnostic against the original root.
    assertThat(narrowed.first()).containsExactly(root);
    assertThat(narrowed.second()).containsExactlyEntriesOf(ImmutableMap.of("id", "99"));
  }

  @Test
  public void testNarrowScanRootsEmptyFilterReturnsRoot() throws Exception {
    createHiveLikeDirs("id=1/name=a");

    Path root = new Path(narrowScanTempDir.toUri());
    Pair<List<Path>, Map<String, String>> narrowed =
        Spark3Util.narrowScanRoots(spark, root, ImmutableMap.of());

    assertThat(narrowed.first()).containsExactly(root);
    assertThat(narrowed.second()).isEmpty();
  }

  @Test
  public void testNarrowScanRootsDoesNotListSiblingPartitions() throws Exception {
    createHiveLikeDirs(
        "id=1/name=a/date=1", "id=2/name=b/date=2", "id=3/name=c/date=3", "id=4/name=d/date=4");

    Configuration conf = spark.sparkContext().hadoopConfiguration();
    String prevImpl = conf.get("fs.file.impl");
    boolean prevCacheDisabled = conf.getBoolean("fs.file.impl.disable.cache", false);
    conf.set("fs.file.impl", RecordingLocalFileSystem.class.getName());
    conf.setBoolean("fs.file.impl.disable.cache", true);
    FileSystem.closeAll();
    RecordingLocalFileSystem.reset();

    try {
      Path root = new Path(narrowScanTempDir.toUri());
      Pair<List<Path>, Map<String, String>> narrowed =
          Spark3Util.narrowScanRoots(spark, root, ImmutableMap.of("id", "3"));

      assertThat(narrowed.first()).hasSize(1);
      assertThat(narrowed.first().get(0).getName()).isEqualTo("id=3");

      List<String> listed = RecordingLocalFileSystem.snapshot();
      String rootPathStr = narrowScanTempDir.toString();
      // The root must have been probed (to detect the 'id' partition column at this level).
      assertThat(listed).anyMatch(p -> stripTrailingSlash(p).equals(rootPathStr));

      // Sibling partitions must NOT have been listed — that is the whole point of the pushdown.
      // Neither the id=* directory itself, nor anything under id=1/id=2/id=4.
      assertThat(listed)
          .as("optimized scan must not list sibling partitions, but did: %s", listed)
          .noneMatch(p -> stripTrailingSlash(p).endsWith("/id=1"))
          .noneMatch(p -> stripTrailingSlash(p).endsWith("/id=2"))
          .noneMatch(p -> stripTrailingSlash(p).endsWith("/id=4"))
          .noneMatch(p -> p.contains("/id=1/"))
          .noneMatch(p -> p.contains("/id=2/"))
          .noneMatch(p -> p.contains("/id=4/"));
    } finally {
      if (prevImpl == null) {
        conf.unset("fs.file.impl");
      } else {
        conf.set("fs.file.impl", prevImpl);
      }
      conf.setBoolean("fs.file.impl.disable.cache", prevCacheDisabled);
      FileSystem.closeAll();
    }
  }

  /**
   * A drop-in replacement for the default LocalFileSystem that records every {@code listStatus}
   * call. Used by {@link #testNarrowScanRootsDoesNotListSiblingPartitions()} to prove that the
   * partition-filter pushdown does not descend into sibling partitions.
   */
  public static class RecordingLocalFileSystem extends RawLocalFileSystem {
    private static final List<String> LISTED_PATHS =
        Collections.synchronizedList(new ArrayList<>());

    public static void reset() {
      LISTED_PATHS.clear();
    }

    public static List<String> snapshot() {
      synchronized (LISTED_PATHS) {
        return new ArrayList<>(LISTED_PATHS);
      }
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      LISTED_PATHS.add(f.toUri().getPath());
      return super.listStatus(f);
    }
  }

  @Test
  public void testNarrowScanRootsDoesNotListSiblingsAtDeeperLevel() throws Exception {
    createHiveLikeDirs(
        "id=1/name=a", "id=2/name=a", "id=3/name=a", "id=3/name=b", "id=3/name=c", "id=4/name=a");

    Configuration conf = spark.sparkContext().hadoopConfiguration();
    String prevImpl = conf.get("fs.file.impl");
    boolean prevCacheDisabled = conf.getBoolean("fs.file.impl.disable.cache", false);
    conf.set("fs.file.impl", RecordingLocalFileSystem.class.getName());
    conf.setBoolean("fs.file.impl.disable.cache", true);
    FileSystem.closeAll();
    RecordingLocalFileSystem.reset();

    try {
      Path root = new Path(narrowScanTempDir.toUri());
      Pair<List<Path>, Map<String, String>> narrowed =
          Spark3Util.narrowScanRoots(
              spark, root, ImmutableMap.of("id", "3", "name", "c"));

      assertThat(narrowed.first()).hasSize(1);
      assertThat(narrowed.first().get(0).toString()).endsWith("/id=3/name=c");

      List<String> listed = RecordingLocalFileSystem.snapshot();

      // id=3 MUST have been listed (to descend to the second partition level).
      assertThat(listed).anyMatch(p -> stripTrailingSlash(p).endsWith("/id=3"));

      // Sibling id=* directories at the top level must NOT have been listed.
      assertThat(listed)
          .as("sibling id partitions must not be listed at the first level: %s", listed)
          .noneMatch(p -> stripTrailingSlash(p).endsWith("/id=1"))
          .noneMatch(p -> stripTrailingSlash(p).endsWith("/id=2"))
          .noneMatch(p -> stripTrailingSlash(p).endsWith("/id=4"))
          .noneMatch(p -> p.contains("/id=1/"))
          .noneMatch(p -> p.contains("/id=2/"))
          .noneMatch(p -> p.contains("/id=4/"));

      // Sibling name=* directories under id=3 must NOT have been descended into either.
      assertThat(listed)
          .as("sibling name partitions under id=3 must not be listed: %s", listed)
          .noneMatch(p -> p.contains("/id=3/name=a/"))
          .noneMatch(p -> p.contains("/id=3/name=b/"));
    } finally {
      if (prevImpl == null) {
        conf.unset("fs.file.impl");
      } else {
        conf.set("fs.file.impl", prevImpl);
      }
      conf.setBoolean("fs.file.impl.disable.cache", prevCacheDisabled);
      FileSystem.closeAll();
    }
  }

  private static String stripTrailingSlash(String path) {
    return path.endsWith("/") && path.length() > 1 ? path.substring(0, path.length() - 1) : path;
  }

  private void createHiveLikeDirs(String... relativePartitions) throws Exception {
    for (String rel : relativePartitions) {
      java.nio.file.Path dir = narrowScanTempDir.resolve(rel);
      java.nio.file.Files.createDirectories(dir);
      // Add a placeholder file so listStatus sees a real subtree at the deepest level.
      java.nio.file.Files.createFile(dir.resolve("data.txt"));
    }
  }

  @Test
  public void testDescribeSortOrder() {
    Schema schema =
        new Schema(
            required(1, "data", Types.StringType.get()),
            required(2, "time", Types.TimestampType.withoutZone()));

    assertThat(Spark3Util.describe(buildSortOrder("Identity", schema, 1)))
        .as("Sort order isn't correct.")
        .isEqualTo("data DESC NULLS FIRST");

    assertThat(Spark3Util.describe(buildSortOrder("bucket[1]", schema, 1)))
        .as("Sort order isn't correct.")
        .isEqualTo("bucket(1, data) DESC NULLS FIRST");

    assertThat(Spark3Util.describe(buildSortOrder("truncate[3]", schema, 1)))
        .as("Sort order isn't correct.")
        .isEqualTo("truncate(data, 3) DESC NULLS FIRST");

    assertThat(Spark3Util.describe(buildSortOrder("year", schema, 2)))
        .as("Sort order isn't correct.")
        .isEqualTo("years(time) DESC NULLS FIRST");

    assertThat(Spark3Util.describe(buildSortOrder("month", schema, 2)))
        .as("Sort order isn't correct.")
        .isEqualTo("months(time) DESC NULLS FIRST");

    assertThat(Spark3Util.describe(buildSortOrder("day", schema, 2)))
        .as("Sort order isn't correct.")
        .isEqualTo("days(time) DESC NULLS FIRST");

    assertThat(Spark3Util.describe(buildSortOrder("hour", schema, 2)))
        .as("Sort order isn't correct.")
        .isEqualTo("hours(time) DESC NULLS FIRST");

    assertThat(Spark3Util.describe(buildSortOrder("unknown", schema, 1)))
        .as("Sort order isn't correct.")
        .isEqualTo("unknown(data) DESC NULLS FIRST");

    // multiple sort orders
    SortOrder multiOrder =
        SortOrder.builderFor(schema).asc("time", NULLS_FIRST).asc("data", NULLS_LAST).build();
    assertThat(Spark3Util.describe(multiOrder))
        .as("Sort order isn't correct.")
        .isEqualTo("time ASC NULLS FIRST, data ASC NULLS LAST");
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
            required(6, "time", Types.TimestampType.withoutZone()),
            required(7, "v", Types.VariantType.get()));

    assertThat(Spark3Util.describe(schema))
        .as("Schema description isn't correct.")
        .isEqualTo(
            "struct<data: list<string> not null,pairs: map<string, bigint>,time: timestamp not null,v: variant not null>");
  }

  @Test
  public void testLoadIcebergTable() throws Exception {
    spark.conf().set("spark.sql.catalog.hive", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.hive.type", "hive");
    spark.conf().set("spark.sql.catalog.hive.default-namespace", "default");

    String tableFullName = "hive.default.tbl";
    sql("CREATE TABLE %s (c1 bigint, c2 string, c3 string) USING iceberg", tableFullName);

    Table table = Spark3Util.loadIcebergTable(spark, tableFullName);
    assertThat(table.name()).isEqualTo(tableFullName);
  }

  @Test
  public void testLoadIcebergCatalog() throws Exception {
    spark.conf().set("spark.sql.catalog.test_cat", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.test_cat.type", "hive");
    Catalog catalog = Spark3Util.loadIcebergCatalog(spark, "test_cat");
    assertThat(catalog)
        .as("Should retrieve underlying catalog class")
        .isInstanceOf(CachingCatalog.class);
  }

  @Test
  public void testDescribeExpression() {
    Expression refExpression = equal("id", 1);
    assertThat(Spark3Util.describe(refExpression)).isEqualTo("id = 1");

    Expression yearExpression = greaterThan(year("ts"), 10);
    assertThat(Spark3Util.describe(yearExpression)).isEqualTo("year(ts) > 10");

    Expression monthExpression = greaterThanOrEqual(month("ts"), 10);
    assertThat(Spark3Util.describe(monthExpression)).isEqualTo("month(ts) >= 10");

    Expression dayExpression = lessThan(day("ts"), 10);
    assertThat(Spark3Util.describe(dayExpression)).isEqualTo("day(ts) < 10");

    Expression hourExpression = lessThanOrEqual(hour("ts"), 10);
    assertThat(Spark3Util.describe(hourExpression)).isEqualTo("hour(ts) <= 10");

    Expression bucketExpression = in(bucket("id", 5), 3);
    assertThat(Spark3Util.describe(bucketExpression)).isEqualTo("bucket[5](id) IN (3)");

    Expression truncateExpression = notIn(truncate("name", 3), "abc");
    assertThat(Spark3Util.describe(truncateExpression))
        .isEqualTo("truncate[3](name) NOT IN ('abc')");

    Expression andExpression = and(refExpression, yearExpression);
    assertThat(Spark3Util.describe(andExpression)).isEqualTo("(id = 1 AND year(ts) > 10)");
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
