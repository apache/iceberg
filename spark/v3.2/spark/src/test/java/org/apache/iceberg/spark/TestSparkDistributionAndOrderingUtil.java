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

import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TableProperties.DELETE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.UPDATE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_HASH;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_RANGE;
import static org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command.DELETE;
import static org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command.UPDATE;

public class TestSparkDistributionAndOrderingUtil extends SparkTestBaseWithCatalog {

  private static final Distribution UNSPECIFIED_DISTRIBUTION = Distributions.unspecified();
  private static final Distribution FILE_CLUSTERED_DISTRIBUTION = Distributions.clustered(new Expression[]{
      Expressions.column(MetadataColumns.FILE_PATH.name())
  });

  private static final SortOrder[] EMPTY_ORDERING = new SortOrder[]{};
  private static final SortOrder[] FILE_POSITION_ORDERING = new SortOrder[]{
      Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
      Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
  };

  @After
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testDefaultWriteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    checkWriteDistributionAndOrdering(table, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashWriteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    checkWriteDistributionAndOrdering(table, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testRangeWriteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    checkWriteDistributionAndOrdering(table, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultWriteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testHashWriteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    checkWriteDistributionAndOrdering(table, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeWriteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultWritePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
    };

    checkWriteDistributionAndOrdering(table, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashWritePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    Expression[] expectedClustering = new Expression[]{
        Expressions.identity("date"),
        Expressions.days("ts")
    };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
    };

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeWritePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultWritePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder()
        .desc("id")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testHashWritePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, bucket(8, data))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .commit();

    Expression[] expectedClustering = new Expression[]{
        Expressions.identity("date"),
        Expressions.bucket(8, "data")
    };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
    };

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeWritePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder()
        .asc("id")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  // =============================================================
  // Distribution and ordering for copy-on-write DELETE operations
  // =============================================================
  //
  // UNPARTITIONED UNORDERED
  // -------------------------------------------------------------------------
  // delete mode is NOT SET -> CLUSTER BY _file + LOCALLY ORDER BY _file, _pos
  // delete mode is NONE -> unspecified distribution + empty ordering
  // delete mode is HASH -> CLUSTER BY _file + LOCALLY ORDER BY _file, _pos
  // delete mode is RANGE -> ORDER BY _file, _pos
  //
  // UNPARTITIONED ORDERED BY id, data
  // -------------------------------------------------------------------------
  // delete mode is NOT SET -> CLUSTER BY _file + LOCALLY ORDER BY id, data
  // delete mode is NONE -> unspecified distribution + LOCALLY ORDER BY id, data
  // delete mode is HASH -> CLUSTER BY _file + LOCALLY ORDER BY id, data
  // delete mode is RANGE -> ORDER BY id, data
  //
  // PARTITIONED BY date, days(ts) UNORDERED
  // -------------------------------------------------------------------------
  // delete mode is NOT SET -> CLUSTER BY _file + LOCALLY ORDER BY date, days(ts), _file, _pos
  // delete mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, days(ts)
  // delete mode is HASH -> CLUSTER BY _file + LOCALLY ORDER BY date, days(ts), _file, _pos
  // delete mode is RANGE -> ORDER BY date, days(ts), _file, _pos
  //
  // PARTITIONED BY date ORDERED BY id
  // -------------------------------------------------------------------------
  // delete mode is NOT SET -> CLUSTER BY _file + LOCALLY ORDER BY date, id
  // delete mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, id
  // delete mode is HASH -> CLUSTER BY _file + LOCALLY ORDER BY date, id
  // delete mode is RANGE -> ORDERED BY date, id

  @Test
  public void testDefaultCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, FILE_CLUSTERED_DISTRIBUTION, FILE_POSITION_ORDERING);
  }

  @Test
  public void testNoneCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .commit();

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, FILE_CLUSTERED_DISTRIBUTION, FILE_POSITION_ORDERING);
  }

  @Test
  public void testRangeCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    Distribution expectedDistribution = Distributions.ordered(FILE_POSITION_ORDERING);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, FILE_POSITION_ORDERING);
  }

  @Test
  public void testDefaultCopyOnWriteDeleteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteDeleteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteDeleteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteDeleteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteDeletePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteDeletePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteDeletePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteDeletePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteDeletePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder()
        .desc("id")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteDeletePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .commit();

    table.replaceSortOrder()
        .desc("id")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteDeletePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, bucket(8, data))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteDeletePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, expectedOrdering);
  }

  // =============================================================
  // Distribution and ordering for copy-on-write UPDATE operations
  // =============================================================
  //
  // UNPARTITIONED UNORDERED
  // -------------------------------------------------------------------------
  // update mode is NOT SET -> CLUSTER BY _file + LOCALLY ORDER BY _file, _pos
  // update mode is NONE -> unspecified distribution + empty ordering
  // update mode is HASH -> CLUSTER BY _file + LOCALLY ORDER BY _file, _pos
  // update mode is RANGE -> ORDER BY _file, _pos
  //
  // UNPARTITIONED ORDERED BY id, data
  // -------------------------------------------------------------------------
  // update mode is NOT SET -> CLUSTER BY _file + LOCALLY ORDER BY id, data
  // update mode is NONE -> unspecified distribution + LOCALLY ORDER BY id, data
  // update mode is HASH -> CLUSTER BY _file + LOCALLY ORDER BY id, data
  // update mode is RANGE -> ORDER BY id, data
  //
  // PARTITIONED BY date, days(ts) UNORDERED
  // -------------------------------------------------------------------------
  // update mode is NOT SET -> CLUSTER BY _file + LOCALLY ORDER BY date, days(ts), _file, _pos
  // update mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, days(ts)
  // update mode is HASH -> CLUSTER BY _file + LOCALLY ORDER BY date, days(ts), _file, _pos
  // update mode is RANGE -> ORDER BY date, days(ts), _file, _pos
  //
  // PARTITIONED BY date ORDERED BY id
  // -------------------------------------------------------------------------
  // update mode is NOT SET -> CLUSTER BY _file + LOCALLY ORDER BY date, id
  // update mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, id
  // update mode is HASH -> CLUSTER BY _file + LOCALLY ORDER BY date, id
  // update mode is RANGE -> ORDERED BY date, id

  @Test
  public void testDefaultCopyOnWriteUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, FILE_POSITION_ORDERING);
  }

  @Test
  public void testNoneCopyOnWriteUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .commit();

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashCopyOnWriteUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, FILE_POSITION_ORDERING);
  }

  @Test
  public void testRangeCopyOnWriteUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    Distribution expectedDistribution = Distributions.ordered(FILE_POSITION_ORDERING);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, FILE_POSITION_ORDERING);
  }

  @Test
  public void testDefaultCopyOnWriteUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteUpdatePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }


  @Test
  public void testNoneCopyOnWriteUpdatePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteUpdatePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteUpdatePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteUpdatePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder()
        .desc("id")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteUpdatePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .commit();

    table.replaceSortOrder()
        .desc("id")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteUpdatePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, bucket(8, data))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteUpdatePartitionedSortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    table.replaceSortOrder()
        .asc("id")
        .commit();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, expectedOrdering);
  }

  private void checkWriteDistributionAndOrdering(Table table, Distribution expectedDistribution,
                                                 SortOrder[] expectedOrdering) {
    SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());
    DistributionMode distributionMode = writeConf.distributionMode();
    Distribution distribution = SparkDistributionAndOrderingUtil.buildRequiredDistribution(table, distributionMode);
    Assert.assertEquals("Distribution must match", expectedDistribution, distribution);

    SortOrder[] ordering = SparkDistributionAndOrderingUtil.buildRequiredOrdering(table, distribution);
    Assert.assertArrayEquals("Ordering must match", expectedOrdering, ordering);
  }

  private void checkCopyOnWriteDistributionAndOrdering(Table table, Command command,
                                                       Distribution expectedDistribution,
                                                       SortOrder[] expectedOrdering) {
    SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());

    DistributionMode mode = copyOnWriteDistributionMode(command, writeConf);

    Distribution distribution = SparkDistributionAndOrderingUtil.buildCopyOnWriteDistribution(table, command, mode);
    Assert.assertEquals("Distribution must match", expectedDistribution, distribution);

    SortOrder[] ordering = SparkDistributionAndOrderingUtil.buildCopyOnWriteOrdering(table, command, distribution);
    Assert.assertArrayEquals("Ordering must match", expectedOrdering, ordering);
  }

  private DistributionMode copyOnWriteDistributionMode(Command command, SparkWriteConf writeConf) {
    switch (command) {
      case DELETE:
        return writeConf.copyOnWriteDeleteDistributionMode();
      case UPDATE:
        return writeConf.copyOnWriteUpdateDistributionMode();
      case MERGE:
        return writeConf.copyOnWriteMergeDistributionMode();
      default:
        throw new IllegalArgumentException("Unexpected command: " + command);
    }
  }
}
