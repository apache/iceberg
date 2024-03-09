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

import static org.apache.iceberg.TableProperties.DELETE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.MERGE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED;
import static org.apache.iceberg.TableProperties.UPDATE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_HASH;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_RANGE;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE;

import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkDistributionAndOrderingUtil extends SparkTestBaseWithCatalog {

  private static final Distribution UNSPECIFIED_DISTRIBUTION = Distributions.unspecified();
  private static final Distribution FILE_CLUSTERED_DISTRIBUTION =
      Distributions.clustered(
          new Expression[] {Expressions.column(MetadataColumns.FILE_PATH.name())});
  private static final Distribution SPEC_ID_PARTITION_CLUSTERED_DISTRIBUTION =
      Distributions.clustered(
          new Expression[] {
            Expressions.column(MetadataColumns.SPEC_ID.name()),
            Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME)
          });
  private static final Distribution SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION =
      Distributions.clustered(
          new Expression[] {
            Expressions.column(MetadataColumns.SPEC_ID.name()),
            Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
            Expressions.column(MetadataColumns.FILE_PATH.name())
          });

  private static final SortOrder[] EMPTY_ORDERING = new SortOrder[] {};
  private static final SortOrder[] FILE_POSITION_ORDERING =
      new SortOrder[] {
        Expressions.sort(
            Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(
            Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
      };
  private static final SortOrder[] SPEC_ID_PARTITION_ORDERING =
      new SortOrder[] {
        Expressions.sort(
            Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
        Expressions.sort(
            Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING)
      };
  private static final SortOrder[] SPEC_ID_PARTITION_FILE_ORDERING =
      new SortOrder[] {
        Expressions.sort(
            Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
        Expressions.sort(
            Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
        Expressions.sort(
            Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING)
      };
  private static final SortOrder[] SPEC_ID_PARTITION_FILE_POSITION_ORDERING =
      new SortOrder[] {
        Expressions.sort(
            Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
        Expressions.sort(
            Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
        Expressions.sort(
            Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(
            Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
      };

  @After
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  // =============================================================
  // Distribution and ordering for write operations such as APPEND
  // =============================================================
  //
  // UNPARTITIONED UNORDERED
  // -------------------------------------------------------------------------
  // write mode is NOT SET -> unspecified distribution + empty ordering
  // write mode is NONE -> unspecified distribution + empty ordering
  // write mode is HASH -> unspecified distribution + empty ordering
  // write mode is RANGE -> unspecified distribution + empty ordering
  //
  // UNPARTITIONED ORDERED BY id, data
  // -------------------------------------------------------------------------
  // write mode is NOT SET -> ORDER BY id, data
  // write mode is NONE -> unspecified distribution + LOCALLY ORDER BY id, data
  // write mode is HASH -> unspecified distribution + LOCALLY ORDER BY id, data
  // write mode is RANGE -> ORDER BY id, data
  //
  // PARTITIONED BY date, days(ts) UNORDERED
  // -------------------------------------------------------------------------
  // write mode is NOT SET -> CLUSTER BY date, days(ts) + LOCALLY ORDER BY date, days(ts)
  // write mode is NOT SET (fanout) -> CLUSTER BY date, days(ts) + empty ordering
  // write mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, days(ts)
  // write mode is NONE (fanout) -> unspecified distribution + empty ordering
  // write mode is HASH -> CLUSTER BY date, days(ts) + LOCALLY ORDER BY date, days(ts)
  // write mode is HASH (fanout) -> CLUSTER BY date, days(ts) + empty ordering
  // write mode is RANGE -> ORDER BY date, days(ts)
  // write mode is RANGE (fanout) -> RANGE DISTRIBUTE BY date, days(ts) + empty ordering
  //
  // PARTITIONED BY date ORDERED BY id
  // -------------------------------------------------------------------------
  // write mode is NOT SET -> ORDER BY date, id
  // write mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, id
  // write mode is HASH -> CLUSTER BY date + LOCALLY ORDER BY date, id
  // write mode is RANGE -> ORDER BY date, id

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

    table.updateProperties().set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    checkWriteDistributionAndOrdering(table, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testRangeWriteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    checkWriteDistributionAndOrdering(table, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultWriteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
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

    table.updateProperties().set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkWriteDistributionAndOrdering(table, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeWriteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultWritePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultWritePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true").commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testHashWritePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testHashWritePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testRangeWritePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeWritePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultWritePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().desc("id").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testHashWritePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    table.replaceSortOrder().asc("id").commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.bucket(8, "data")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeWritePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeWritePartitionedSortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").commit();

    table.updateProperties().set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
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
  // delete mode is NOT SET -> CLUSTER BY _file + empty ordering
  // delete mode is NONE -> unspecified distribution + empty ordering
  // delete mode is HASH -> CLUSTER BY _file + empty ordering
  // delete mode is RANGE -> RANGE DISTRIBUTE BY _file, _pos + empty ordering
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
  // delete mode is NOT SET -> CLUSTER BY date, days(ts) + LOCALLY ORDER BY date, days(ts)
  // delete mode is NOT SET (fanout) -> CLUSTER BY date, days(ts) + empty ordering
  // delete mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, days(ts)
  // delete mode is NONE (fanout) -> unspecified distribution + empty ordering
  // delete mode is HASH -> CLUSTER BY date, days(ts) + LOCALLY ORDER BY date, days(ts)
  // delete mode is HASH (fanout) -> CLUSTER BY date, days(ts) + empty ordering
  // delete mode is RANGE -> ORDER BY date, days(ts)
  // delete mode is RANGE (fanout) -> RANGE DISTRIBUTE BY date, days(ts) + empty ordering
  //
  // PARTITIONED BY date ORDERED BY id
  // -------------------------------------------------------------------------
  // delete mode is NOT SET -> CLUSTER BY date + LOCALLY ORDER BY date, id
  // delete mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, id
  // delete mode is HASH -> CLUSTER BY date + LOCALLY ORDER BY date, id
  // delete mode is RANGE -> ORDER BY date, id

  @Test
  public void testDefaultCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    checkCopyOnWriteDistributionAndOrdering(
        table, DELETE, FILE_CLUSTERED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testNoneCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    checkCopyOnWriteDistributionAndOrdering(
        table, DELETE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    checkCopyOnWriteDistributionAndOrdering(
        table, DELETE, FILE_CLUSTERED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testRangeCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    Distribution expectedDistribution = Distributions.ordered(FILE_POSITION_ORDERING);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultCopyOnWriteDeleteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, DELETE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteDeleteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, DELETE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteDeleteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, DELETE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteDeleteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteDeletePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteDeletePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true").commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testNoneCopyOnWriteDeletePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, DELETE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteDeletePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    checkCopyOnWriteDistributionAndOrdering(
        table, DELETE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashCopyOnWriteDeletePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteDeletePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testRangeCopyOnWriteDeletePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteDeletePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };
    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultCopyOnWriteDeletePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().desc("id").commit();

    Expression[] expectedClustering = new Expression[] {Expressions.identity("date")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteDeletePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    table.replaceSortOrder().desc("id").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, DELETE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteDeletePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    table.replaceSortOrder().asc("id").commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.bucket(8, "data")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, DELETE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteDeletePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    table.replaceSortOrder().asc("id").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
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
  // update mode is NOT SET -> CLUSTER BY _file + empty ordering
  // update mode is NONE -> unspecified distribution + empty ordering
  // update mode is HASH -> CLUSTER BY _file + empty ordering
  // update mode is RANGE -> RANGE DISTRIBUTE BY _file, _pos + empty ordering
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
  // update mode is NOT SET -> CLUSTER BY date, days(ts) + LOCALLY ORDER BY date, days(ts)
  // update mode is NOT SET (fanout) -> CLUSTER BY _file + empty ordering
  // update mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, days(ts)
  // update mode is NONE (fanout) -> unspecified distribution + empty ordering
  // update mode is HASH -> CLUSTER BY date, days(ts) + LOCALLY ORDER BY date, days(ts)
  // update mode is HASH (fanout) -> CLUSTER BY date, days(ts) + empty ordering
  // update mode is RANGE -> ORDER BY date, days(ts)
  // update mode is RANGE (fanout) -> RANGE DISTRIBUTED BY date, days(ts) + empty ordering
  //
  // PARTITIONED BY date ORDERED BY id
  // -------------------------------------------------------------------------
  // update mode is NOT SET -> CLUSTER BY date + LOCALLY ORDER BY date, id
  // update mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, id
  // update mode is HASH -> CLUSTER BY date + LOCALLY ORDER BY date, id
  // update mode is RANGE -> ORDER BY date, id

  @Test
  public void testDefaultCopyOnWriteUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    checkCopyOnWriteDistributionAndOrdering(
        table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testNoneCopyOnWriteUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    checkCopyOnWriteDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashCopyOnWriteUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    checkCopyOnWriteDistributionAndOrdering(
        table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testRangeCopyOnWriteUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    Distribution expectedDistribution = Distributions.ordered(FILE_POSITION_ORDERING);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultCopyOnWriteUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, UPDATE, FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteUpdatePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteUpdatePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true").commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testNoneCopyOnWriteUpdatePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteUpdatePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    checkCopyOnWriteDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashCopyOnWriteUpdatePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteUpdatePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testRangeCopyOnWriteUpdatePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteUpdatePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };
    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultCopyOnWriteUpdatePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().desc("id").commit();

    Expression[] expectedClustering = new Expression[] {Expressions.identity("date")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteUpdatePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    table.replaceSortOrder().desc("id").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteUpdatePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    table.replaceSortOrder().asc("id").commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.bucket(8, "data")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteUpdatePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    table.replaceSortOrder().asc("id").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, UPDATE, expectedDistribution, expectedOrdering);
  }

  // =============================================================
  // Distribution and ordering for copy-on-write MERGE operations
  // =============================================================
  //
  // UNPARTITIONED UNORDERED
  // -------------------------------------------------------------------------
  // merge mode is NOT SET -> use write distribution and ordering
  // merge mode is NONE -> unspecified distribution + empty ordering
  // merge mode is HASH -> unspecified distribution + empty ordering
  // merge mode is RANGE -> unspecified distribution + empty ordering
  //
  // UNPARTITIONED ORDERED BY id, data
  // -------------------------------------------------------------------------
  // merge mode is NOT SET -> use write distribution and ordering
  // merge mode is NONE -> unspecified distribution + LOCALLY ORDER BY id, data
  // merge mode is HASH -> unspecified distribution + LOCALLY ORDER BY id, data
  // merge mode is RANGE -> ORDER BY id, data
  //
  // PARTITIONED BY date, days(ts) UNORDERED
  // -------------------------------------------------------------------------
  // merge mode is NOT SET -> CLUSTER BY date, days(ts) + LOCALLY ORDER BY date, days(ts)
  // merge mode is NOT SET (fanout) -> CLUSTER BY date, days(ts) + empty ordering
  // merge mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, days(ts)
  // merge mode is NONE (fanout) -> unspecified distribution + empty ordering
  // merge mode is HASH -> CLUSTER BY date, days(ts) + LOCALLY ORDER BY date, days(ts)
  // merge mode is HASH (fanout) -> CLUSTER BY date, days(ts) + empty ordering
  // merge mode is RANGE -> ORDER BY date, days(ts)
  // merge mode is RANGE (fanout) -> RANGE DISTRIBUTE BY date, days(ts) + empty ordering
  //
  // PARTITIONED BY date ORDERED BY id
  // -------------------------------------------------------------------------
  // merge mode is NOT SET -> CLUSTER BY date + LOCALLY ORDER BY date, id
  // merge mode is NONE -> unspecified distribution + LOCALLY ORDERED BY date, id
  // merge mode is HASH -> CLUSTER BY date + LOCALLY ORDER BY date, id
  // merge mode is RANGE -> ORDERED BY date, id

  @Test
  public void testDefaultCopyOnWriteMergeUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testNoneCopyOnWriteMergeUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashCopyOnWriteMergeUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testRangeCopyOnWriteMergeUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultCopyOnWriteMergeUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteMergeUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, MERGE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteMergeUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, MERGE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteMergeUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteMergePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteMergePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true").commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testNoneCopyOnWriteMergePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, MERGE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteMergePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashCopyOnWriteMergePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteMergePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.days("ts")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testRangeCopyOnWriteMergePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteMergePartitionedUnsortedTableFanout() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };
    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultCopyOnWriteMergePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().desc("id").commit();

    Expression[] expectedClustering = new Expression[] {Expressions.identity("date")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testNoneCopyOnWriteMergePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    table.replaceSortOrder().desc("id").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(
        table, MERGE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteMergePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    table.replaceSortOrder().asc("id").commit();

    Expression[] expectedClustering =
        new Expression[] {Expressions.identity("date"), Expressions.bucket(8, "data")};
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteMergePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    table.replaceSortOrder().asc("id").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  // ===================================================================================
  // Distribution and ordering for merge-on-read DELETE operations with position deletes
  // ===================================================================================
  //
  // UNPARTITIONED (ORDERED & UNORDERED)
  // -------------------------------------------------------------------------
  // delete mode is NOT SET -> CLUSTER BY _spec_id, _partition, _file +
  //                           LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // delete mode is NOT SET (fanout) -> CLUSTER BY _spec_id, _partition, _file + empty ordering
  // delete mode is NONE -> unspecified distribution +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // delete mode is NOT SET (fanout) -> CLUSTER BY _spec_id, _partition, _file + empty ordering
  // delete mode is HASH -> CLUSTER BY _spec_id, _partition, _file  +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // delete mode is HASH (fanout) -> CLUSTER BY _spec_id, _partition, _file + empty ordering
  // delete mode is RANGE -> RANGE DISTRIBUTE BY _spec_id, _partition, _file +
  //                         LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // delete mode is RANGE (fanout) -> RANGE DISTRIBUTE BY _spec_id, _partition, _file +
  //                                  empty ordering
  //
  // PARTITIONED BY date, days(ts) (ORDERED & UNORDERED)
  // -------------------------------------------------------------------------
  // delete mode is NOT SET -> CLUSTER BY _spec_id, _partition +
  //                           LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // delete mode is NOT SET (fanout) -> CLUSTER BY _spec_id, _partition + empty ordering
  // delete mode is NONE -> unspecified distribution +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // delete mode is NONE (fanout) -> unspecified distribution + empty ordering
  // delete mode is HASH -> CLUSTER BY _spec_id, _partition +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // delete mode is HASH (fanout) -> CLUSTER BY _spec_id, _partition + empty ordering
  // delete mode is RANGE -> RANGE DISTRIBUTE BY _spec_id, _partition +
  //                         LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // delete mode is RANGE (fanout) -> RANGE DISTRIBUTE BY _spec_id, _partition + empty ordering

  @Test
  public void testDefaultPositionDeltaDeleteUnpartitionedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    checkPositionDeltaDistributionAndOrdering(
        table,
        DELETE,
        SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION,
        SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, DELETE, SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testNonePositionDeltaDeleteUnpartitionedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    checkPositionDeltaDistributionAndOrdering(
        table, DELETE, UNSPECIFIED_DISTRIBUTION, SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, DELETE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashPositionDeltaDeleteUnpartitionedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    checkPositionDeltaDistributionAndOrdering(
        table,
        DELETE,
        SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION,
        SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, DELETE, SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testRangePositionDeltaDeleteUnpartitionedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    Distribution expectedDistribution = Distributions.ordered(SPEC_ID_PARTITION_FILE_ORDERING);

    checkPositionDeltaDistributionAndOrdering(
        table, DELETE, expectedDistribution, SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, DELETE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultPositionDeltaDeletePartitionedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    checkPositionDeltaDistributionAndOrdering(
        table,
        DELETE,
        SPEC_ID_PARTITION_CLUSTERED_DISTRIBUTION,
        SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, DELETE, SPEC_ID_PARTITION_CLUSTERED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testNonePositionDeltaDeletePartitionedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    checkPositionDeltaDistributionAndOrdering(
        table, DELETE, UNSPECIFIED_DISTRIBUTION, SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, DELETE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashPositionDeltaDeletePartitionedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    checkPositionDeltaDistributionAndOrdering(
        table,
        DELETE,
        SPEC_ID_PARTITION_CLUSTERED_DISTRIBUTION,
        SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, DELETE, SPEC_ID_PARTITION_CLUSTERED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testRangePositionDeltaDeletePartitionedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    Distribution expectedDistribution = Distributions.ordered(SPEC_ID_PARTITION_ORDERING);

    checkPositionDeltaDistributionAndOrdering(
        table, DELETE, expectedDistribution, SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, DELETE, expectedDistribution, EMPTY_ORDERING);
  }

  // ===================================================================================
  // Distribution and ordering for merge-on-read UPDATE operations with position deletes
  // ===================================================================================
  //
  // IMPORTANT: updates are represented as delete and insert
  // IMPORTANT: metadata columns like _spec_id and _partition are NULL for new insert rows
  //
  // UNPARTITIONED UNORDERED
  // -------------------------------------------------------------------------
  // update mode is NOT SET -> CLUSTER BY _spec_id, _partition, _file +
  //                           LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // update mode is NOT SET (fanout) -> CLUSTER BY _spec_id, _partition, _file + empty ordering
  // update mode is NONE -> unspecified distribution +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // update mode is NONE (fanout) -> unspecified distribution + empty ordering
  // update mode is HASH -> CLUSTER BY _spec_id, _partition, _file +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // update mode is HASH (fanout) -> CLUSTER BY _spec_id, _partition, _file + empty ordering
  // update mode is RANGE -> RANGE DISTRIBUTE BY _spec_id, _partition, _file +
  //                         LOCALLY ORDERED BY _spec_id, _partition, _file, _pos
  // update mode is RANGE (fanout) -> RANGE DISTRIBUTE BY _spec_id, _partition, _file + empty
  // ordering
  //
  // UNPARTITIONED ORDERED BY id, data
  // -------------------------------------------------------------------------
  // update mode is NOT SET -> CLUSTER BY _spec_id, _partition, _file +
  //                           LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, id, data
  // update mode is NONE -> unspecified distribution +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, id, data
  // update mode is HASH -> CLUSTER BY _spec_id, _partition, _file +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, id, data
  // update mode is RANGE -> RANGE DISTRIBUTE BY _spec_id, _partition, _file, id, data +
  //                         LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, id, data
  //
  // PARTITIONED BY date, days(ts) UNORDERED
  // -------------------------------------------------------------------------
  // update mode is NOT SET -> CLUSTER BY _spec_id, _partition, date, days(ts) +
  //                           LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, days(ts)
  // update mode is NOT SET (fanout) -> CLUSTER BY _spec_id, _partition, date, days(ts) +
  //                                    empty ordering
  // update mode is NONE -> unspecified distribution +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, days(ts)
  // update mode is NONE (fanout) -> unspecified distribution + empty ordering
  // update mode is HASH -> CLUSTER BY _spec_id, _partition, date, days(ts) +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, days(ts)
  // update mode is HASH (fanout) -> CLUSTER BY _spec_id, _partition, date, days(ts) +
  //                                 empty ordering
  // update mode is RANGE -> RANGE DISTRIBUTE BY _spec_id, _partition, date, days(ts) +
  //                         LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, days(ts)
  // update mode is RANGE (fanout) -> RANGE DISTRIBUTE BY _spec_id, _partition, date, days(ts) +
  //                                  empty ordering
  //
  // PARTITIONED BY date ORDERED BY id
  // -------------------------------------------------------------------------
  // update mode is NOT SET -> CLUSTER BY _spec_id, _partition, date +
  //                           LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, id
  // update mode is NONE -> unspecified distribution +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, id
  // update mode is HASH -> CLUSTER BY _spec_id, _partition, date +
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, id
  // update mode is RANGE -> RANGE DISTRIBUTE BY _spec_id, _partition, date, id +
  //                         LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, id

  @Test
  public void testDefaultPositionDeltaUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    checkPositionDeltaDistributionAndOrdering(
        table,
        UPDATE,
        SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION,
        SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testNonePositionDeltaUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashPositionDeltaUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    checkPositionDeltaDistributionAndOrdering(
        table,
        UPDATE,
        SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION,
        SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testRangePositionDeltaUpdateUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    Distribution expectedDistribution = Distributions.ordered(SPEC_ID_PARTITION_FILE_ORDERING);

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, expectedDistribution, SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    table.updateProperties().set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true").commit();

    checkPositionDeltaDistributionAndOrdering(table, UPDATE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultPositionDeltaUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testNonePositionDeltaUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").asc("data").commit();

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashPositionDeltaUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").asc("data").commit();

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, SPEC_ID_PARTITION_FILE_CLUSTERED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testRangePositionDeltaUpdateUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").asc("data").commit();

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    SortOrder[] expectedDistributionOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };
    Distribution expectedDistribution = Distributions.ordered(expectedDistributionOrdering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultPositionDeltaUpdatePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.identity("date"),
          Expressions.bucket(8, "data")
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, expectedDistribution, expectedOrdering);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, UPDATE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testNonePositionDeltaUpdatePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashPositionDeltaUpdatePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.identity("date"),
          Expressions.bucket(8, "data")
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, expectedDistribution, expectedOrdering);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, UPDATE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testRangePositionDeltaUpdatePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    SortOrder[] expectedDistributionOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING)
        };
    Distribution expectedDistribution = Distributions.ordered(expectedDistributionOrdering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, expectedDistribution, expectedOrdering);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, UPDATE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultPositionDeltaUpdatePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").commit();

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.identity("date"),
          Expressions.bucket(8, "data")
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testNonePositionDeltaUpdatePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").commit();

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashPositionDeltaUpdatePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").commit();

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.identity("date"),
          Expressions.bucket(8, "data")
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangePositionDeltaUpdatePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").commit();

    table.updateProperties().set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    SortOrder[] expectedDistributionOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };
    Distribution expectedDistribution = Distributions.ordered(expectedDistributionOrdering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, UPDATE, expectedDistribution, expectedOrdering);
  }

  // ==================================================================================
  // Distribution and ordering for merge-on-read MERGE operations with position deletes
  // ==================================================================================
  //
  // IMPORTANT: updates are represented as delete and insert
  // IMPORTANT: metadata columns like _spec_id and _partition are NULL for new rows
  //
  // UNPARTITIONED UNORDERED
  // -------------------------------------------------------------------------
  // merge mode is NOT SET -> CLUSTER BY _spec_id, _partition, _file +
  //                          LOCALLY ORDER BY _spec_id, _partition, _file, _pos
  // merge mode is NOT SET (fanout) -> CLUSTER BY _spec_id, _partition, _file + empty ordering
  // merge mode is NONE -> unspecified distribution +
  //                       LOCALLY ORDER BY _spec_id, _partition, _file, _pos
  // merge mode is NONE (fanout) -> unspecified distribution + empty ordering
  // merge mode is HASH -> CLUSTER BY _spec_id, _partition, _file +
  //                       LOCALLY ORDER BY _spec_id, _partition, _file, _pos
  // merge mode is HASH (fanout) -> CLUSTER BY _spec_id, _partition, _file +
  //                                empty ordering
  // merge mode is RANGE -> RANGE DISTRIBUTE BY _spec_id, _partition, _file +
  //                        LOCALLY ORDER BY _spec_id, _partition, _file, _pos
  // merge mode is RANGE (fanout) -> RANGE DISTRIBUTE BY _spec_id, _partition, _file +
  //                                 empty ordering
  //
  // UNPARTITIONED ORDERED BY id, data
  // -------------------------------------------------------------------------
  // merge mode is NOT SET -> CLUSTER BY _spec_id, _partition, _file +
  //                          LOCALLY ORDER BY _spec_id, _partition, _file, _pos, id, data
  // merge mode is NONE -> unspecified distribution +
  //                       LOCALLY ORDER BY _spec_id, _partition, _file, _pos, id, data
  // merge mode is HASH -> CLUSTER BY _spec_id, _partition, _file +
  //                       LOCALLY ORDER BY _spec_id, _partition, _file, _pos, id, data
  // merge mode is RANGE -> RANGE DISTRIBUTE BY _spec_id, _partition, _file, id, data +
  //                        LOCALLY ORDER BY _spec_id, _partition, _file, _pos, id, data
  //
  // PARTITIONED BY date, days(ts) UNORDERED
  // -------------------------------------------------------------------------
  // merge mode is NOT SET -> CLUSTER BY _spec_id, _partition, date, days(ts) +
  //                          LOCALLY ORDER BY _spec_id, _partition, _file, _pos, date, days(ts)
  // merge mode is NOT SET (fanout) -> CLUSTER BY _spec_id, _partition, date, days(ts) +
  //                                   empty ordering
  // merge mode is NONE -> unspecified distribution +
  //                       LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, days(ts)
  // merge mode is NONE (fanout) -> unspecified distribution + empty ordering
  // merge mode is HASH -> CLUSTER BY _spec_id, _partition, date, days(ts) +
  //                       LOCALLY ORDER BY _spec_id, _partition, _file, _pos, date, days(ts)
  // merge mode is HASH (fanout) -> CLUSTER BY _spec_id, _partition, date, days(ts) +
  //                                empty ordering
  // merge mode is RANGE -> RANGE DISTRIBUTE BY _spec_id, _partition, date, days(ts) +
  //                        LOCALLY ORDER BY _spec_id, _partition, _file, _pos, date, days(ts)
  // merge mode is RANGE (fanout) -> RANGE DISTRIBUTE BY _spec_id, _partition, date, days(ts) +
  //                                 empty ordering
  //
  // PARTITIONED BY date ORDERED BY id
  // -------------------------------------------------------------------------
  // merge mode is NOT SET -> CLUSTER BY _spec_id, _partition, date +
  //                          LOCALLY ORDER BY _spec_id, _partition, _file, _pos, date, id
  // merge mode is NONE -> unspecified distribution +
  //                       LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, id
  // merge mode is HASH -> CLUSTER BY _spec_id, _partition, date +
  //                       LOCALLY ORDER BY _spec_id, _partition, _file, _pos, date, id
  // merge mode is RANGE -> RANGE DISTRIBUTE BY _spec_id, _partition, date, id
  //                        LOCALLY ORDERED BY _spec_id, _partition, _file, _pos, date, id

  @Test
  public void testDefaultPositionDeltaMergeUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.column(MetadataColumns.FILE_PATH.name())
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    checkPositionDeltaDistributionAndOrdering(
        table, MERGE, expectedDistribution, SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testNonePositionDeltaMergeUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    checkPositionDeltaDistributionAndOrdering(
        table, MERGE, UNSPECIFIED_DISTRIBUTION, SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, MERGE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashPositionDeltaMergeUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.column(MetadataColumns.FILE_PATH.name())
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    checkPositionDeltaDistributionAndOrdering(
        table, MERGE, expectedDistribution, SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testRangePositionDeltaMergeUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    SortOrder[] expectedDistributionOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING)
        };
    Distribution expectedDistribution = Distributions.ordered(expectedDistributionOrdering);

    checkPositionDeltaDistributionAndOrdering(
        table, MERGE, expectedDistribution, SPEC_ID_PARTITION_FILE_POSITION_ORDERING);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testDefaultPositionDeltaMergeUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").asc("data").commit();

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.column(MetadataColumns.FILE_PATH.name())
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testNonePositionDeltaMergeUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, MERGE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testHashPositionDeltaMergeUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.column(MetadataColumns.FILE_PATH.name())
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangePositionDeltaMergeUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    table.replaceSortOrder().asc("id").asc("data").commit();

    SortOrder[] expectedDistributionOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };
    Distribution expectedDistribution = Distributions.ordered(expectedDistributionOrdering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultPositionDeltaMergePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.identity("date"),
          Expressions.days("ts")
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testNonePositionDeltaMergePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, MERGE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(
        table, MERGE, UNSPECIFIED_DISTRIBUTION, EMPTY_ORDERING);
  }

  @Test
  public void testHashPositionDeltaMergePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.identity("date"),
          Expressions.days("ts")
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testRangePositionDeltaMergePartitionedUnsortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    SortOrder[] expectedDistributionOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };
    Distribution expectedDistribution = Distributions.ordered(expectedDistributionOrdering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);

    enableFanoutWriters(table);

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, EMPTY_ORDERING);
  }

  @Test
  public void testNonePositionDeltaMergePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE).commit();

    table.replaceSortOrder().desc("id").commit();

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(
        table, MERGE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
  }

  @Test
  public void testDefaultPositionDeltaMergePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder().asc("id").commit();

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.identity("date"),
          Expressions.bucket(8, "data")
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testHashPositionDeltaMergePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, bucket(8, data))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();

    table.replaceSortOrder().asc("id").commit();

    Expression[] expectedClustering =
        new Expression[] {
          Expressions.column(MetadataColumns.SPEC_ID.name()),
          Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME),
          Expressions.identity("date"),
          Expressions.bucket(8, "data")
        };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.bucket(8, "data"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangePositionDeltaMergePartitionedSortedTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();

    table.replaceSortOrder().asc("id").commit();

    SortOrder[] expectedDistributionOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };
    Distribution expectedDistribution = Distributions.ordered(expectedDistributionOrdering);

    SortOrder[] expectedOrdering =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column(MetadataColumns.SPEC_ID.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
          Expressions.sort(
              Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
          Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
        };

    checkPositionDeltaDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
  }

  private void checkWriteDistributionAndOrdering(
      Table table, Distribution expectedDistribution, SortOrder[] expectedOrdering) {
    SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());

    SparkWriteRequirements requirements = writeConf.writeRequirements();

    Distribution distribution = requirements.distribution();
    Assert.assertEquals("Distribution must match", expectedDistribution, distribution);

    SortOrder[] ordering = requirements.ordering();
    Assert.assertArrayEquals("Ordering must match", expectedOrdering, ordering);
  }

  private void checkCopyOnWriteDistributionAndOrdering(
      Table table,
      Command command,
      Distribution expectedDistribution,
      SortOrder[] expectedOrdering) {
    SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());

    SparkWriteRequirements requirements = writeConf.copyOnWriteRequirements(command);

    Distribution distribution = requirements.distribution();
    Assert.assertEquals("Distribution must match", expectedDistribution, distribution);

    SortOrder[] ordering = requirements.ordering();
    Assert.assertArrayEquals("Ordering must match", expectedOrdering, ordering);
  }

  private void checkPositionDeltaDistributionAndOrdering(
      Table table,
      Command command,
      Distribution expectedDistribution,
      SortOrder[] expectedOrdering) {
    SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());

    SparkWriteRequirements requirements = writeConf.positionDeltaRequirements(command);

    Distribution distribution = requirements.distribution();
    Assert.assertEquals("Distribution must match", expectedDistribution, distribution);

    SortOrder[] ordering = requirements.ordering();
    Assert.assertArrayEquals("Ordering must match", expectedOrdering, ordering);
  }

  private void enableFanoutWriters(Table table) {
    table.updateProperties().set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true").commit();
  }
}
