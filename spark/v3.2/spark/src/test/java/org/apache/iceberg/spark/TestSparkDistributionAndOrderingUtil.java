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
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TableProperties.DELETE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_HASH;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_RANGE;
import static org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command.DELETE;

public class TestSparkDistributionAndOrderingUtil extends SparkTestBaseWithCatalog {

  @After
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testDefaultWriteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Distribution expectedDistribution = Distributions.unspecified();
    SortOrder[] expectedOrdering = new SortOrder[]{};
    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testHashWriteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    Distribution expectedDistribution = Distributions.unspecified();
    SortOrder[] expectedOrdering = new SortOrder[]{};
    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeWriteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    Distribution expectedDistribution = Distributions.unspecified();
    SortOrder[] expectedOrdering = new SortOrder[]{};
    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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

    Distribution expectedDistribution = Distributions.unspecified();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column("data"), SortDirection.ASCENDING)
    };

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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

    Distribution expectedDistribution = Distributions.unspecified();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
    };

    checkWriteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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

  @Test
  public void testDefaultCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Distribution expectedDistribution = Distributions.unspecified();
    SortOrder[] expectedOrdering = new SortOrder[]{};
    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testHashCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
        .commit();

    Expression[] expectedClustering = new Expression[]{
        Expressions.column(MetadataColumns.FILE_PATH.name()),
    };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testRangeCopyOnWriteDeleteUnpartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties()
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
        .commit();

    Expression[] expectedClustering = new Expression[]{
        Expressions.column(MetadataColumns.FILE_PATH.name()),
    };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteDeleteUnpartitionedSortedTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table.replaceSortOrder()
        .asc("id")
        .asc("data")
        .commit();

    Expression[] expectedClustering = new Expression[]{
        Expressions.column(MetadataColumns.FILE_PATH.name()),
    };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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

    Expression[] expectedClustering = new Expression[]{
        Expressions.column(MetadataColumns.FILE_PATH.name()),
    };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
  }

  @Test
  public void testDefaultCopyOnWriteDeletePartitionedUnsortedTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date, days(ts))", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Distribution expectedDistribution = Distributions.unspecified();

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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

    Expression[] expectedClustering = new Expression[]{
        Expressions.column(MetadataColumns.FILE_PATH.name()),
    };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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
        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
    };

    Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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

    Expression[] expectedClustering = new Expression[]{
        Expressions.column(MetadataColumns.FILE_PATH.name()),
    };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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

    Expression[] expectedClustering = new Expression[]{
        Expressions.column(MetadataColumns.FILE_PATH.name()),
    };
    Distribution expectedDistribution = Distributions.clustered(expectedClustering);

    SortOrder[] expectedOrdering = new SortOrder[]{
        Expressions.sort(Expressions.column(MetadataColumns.FILE_PATH.name()), SortDirection.ASCENDING),
        Expressions.sort(Expressions.column(MetadataColumns.ROW_POSITION.name()), SortDirection.ASCENDING)
    };

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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

    checkCopyOnWriteDeleteDistributionAndOrdering(table, expectedDistribution, expectedOrdering);
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

  private void checkCopyOnWriteDeleteDistributionAndOrdering(Table table, Distribution expectedDistribution,
                                                             SortOrder[] expectedOrdering) {
    SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());
    DistributionMode mode = writeConf.copyOnWriteDeleteDistributionMode();
    Distribution distribution = SparkDistributionAndOrderingUtil.buildCopyOnWriteDistribution(table, DELETE, mode);
    Assert.assertEquals("Distribution must match", expectedDistribution, distribution);

    SortOrder[] ordering = SparkDistributionAndOrderingUtil.buildCopyOnWriteOrdering(table, DELETE, distribution);
    Assert.assertArrayEquals("Ordering must match", expectedOrdering, ordering);
  }
}
