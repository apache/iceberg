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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
class TestScanBroadcastReuse extends TestBaseWithCatalog {

  private static final int NUM_JOINS = 6;
  private static final int NUM_TABLES = NUM_JOINS + 1;

  @AfterEach
  void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    for (int index = 1; index <= NUM_JOINS; index++) {
      sql("DROP TABLE IF EXISTS %s", joinedTableName(index));
    }
  }

  @TestTemplate
  void batchesOfSameScanShareBroadcasts() {
    createTable(tableName);

    SparkTable sparkTable = new SparkTable(validationCatalog.loadTable(tableIdent));
    Scan scan = sparkTable.newScanBuilder(CaseInsensitiveStringMap.empty()).build();
    scan.toBatch().planInputPartitions();

    long idBefore = nextBroadcastId();
    scan.toBatch().planInputPartitions();
    scan.toBatch().planInputPartitions();

    assertThat(broadcastsCreatedSince(idBefore)).isEqualTo(0);
  }

  @TestTemplate
  void nestedRuntimeFiltersDoNotMultiplyBroadcasts() {
    createTable(tableName);
    for (int index = 1; index <= NUM_JOINS; index++) {
      createTable(joinedTableName(index));
    }

    // each join gets a runtime filter built from everything to its left, including the runtime
    // filters of the previous joins, so the number of planned scan copies doubles with each join
    StringBuilder query = new StringBuilder();
    query.append(String.format("SELECT t0.id FROM %s t0", tableName));
    for (int index = 1; index <= NUM_JOINS; index++) {
      query.append(
          String.format(
              " LEFT JOIN %s t%d ON t0.ds = t%d.ds AND t0.id = t%d.id",
              joinedTableName(index), index, index, index));
    }
    query.append(" WHERE t0.id < 3");

    long idBefore = nextBroadcastId();
    List<Object[]> rows = sql(query.toString());
    long broadcasts = broadcastsCreatedSince(idBefore);

    assertThat(rows).hasSize(2);
    assertThat(broadcasts).isLessThan(10L * NUM_TABLES);
  }

  private void createTable(String name) {
    sql("CREATE TABLE %s (id BIGINT, ds STRING) USING iceberg PARTITIONED BY (ds)", name);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", name);
  }

  private String joinedTableName(int index) {
    return tableName + "_" + index;
  }

  // broadcast IDs are sequential, so the ID of a new broadcast is the number created before it
  private long nextBroadcastId() {
    Broadcast<Integer> probe = sparkContext.broadcast(0);
    long id = probe.id();
    probe.destroy();
    return id;
  }

  private long broadcastsCreatedSince(long idBefore) {
    return nextBroadcastId() - idBefore - 1;
  }
}
