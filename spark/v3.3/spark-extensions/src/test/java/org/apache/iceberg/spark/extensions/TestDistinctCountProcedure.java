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
import java.util.Map;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDistinctCountProcedure extends SparkExtensionsTestBase {

  public TestDistinctCountProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void setupTable() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testAnalyze() {
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    sql("INSERT INTO %s VALUES (3, 'a')", tableName);
    sql("INSERT INTO %s VALUES (4, 'b')", tableName);
    sql("INSERT INTO %s VALUES (5, 'a')", tableName);
    sql("INSERT INTO %s VALUES (6, 'b')", tableName);

    List<Object[]> returns =
        sql(
            "CALL %s.system.distinct_count("
                + "table => '%s',"
                + "distinct_count_view => '%s',"
                + "columns => array('%s','%s'))",
            catalogName, tableName, "result", "id", "data");

    Table table = validationCatalog.loadTable(tableIdent);
    List files = table.statisticsFiles();
    List<BlobMetadata> metadataList = ((GenericStatisticsFile) files.get(0)).blobMetadata();

    BlobMetadata firstBlob = metadataList.get(0);
    assertThat(firstBlob.type()).as("type").isEqualTo(StandardBlobTypes.NDV_BLOB);
    assertThat(firstBlob.fields()).as("columns").isEqualTo(ImmutableList.of(0));
    assertThat(Long.parseLong(firstBlob.properties().get("ndv"))).isEqualTo(6);

    BlobMetadata secondBlob = metadataList.get(1);
    assertThat(secondBlob.type()).as("type").isEqualTo(StandardBlobTypes.NDV_BLOB);
    assertThat(secondBlob.fields()).as("columns").isEqualTo(ImmutableList.of(1));
    assertThat(Long.parseLong(secondBlob.properties().get("ndv"))).isEqualTo(2);

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match", ImmutableList.of(row(6L, 2L)), sql("select * from %s", viewName));
  }
}
