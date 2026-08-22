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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteDataFilesReportOnly extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private List<Object[]> callReportOnly(String extraOptions) {
    return sql(
        "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort',"
            + " options => map('report-only', 'true'%s))",
        catalogName, tableIdent, extraOptions);
  }

  @TestTemplate
  public void reportOnEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);
    assertThat(callReportOnly("")).isEmpty();
  }

  @TestTemplate
  public void reportOnUnsortedTableFails() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    assertThatThrownBy(() -> callReportOnly(""))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("does not declare a sort order");
  }

  @TestTemplate
  public void reportOverlappingUnpartitionedTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    // three appends with the same key: every file's [lower, upper] range contains id=50, so all
    // three files overlap regardless of how the rows of each insert are split into files
    sql("INSERT INTO TABLE %s VALUES (50, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'c')", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    List<Object[]> result = callReportOnly("");
    assertThat(result).hasSize(1);
    Object[] row = result.get(0);
    assertThat(row[0]).isEqualTo(0); // rewritten_data_files_count
    assertThat(row[2]).isEqualTo(0L); // rewritten_bytes_count
    assertThat(row[5]).isNull(); // partition (unpartitioned)
    assertThat(row[6]).isEqualTo(3); // max_overlap_depth
    assertThat(row[8]).isNull(); // candidate_file_count (no min-overlap-depth given)
    assertThat(row[10]).isEqualTo(0); // missing_bounds_file_count
  }

  @TestTemplate
  public void reportDisjointFilesDepthOne() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (10, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (20, 'b')", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    List<Object[]> result = callReportOnly("");
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[6]).isEqualTo(1);
  }

  @TestTemplate
  public void reportPerPartitionRows() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (7, 'a'), (7, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (7, 'a')", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    List<Object[]> result = callReportOnly("");
    assertThat(result).hasSize(2);
    for (Object[] row : result) {
      String partition = row[5].toString();
      if (partition.contains("data=a")) {
        assertThat(row[6]).isEqualTo(2); // both files contain id=7 -> ranges overlap
      } else {
        assertThat(row[6]).isEqualTo(1);
      }
    }
  }

  @TestTemplate
  public void reportWithMinOverlapDepthCountsCandidates() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (200, 'c')", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    List<Object[]> result = callReportOnly(", 'min-overlap-depth', '2'");
    assertThat(result).hasSize(1);
    Object[] row = result.get(0);
    assertThat(row[8]).isEqualTo(2); // the two id=50 files sit in a depth-2 region
    assertThat((Long) row[9]).isPositive(); // candidate_bytes
  }

  @TestTemplate
  public void reportOnlyDoesNotCommit() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'b')", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    int snapshotsBefore = Iterables.size(table.snapshots());

    callReportOnly("");

    table.refresh();
    assertThat(Iterables.size(table.snapshots())).isEqualTo(snapshotsBefore);
  }

  @TestTemplate
  public void invalidReportOnlyValueFails() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s',"
                        + " options => map('report-only', 'maybe'))",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be true or false");
  }

  @TestTemplate
  public void reportOnlyWithBinpackFails() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'binpack',"
                        + " options => map('report-only', 'true'))",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires the sort strategy");
  }

  @TestTemplate
  public void reportOnlyWithSortOrderFails() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort',"
                        + " sort_order => 'zorder(id, data)',"
                        + " options => map('report-only', 'true'))",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be used with sort_order");
  }

  @TestTemplate
  public void reportOnlyWithWhereFails() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort',"
                        + " where => 'id > 0', options => map('report-only', 'true'))",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be used with a where filter");
  }

  @TestTemplate
  public void reportOnlyFalseRunsRewrite() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'b')", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    List<Object[]> result =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort',"
                + " options => map('report-only', 'false', 'min-input-files', '1',"
                + " 'rewrite-all', 'true'))",
            catalogName, tableIdent);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo(2); // rewritten_data_files_count
    assertThat(result.get(0)[5]).isNull(); // overlap columns null in normal mode
  }
}
