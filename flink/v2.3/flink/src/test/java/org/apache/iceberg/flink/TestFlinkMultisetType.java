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
package org.apache.iceberg.flink;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
class TestFlinkMultisetType extends TestBase {
  private static final String TABLE_NAME = "multiset_table";
  private static final List<String> TAGS = ImmutableList.of("a", "b", "a", "c", "a", "b");

  @Parameter(index = 0)
  private FileFormat fileFormat;

  @Parameters(name = "fileFormat={0}")
  protected static List<Object[]> parameters() {
    return ImmutableList.of(
        new Object[] {FileFormat.PARQUET},
        new Object[] {FileFormat.AVRO},
        new Object[] {FileFormat.ORC});
  }

  @AfterEach
  void dropTable() {
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);
  }

  @TestTemplate
  void sqlInsertMultiset() throws Exception {
    String warehouse = "file://" + temporaryDirectory.toAbsolutePath();
    sql(
        "CREATE TABLE %s (id INT, tags MULTISET<STRING>) WITH ("
            + "'connector'='iceberg', 'catalog-name'='multiset_catalog', 'catalog-type'='hadoop', "
            + "'warehouse'='%s', '%s'='%s')",
        TABLE_NAME, warehouse, DEFAULT_FILE_FORMAT, fileFormat.name());

    String values =
        TAGS.stream().map(tag -> "(1, '" + tag + "')").collect(Collectors.joining(", "));
    sql(
        "INSERT INTO %s SELECT id, COLLECT(tag) FROM (VALUES %s) AS t (id, tag) GROUP BY id",
        TABLE_NAME, values);

    Table table;
    try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouse)) {
      table = catalog.loadTable(TableIdentifier.of("default_database", TABLE_NAME));
    }

    Map<String, Integer> expectedCounts = Maps.newHashMap();
    TAGS.forEach(tag -> expectedCounts.merge(tag, 1, Integer::sum));

    List<Record> records = SimpleDataUtil.tableRecords(table);
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getField("tags")).isEqualTo(expectedCounts);
  }
}
