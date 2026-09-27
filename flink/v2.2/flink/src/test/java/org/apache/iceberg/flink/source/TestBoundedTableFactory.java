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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.types.Row;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.junit.jupiter.api.Test;

public class TestBoundedTableFactory extends ChangeLogTableTestBase {

  @Test
  public void testEmptyDataSet() {
    List<List<Row>> emptyDataSet = ImmutableList.of();

    String dataId = BoundedTableFactory.registerDataSet(emptyDataSet);
    sql(
        "CREATE TABLE %s(id INT, data STRING) WITH ('connector'='BoundedSource', 'data-id'='%s')",
        tableName, dataId);

    assertThat(sql("SELECT * FROM %s", tableName)).isEmpty();
  }

  @Test
  public void testBoundedTableFactory() {
    List<List<Row>> dataSet =
        ImmutableList.of(
            ImmutableList.of(
                insertRow(1, "aaa"),
                deleteRow(1, "aaa"),
                insertRow(1, "bbb"),
                insertRow(2, "aaa"),
                deleteRow(2, "aaa"),
                insertRow(2, "bbb")),
            ImmutableList.of(
                updateBeforeRow(2, "bbb"),
                updateAfterRow(2, "ccc"),
                deleteRow(2, "ccc"),
                insertRow(2, "ddd")),
            ImmutableList.of(
                deleteRow(1, "bbb"),
                insertRow(1, "ccc"),
                deleteRow(1, "ccc"),
                insertRow(1, "ddd")));

    String dataId = BoundedTableFactory.registerDataSet(dataSet);
    sql(
        "CREATE TABLE %s(id INT, data STRING) WITH ('connector'='BoundedSource', 'data-id'='%s')",
        tableName, dataId);

    List<Row> rowSet = dataSet.stream().flatMap(Streams::stream).collect(Collectors.toList());
    assertThat(sql("SELECT * FROM %s", tableName)).isEqualTo(rowSet);

    assertThat(sql("SELECT * FROM %s WHERE data='aaa'", tableName))
        .isEqualTo(
            rowSet.stream()
                .filter(r -> Objects.equals(r.getField(1), "aaa"))
                .collect(Collectors.toList()));
  }
}
