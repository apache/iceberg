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
package org.apache.iceberg.rest;

import static org.apache.iceberg.TestBase.FILE_A;
import static org.apache.iceberg.TestBase.FILE_A_DELETES;
import static org.apache.iceberg.TestBase.FILE_B_DELETES;
import static org.apache.iceberg.TestBase.PARTITION_SPECS_BY_ID;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

public class TestTableScanResponseParser {

  private static final String TASK_WITHOUT_RESIDUAL =
      "{\"data-file\":{\"spec-id\":0,\"content\":\"data\","
          + "\"file-path\":\"/path/to/data-a.parquet\",\"file-format\":\"parquet\","
          + "\"partition\":[0],\"file-size-in-bytes\":10,\"record-count\":1}";

  @Test
  public void missingResidualFilterUsesScanFilter() {
    Expression scanFilter = Expressions.equal("id", 1);
    List<FileScanTask> tasks =
        TableScanResponseParser.parseFileScanTasks(
            JsonUtil.parse("{\"file-scan-tasks\":[" + TASK_WITHOUT_RESIDUAL + "}]}", node -> node),
            List.of(),
            PARTITION_SPECS_BY_ID,
            true,
            scanFilter);

    FileScanTask task = tasks.get(0);
    Expression expected =
        ResidualEvaluator.of(SPEC, scanFilter, true).residualFor(task.file().partition());
    assertThat(task.residual().toString()).isEqualTo(expected.toString());
  }

  @Test
  public void residualFilterTakesPrecedenceOverScanFilter() {
    List<FileScanTask> tasks =
        TableScanResponseParser.parseFileScanTasks(
            JsonUtil.parse(
                "{\"file-scan-tasks\":[" + TASK_WITHOUT_RESIDUAL + ",\"residual-filter\":true}]}",
                node -> node),
            List.of(),
            PARTITION_SPECS_BY_ID,
            true,
            Expressions.equal("id", 1));

    assertThat(tasks.get(0).residual()).isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void cannotSerializeScanTaskWithDeleteFileMissingFromDeleteFiles() {
    ResidualEvaluator residualEvaluator =
        ResidualEvaluator.of(SPEC, Expressions.alwaysTrue(), true);
    FileScanTask fileScanTask =
        new BaseFileScanTask(
            FILE_A,
            new DeleteFile[] {FILE_A_DELETES},
            SchemaParser.toJson(SCHEMA),
            PartitionSpecParser.toJson(SPEC),
            residualEvaluator);

    // The task references FILE_A_DELETES, but the delete files list only contains FILE_B_DELETES,
    // so the referenced delete file cannot be resolved to an index.
    assertThatThrownBy(
            () ->
                JsonUtil.generate(
                    gen -> {
                      gen.writeStartObject();
                      TableScanResponseParser.serializeScanTasks(
                          List.of(fileScanTask),
                          List.of(FILE_B_DELETES),
                          PARTITION_SPECS_BY_ID,
                          gen);
                      gen.writeEndObject();
                    },
                    false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot serialize scan task with delete file missing from delete files: "
                + FILE_A_DELETES.location());
  }
}
