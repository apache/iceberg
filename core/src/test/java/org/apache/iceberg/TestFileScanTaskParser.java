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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestFileScanTaskParser {
  @Test
  public void testNullArguments() {
    assertThatThrownBy(() -> ScanTaskParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid scan task: null");

    assertThatThrownBy(() -> ScanTaskParser.fromJson(null, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON string for scan task: null");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testScanTaskParser(boolean caseSensitive) {
    PartitionSpec spec = TestBase.SPEC;
    FileScanTask fileScanTask = createFileScanTask(spec, caseSensitive);
    String jsonStr = ScanTaskParser.toJson(fileScanTask);
    assertThat(jsonStr).isEqualTo(fileScanTaskJson());
    FileScanTask deserializedTask = ScanTaskParser.fromJson(jsonStr, caseSensitive);
    assertFileScanTaskEquals(fileScanTask, deserializedTask, spec, caseSensitive);
  }

  /** Test backward compatibility where task-type field is absent from the JSON string */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testScanTaskParserWithoutTaskTypeField(boolean caseSensitive) {
    PartitionSpec spec = TestBase.SPEC;
    FileScanTask fileScanTask = createFileScanTask(spec, caseSensitive);
    FileScanTask deserializedTask =
        ScanTaskParser.fromJson(fileScanTaskJsonWithoutTaskType(), caseSensitive);
    assertFileScanTaskEquals(fileScanTask, deserializedTask, spec, caseSensitive);
  }

  @Test
  public void testFileScanTaskParsesFieldIdPartitionMap() {
    boolean caseSensitive = true;
    PartitionSpec spec = TestBase.SPEC;
    FileScanTask expected = createFileScanTask(spec, caseSensitive);

    FileScanTask deserializedTask =
        ScanTaskParser.fromJson(fileScanTaskFieldIdPartitionMapJson(), caseSensitive);

    assertFileScanTaskEquals(expected, deserializedTask, spec, caseSensitive);
  }

  private FileScanTask createFileScanTask(PartitionSpec spec, boolean caseSensitive) {
    ResidualEvaluator residualEvaluator;
    if (spec.isUnpartitioned()) {
      residualEvaluator = ResidualEvaluator.unpartitioned(Expressions.alwaysTrue());
    } else {
      residualEvaluator = ResidualEvaluator.of(spec, Expressions.equal("id", 1), caseSensitive);
    }

    return new BaseFileScanTask(
        TestBase.FILE_A,
        new DeleteFile[] {TestBase.FILE_A_DELETES, TestBase.FILE_A2_DELETES},
        SchemaParser.toJson(TestBase.SCHEMA),
        PartitionSpecParser.toJson(spec),
        residualEvaluator);
  }

  private String fileScanTaskJsonWithoutTaskType() {
    return "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
        + "{\"id\":3,\"name\":\"id\",\"required\":true,\"type\":\"int\"},"
        + "{\"id\":4,\"name\":\"data\",\"required\":true,\"type\":\"string\"}]},"
        + "\"spec\":{\"spec-id\":0,\"fields\":[{\"name\":\"data_bucket\","
        + "\"transform\":\"bucket[16]\",\"source-id\":4,\"field-id\":1000}]},"
        + "\"data-file\":{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data-a.parquet\","
        + "\"file-format\":\"parquet\",\"partition\":[0],"
        + "\"file-size-in-bytes\":10,\"record-count\":1,\"sort-order-id\":0},"
        + "\"start\":0,\"length\":10,"
        + "\"delete-files\":[{\"spec-id\":0,\"content\":\"position-deletes\","
        + "\"file-path\":\"/path/to/data-a-deletes.parquet\",\"file-format\":\"parquet\","
        + "\"partition\":[0],\"file-size-in-bytes\":10,\"record-count\":1},"
        + "{\"spec-id\":0,\"content\":\"equality-deletes\",\"file-path\":\"/path/to/data-a2-deletes.parquet\","
        + "\"file-format\":\"parquet\",\"partition\":[0],\"file-size-in-bytes\":10,"
        + "\"record-count\":1,\"equality-ids\":[1],\"sort-order-id\":0}],"
        + "\"residual-filter\":{\"type\":\"eq\",\"term\":\"id\",\"value\":1}}";
  }

  private String fileScanTaskJson() {
    return "{\"task-type\":\"file-scan-task\","
        + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
        + "{\"id\":3,\"name\":\"id\",\"required\":true,\"type\":\"int\"},"
        + "{\"id\":4,\"name\":\"data\",\"required\":true,\"type\":\"string\"}]},"
        + "\"spec\":{\"spec-id\":0,\"fields\":[{\"name\":\"data_bucket\","
        + "\"transform\":\"bucket[16]\",\"source-id\":4,\"field-id\":1000}]},"
        + "\"data-file\":{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data-a.parquet\","
        + "\"file-format\":\"parquet\",\"partition\":[0],"
        + "\"file-size-in-bytes\":10,\"record-count\":1,\"sort-order-id\":0},"
        + "\"start\":0,\"length\":10,"
        + "\"delete-files\":[{\"spec-id\":0,\"content\":\"position-deletes\","
        + "\"file-path\":\"/path/to/data-a-deletes.parquet\",\"file-format\":\"parquet\","
        + "\"partition\":[0],\"file-size-in-bytes\":10,\"record-count\":1},"
        + "{\"spec-id\":0,\"content\":\"equality-deletes\",\"file-path\":\"/path/to/data-a2-deletes.parquet\","
        + "\"file-format\":\"parquet\",\"partition\":[0],\"file-size-in-bytes\":10,"
        + "\"record-count\":1,\"equality-ids\":[1],\"sort-order-id\":0}],"
        + "\"residual-filter\":{\"type\":\"eq\",\"term\":\"id\",\"value\":1}}";
  }

  private String fileScanTaskFieldIdPartitionMapJson() {
    return "{\"task-type\":\"file-scan-task\","
        + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
        + "{\"id\":3,\"name\":\"id\",\"required\":true,\"type\":\"int\"},"
        + "{\"id\":4,\"name\":\"data\",\"required\":true,\"type\":\"string\"}]},"
        + "\"spec\":{\"spec-id\":0,\"fields\":[{\"name\":\"data_bucket\","
        + "\"transform\":\"bucket[16]\",\"source-id\":4,\"field-id\":1000}]},"
        + "\"data-file\":{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data-a.parquet\","
        + "\"file-format\":\"parquet\",\"partition\":{\"1000\":0},"
        + "\"file-size-in-bytes\":10,\"record-count\":1,\"sort-order-id\":0},"
        + "\"start\":0,\"length\":10,"
        + "\"delete-files\":[{\"spec-id\":0,\"content\":\"position-deletes\","
        + "\"file-path\":\"/path/to/data-a-deletes.parquet\",\"file-format\":\"parquet\","
        + "\"partition\":{\"1000\":0},\"file-size-in-bytes\":10,\"record-count\":1},"
        + "{\"spec-id\":0,\"content\":\"equality-deletes\",\"file-path\":\"/path/to/data-a2-deletes.parquet\","
        + "\"file-format\":\"parquet\",\"partition\":{\"1000\":0},\"file-size-in-bytes\":10,"
        + "\"record-count\":1,\"equality-ids\":[1],\"sort-order-id\":0}],"
        + "\"residual-filter\":{\"type\":\"eq\",\"term\":\"id\",\"value\":1}}";
  }

  private static void assertFileScanTaskEquals(
      FileScanTask expected, FileScanTask actual, PartitionSpec spec, boolean caseSensitive) {
    TestContentFileParser.assertContentFileEquals(expected.file(), actual.file(), spec);
    assertThat(actual.deletes()).hasSameSizeAs(expected.deletes());
    for (int pos = 0; pos < expected.deletes().size(); ++pos) {
      TestContentFileParser.assertContentFileEquals(
          expected.deletes().get(pos), actual.deletes().get(pos), spec);
    }

    assertThat(actual.schema().asStruct()).isEqualTo(expected.schema().asStruct());
    assertThat(actual.spec()).isEqualTo(expected.spec());
    assertThat(
            ExpressionUtil.equivalent(
                expected.residual(), actual.residual(), TestBase.SCHEMA.asStruct(), caseSensitive))
        .as("Residual expression should match")
        .isTrue();
  }
}
