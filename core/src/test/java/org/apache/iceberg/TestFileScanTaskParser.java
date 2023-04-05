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

import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestFileScanTaskParser {
  @Test
  public void testNullArguments() {
    Assertions.assertThatThrownBy(() -> FileScanTaskParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("File scan task cannot be null");

    Assertions.assertThatThrownBy(() -> FileScanTaskParser.fromJson(null, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse file scan task from null JSON string");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testParser(boolean caseSensitive) {
    PartitionSpec spec = TableTestBase.SPEC;
    FileScanTask fileScanTask = createScanTask(spec, caseSensitive);
    String jsonStr = FileScanTaskParser.toJson(fileScanTask);
    FileScanTask deserializedTask = FileScanTaskParser.fromJson(jsonStr, caseSensitive);
    assertFileScanTaskEquals(fileScanTask, deserializedTask, spec, caseSensitive);
  }

  private FileScanTask createScanTask(PartitionSpec spec, boolean caseSensitive) {
    ResidualEvaluator residualEvaluator;
    if (spec.isUnpartitioned()) {
      residualEvaluator = ResidualEvaluator.unpartitioned(Expressions.alwaysTrue());
    } else {
      residualEvaluator = ResidualEvaluator.of(spec, Expressions.equal("id", 1), caseSensitive);
    }

    return new BaseFileScanTask(
        TableTestBase.FILE_A,
        new DeleteFile[] {TableTestBase.FILE_A_DELETES, TableTestBase.FILE_A2_DELETES},
        SchemaParser.toJson(TableTestBase.SCHEMA),
        PartitionSpecParser.toJson(spec),
        residualEvaluator);
  }

  private static void assertFileScanTaskEquals(
      FileScanTask expected, FileScanTask actual, PartitionSpec spec, boolean caseSensitive) {
    TestContentFileParser.assertContentFileEquals(expected.file(), actual.file(), spec);
    Assertions.assertThat(actual.deletes()).hasSameSizeAs(expected.deletes());
    for (int pos = 0; pos < expected.deletes().size(); ++pos) {
      TestContentFileParser.assertContentFileEquals(
          expected.deletes().get(pos), actual.deletes().get(pos), spec);
    }

    Assert.assertTrue("Schema should be the same", expected.schema().sameSchema(actual.schema()));
    Assert.assertEquals(expected.spec(), actual.spec());
    Assert.assertTrue(
        ExpressionUtil.equivalent(
            expected.residual(),
            actual.residual(),
            TableTestBase.SCHEMA.asStruct(),
            caseSensitive));
  }
}
