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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assume.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestFlinkTimestampNano {

  @Parameter(index = 0)
  private FileFormat format;

  @Parameters(name = "format = {0},")
  public static Object[][] parameters() {
    return new Object[][] {{FileFormat.ORC}, {FileFormat.PARQUET}, {FileFormat.AVRO}};
  }

  public static final Schema TS_SCHEMA =
      new Schema(
          required(1, "ts_nano", Types.TimestampNanoType.withoutZone()),
          required(2, "ts_nano_tz", Types.TimestampNanoType.withZone()));
  @TempDir protected Path temporaryDirectory;

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  @Test
  public void testInvalidVersion() throws Exception {
    assertThatThrownBy(
            () -> CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TS_SCHEMA))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid type in v2 schema: ts_nano timestamp_ns is not supported until v3");
  }

  @TestTemplate
  public void testUseTimestampNano() throws Exception {
    assumeThat(format == FileFormat.PARQUET)
        .as("TIMESTAMP_NANO only supported for Parquet")
        .isTrue();

    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                TS_SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                    TableProperties.DEFAULT_FILE_FORMAT,
                    format.name(),
                    TableProperties.FORMAT_VERSION,
                    "3"));

    List<Record> expectedRecords = RandomGenericData.generate(TS_SCHEMA, 2, 0L);
    new GenericAppenderHelper(table, format, temporaryDirectory).appendToTable(expectedRecords);
    FlinkSource.Builder builder =
        FlinkSource.forRowData().table(table).tableLoader(CATALOG_EXTENSION.tableLoader());
    List<Row> actual = runFormat(builder.buildFormat());
    TestHelpers.assertRecords(actual, expectedRecords, TS_SCHEMA);
  }

  private List<Row> runFormat(FlinkInputFormat inputFormat) throws IOException {
    RowType rowType = FlinkSchemaUtil.convert(TS_SCHEMA);
    return TestHelpers.readRows(inputFormat, rowType);
  }
}
