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

import static org.apache.iceberg.data.FileHelpers.encrypt;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestMergingMetrics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.sink.FlinkFileWriterFactory;
import org.apache.iceberg.io.DataWriter;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestFlinkMergingMetrics extends TestMergingMetrics<RowData> {

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension("test_db", "test_table");

  @Override
  protected DataFile writeAndGetDataFile(List<Record> records) throws IOException {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    RowType flinkSchema = FlinkSchemaUtil.convert(SCHEMA);
    DataWriter<RowData> writer =
        new FlinkFileWriterFactory.Builder(table)
            .dataSchema(SCHEMA)
            .dataFlinkType(flinkSchema)
            .build()
            .newDataWriter(
                encrypt(Files.localOutput(File.createTempFile("junit", null, tempDir))),
                PartitionSpec.unpartitioned(),
                null);
    try (writer) {
      records.stream().map(r -> RowDataConverter.convert(SCHEMA, r)).forEach(writer::write);
    }

    return writer.toDataFile();
  }
}
