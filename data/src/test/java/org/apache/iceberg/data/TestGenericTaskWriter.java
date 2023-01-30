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
package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestGenericTaskWriter {
  @Rule public final TemporaryFolder temp = new TemporaryFolder();

  private final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "level", Types.StringType.get()),
          Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
          Types.NestedField.required(3, "message", Types.StringType.get()),
          Types.NestedField.optional(
              4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get())));

  private final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).hour("event_time").identity("level").build();
  private Table table;
  private List<Record> testRecords = Lists.newArrayList();
  private final String testFileFormat;

  @Parameterized.Parameters(name = "FileFormat = {0}")
  public static Object[][] parameters() {
    return new Object[][] {{"avro"}, {"orc"}, {"parquet"}};
  }

  public TestGenericTaskWriter(String fileFormat) throws IOException {
    this.testFileFormat = fileFormat;
  }

  @Before
  public void createTable() throws IOException {
    File testWareHouse = temp.newFolder();
    if (testWareHouse.exists()) {
      Assert.assertTrue(testWareHouse.delete());
    }
    Catalog catalog = new HadoopCatalog(new Configuration(), testWareHouse.getPath());
    this.table =
        catalog.createTable(
            TableIdentifier.of("logging", "logs"),
            SCHEMA,
            SPEC,
            Collections.singletonMap("write.format.default", testFileFormat));
  }

  // test write and java API write data code demo
  private void writeRecords() throws IOException {
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec());
    int partitionId = 1, taskId = 1;
    FileFormat fileFormat =
        FileFormat.valueOf(
            table.properties().getOrDefault("write.format.default", "parquet").toUpperCase());
    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(table, partitionId, taskId).format(fileFormat).build();
    // TaskWriter write records into file. (the same is ok for unpartition table)
    long targetFileSizeInBytes = 50L * 1024 * 1024;
    GenericTaskWriter<Record> genericTaskWriter =
        new GenericTaskWriter(
            table.spec(),
            fileFormat,
            appenderFactory,
            outputFileFactory,
            table.io(),
            targetFileSizeInBytes);

    GenericRecord genericRecord = GenericRecord.create(table.schema());
    // assume write 1000 records
    for (int i = 0; i < 1000; i++) {
      GenericRecord record = genericRecord.copy();
      record.setField("level", i % 6 == 0 ? "error" : "info");
      record.setField("event_time", OffsetDateTime.now());
      record.setField("message", "Iceberg is a great table format");
      record.setField("call_stack", Collections.singletonList("NullPointerException"));
      genericTaskWriter.write(record);
      // just for test, remove from doc code
      this.testRecords.add(record);
    }
    // after the data file is written above,
    // the written data file is submitted to the metadata of the table through Table API.
    AppendFiles appendFiles = table.newAppend();
    for (DataFile dataFile : genericTaskWriter.dataFiles()) {
      appendFiles.appendFile(dataFile);
    }
    // submit data file.
    appendFiles.commit();
  }

  @Test
  public void writeTableDataCheck() throws IOException {
    writeRecords();
    IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table);
    InternalRecordWrapper recordWrapper = new InternalRecordWrapper(table.schema().asStruct());

    ArrayList<Record> actualRecords = Lists.newArrayList(scanBuilder.build().iterator());
    StructLikeSet actualSet = StructLikeSet.create(table.schema().asStruct());
    actualRecords.forEach(record -> actualSet.add(recordWrapper.wrap(record)));

    StructLikeSet expectedSet = StructLikeSet.create(table.schema().asStruct());
    testRecords.forEach(record -> expectedSet.add(recordWrapper.wrap(record)));
    Assert.assertEquals("should have same records", expectedSet, actualSet);
  }
}
