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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.Files.localOutput;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.io.FileAppender;

public class TestAvroScan extends ScanTestBase {
  @Override
  protected boolean supportsVariant() {
    return true;
  }

  @Override
  protected void writeRecords(Table table, List<Record> records) throws IOException {
    File dataFolder = new File(table.location(), "data");
    dataFolder.mkdirs();

    File avroFile =
        new File(dataFolder, FileFormat.AVRO.addExtension(UUID.randomUUID().toString()));

    try (FileAppender<Record> writer =
        Avro.write(localOutput(avroFile))
            .schema(table.schema())
            .createWriterFunc(DataWriter::create)
            .build()) {
      writer.addAll(records);
    }

    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withFileSizeInBytes(avroFile.length())
            .withRecordCount(records.size())
            .withPath(avroFile.toString())
            .build();

    table.newAppend().appendFile(file).commit();
  }
}
