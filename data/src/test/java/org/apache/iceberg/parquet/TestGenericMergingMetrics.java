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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.data.FileHelpers.encrypt;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TestMergingMetrics;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;

public class TestGenericMergingMetrics extends TestMergingMetrics<Record> {

  @Override
  protected DataFile writeAndGetDataFile(List<Record> records) throws IOException {
    DataWriter<Record> writer =
        new GenericFileWriterFactory.Builder()
            .dataSchema(SCHEMA)
            .dataFileFormat(fileFormat)
            .build()
            .newDataWriter(
                encrypt(Files.localOutput(new File(tempDir, "junit" + System.nanoTime()))),
                PartitionSpec.unpartitioned(),
                null);
    try (writer) {
      writer.write(records);
    }

    return writer.toDataFile();
  }
}
