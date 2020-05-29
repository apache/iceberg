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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.schema.MessageType;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localOutput;

/**
 * Utilities for tests that need to write Parquet files.
 */
class ParquetWritingTestUtils {

  private ParquetWritingTestUtils() {}

  static File writeRecords(TemporaryFolder temp, Schema schema, GenericData.Record... records) throws IOException {
    return writeRecords(temp, schema, Collections.emptyMap(), null, records);
  }

  static File writeRecords(TemporaryFolder temp, Schema schema,
                           Map<String, String> properties, GenericData.Record... records) throws IOException {
    return writeRecords(temp, schema, properties, null, records);
  }

  static File writeRecords(
      TemporaryFolder temp,
      Schema schema, Map<String, String> properties,
      Function<MessageType, ParquetValueWriter<?>> createWriterFunc,
      GenericData.Record... records) throws IOException {
    File tmpFolder = temp.newFolder("parquet");
    String filename = UUID.randomUUID().toString();
    File file = new File(tmpFolder, FileFormat.PARQUET.addExtension(filename));
    try (FileAppender<GenericData.Record> writer = Parquet.write(localOutput(file))
        .schema(schema)
        .setAll(properties)
        .createWriterFunc(createWriterFunc)
        .build()) {
      writer.addAll(Lists.newArrayList(records));
    }
    return file;
  }
}
