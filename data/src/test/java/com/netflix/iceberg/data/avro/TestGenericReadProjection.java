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

package com.netflix.iceberg.data.avro;

import com.google.common.collect.Iterables;
import com.netflix.iceberg.Files;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.data.Record;
import com.netflix.iceberg.data.TestReadProjection;
import com.netflix.iceberg.io.FileAppender;
import java.io.File;
import java.io.IOException;

public class TestGenericReadProjection extends TestReadProjection {
  protected Record writeAndRead(String desc, Schema writeSchema, Schema readSchema, Record record)
      throws IOException {
    File file = temp.newFile(desc + ".avro");
    file.delete();

    try (FileAppender<Record> appender = Avro.write(Files.localOutput(file))
        .schema(writeSchema)
        .createWriterFunc(DataWriter::create)
        .build()) {
      appender.add(record);
    }

    Iterable<Record> records = Avro.read(Files.localInput(file))
        .project(readSchema)
        .createReaderFunc(DataReader::create)
        .build();

    return Iterables.getOnlyElement(records);
  }
}
