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
package org.apache.iceberg.data.orc;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.TestReadProjection;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public class TestGenericReadProjection extends TestReadProjection {

  @Override
  protected Record writeAndRead(String desc, Schema writeSchema, Schema readSchema, Record record)
      throws IOException {

    File file = temp;
    file.delete();

    try (FileAppender<Record> appender =
        ORC.write(Files.localOutput(file))
            .schema(writeSchema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      appender.add(record);
    }

    Iterable<Record> records =
        ORC.read(Files.localInput(file))
            .project(readSchema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema))
            .build();

    return Iterables.getOnlyElement(records);
  }
}
