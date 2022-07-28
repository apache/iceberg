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

import java.io.File;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;

public class GenericOrcReaderBenchmark extends ReaderBenchmark {
  @Override
  protected CloseableIterable<Record> reader(File file, Schema schema) {
    return ORC.read(Files.localInput(file))
        .project(schema)
        .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
        .build();
  }

  @Override
  protected FileAppender<Record> writer(File file, Schema schema) {
    return ORC.write(Files.localOutput(file))
        .schema(schema)
        .createWriterFunc(GenericOrcWriter::buildWriter)
        .build();
  }
}
