/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.data.parquet;

import com.google.common.collect.Iterables;
import com.netflix.iceberg.Files;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.data.Record;
import com.netflix.iceberg.data.TestReadProjection;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.parquet.Parquet;
import java.io.File;
import java.io.IOException;

public class TestGenericReadProjection extends TestReadProjection {
  protected Record writeAndRead(String desc, Schema writeSchema, Schema readSchema, Record record)
      throws IOException {
    File file = temp.newFile(desc + ".parquet");
    file.delete();

    try (FileAppender<Record> appender = Parquet.write(Files.localOutput(file))
        .schema(writeSchema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build()) {
      appender.add(record);
    }

    Iterable<Record> records = Parquet.read(Files.localInput(file))
        .project(readSchema)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema))
        .build();

    return Iterables.getOnlyElement(records);
  }
}
