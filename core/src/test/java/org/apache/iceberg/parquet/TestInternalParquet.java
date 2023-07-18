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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.InternalTestHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RandomInternalData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.AvroDataTest;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class TestInternalParquet extends AvroDataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    List<StructLike> expected = RandomInternalData.generate(schema, 100, 1376L);

    OutputFile outputFile = new InMemoryOutputFile();

    try (DataWriter<StructLike> dataWriter =
        Parquet.writeData(outputFile)
            .schema(schema)
            .createWriterFunc(InternalWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      for (StructLike record : expected) {
        dataWriter.write(record);
      }
    }

    List<StructLike> rows;
    try (CloseableIterable<StructLike> reader =
        Parquet.read(outputFile.toInputFile())
            .project(schema)
            .createReaderFunc(fileSchema -> InternalReader.create(schema, fileSchema))
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      InternalTestHelpers.assertEquals(schema.asStruct(), expected.get(i), rows.get(i));
    }

    // test reuseContainers
    try (CloseableIterable<StructLike> reader =
        Parquet.read(outputFile.toInputFile())
            .project(schema)
            .reuseContainers()
            .createReaderFunc(fileSchema -> InternalReader.create(schema, fileSchema))
            .build()) {
      int index = 0;
      for (StructLike actualRecord : reader) {
        InternalTestHelpers.assertEquals(schema.asStruct(), expected.get(index), actualRecord);
        index += 1;
      }
    }
  }
}
