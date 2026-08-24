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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RandomInternalData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestParquetEagerRead {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  @Test
  public void testEagerRead() throws IOException {
    List<Record> expected = RandomInternalData.generate(SCHEMA, 10, 1376L);

    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (DataWriter<StructLike> writer =
        Parquet.writeData(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(InternalWriter::createWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      for (Record record : expected) {
        writer.write(record);
      }
    }

    // capture every stream the delegate opens so we can assert how the eager wrapper reads it
    InputFile delegate = Mockito.spy(outputFile.toInputFile());
    List<SeekableInputStream> openedStreams = Lists.newArrayList();
    Mockito.doAnswer(
            invocation -> {
              SeekableInputStream spy =
                  Mockito.spy((SeekableInputStream) invocation.callRealMethod());
              openedStreams.add(spy);
              return spy;
            })
        .when(delegate)
        .newStream();

    try (CloseableIterable<Record> reader =
        Parquet.read(delegate)
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> InternalReader.create(SCHEMA, fileSchema))
            .build()) {
      assertThat(reader).as("eager read should return all records").hasSameSizeAs(expected);
    }

    // a sub-threshold file is opened once and drained sequentially; positional reads hit the buffer
    assertThat(openedStreams).as("delegate should be opened exactly once").hasSize(1);
    SeekableInputStream delegateStream = openedStreams.get(0);
    Mockito.verify(delegateStream, Mockito.times(1))
        .read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
    Mockito.verify(delegateStream, Mockito.times(1)).close();
    Mockito.verifyNoMoreInteractions(delegateStream);
  }
}
