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
package org.apache.iceberg.orc;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

public class TestOrcIterableResourceCleanup {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File temp;

  @Test
  public void testClosingIterableClosesAllStreams() throws IOException {
    List<SeekableInputStream> inputStreams = Lists.newArrayList();
    InputFile inputFile = spyOnStreams(writeTestOrcFile(), inputStreams);

    try (CloseableIterable<Record> iterable = newOrcIterable(inputFile)) {
      try (CloseableIterator<Record> iterator = iterable.iterator()) {
        drain(iterator);
      }
    }

    verifyAllStreamsClosed(inputStreams);
  }

  @Test
  public void testClosingIterableClosesIteratorResources() throws IOException {
    List<SeekableInputStream> inputStreams = Lists.newArrayList();
    InputFile inputFile = spyOnStreams(writeTestOrcFile(), inputStreams);

    // Without addCloseable(rowBatchIterator) in OrcIterable, the VectorizedRowBatchIterator
    // and its RecordReader are never closed, leaking ORC input streams / file handles.
    for (int round = 0; round < 5; round++) {
      try (CloseableIterable<Record> iterable = newOrcIterable(inputFile)) {
        drain(iterable.iterator());
      }
    }

    verifyAllStreamsClosed(inputStreams);
  }

  private static <T> void drain(CloseableIterator<T> iterator) {
    while (iterator.hasNext()) {
      iterator.next();
    }
  }

  private InputFile writeTestOrcFile() throws IOException {
    OutputFile outputFile = Files.localOutput(File.createTempFile("test", ".orc", temp));
    try (DataWriter<Record> writer =
        ORC.writeData(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      GenericRecord record = GenericRecord.create(SCHEMA);
      for (int i = 0; i < 10; i++) {
        writer.write(record.copy(ImmutableMap.of("id", (long) i, "data", "val" + i)));
      }
    }

    return outputFile.toInputFile();
  }

  private static CloseableIterable<Record> newOrcIterable(InputFile input) {
    return ORC.read(input)
        .project(SCHEMA)
        .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
        .build();
  }

  private static void verifyAllStreamsClosed(List<SeekableInputStream> streams) throws IOException {
    for (SeekableInputStream stream : streams) {
      Mockito.verify(stream, Mockito.times(1)).close();
    }
  }

  private static InputFile spyOnStreams(InputFile delegate, List<SeekableInputStream> streams) {
    InputFile inputFile = Mockito.spy(delegate);
    Mockito.doAnswer(
            invocation -> {
              SeekableInputStream real = (SeekableInputStream) invocation.callRealMethod();
              SeekableInputStream inputStream = Mockito.spy(real);
              streams.add(inputStream);
              return inputStream;
            })
        .when(inputFile)
        .newStream();
    return inputFile;
  }
}
