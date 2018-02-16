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

package com.netflix.iceberg.avro;

import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.io.OutputFile;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

class AvroFileAppender<D> implements FileAppender<D> {
  private DataFileWriter<D> writer = null;
  private long numRecords = 0L;

  AvroFileAppender(Schema schema, OutputFile file,
                   Function<Schema, DatumWriter<?>> createWriterFunc,
                   CodecFactory codec, Map<String, String> metadata) throws IOException {
    this.writer = newAvroWriter(schema, file, createWriterFunc, codec, metadata);
  }

  @Override
  public void add(D datum) {
    try {
      numRecords += 1L;
      writer.append(datum);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public Metrics metrics() {
    return new Metrics(numRecords, null, null, null);
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
      this.writer = null;
    }
  }

  @SuppressWarnings("unchecked")
  private static <D> DataFileWriter<D> newAvroWriter(
      Schema schema, OutputFile file, Function<Schema, DatumWriter<?>> createWriterFunc,
      CodecFactory codec, Map<String, String> metadata) throws IOException {
    DataFileWriter<D> writer = new DataFileWriter<>(
        (DatumWriter<D>) createWriterFunc.apply(schema));

    writer.setCodec(codec);

    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      writer.setMeta(entry.getKey(), entry.getValue());
    }

    // TODO: support overwrite
    return writer.create(schema, file.create());
  }
}
