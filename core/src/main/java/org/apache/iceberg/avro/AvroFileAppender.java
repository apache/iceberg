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

package org.apache.iceberg.avro;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

class AvroFileAppender<D> implements FileAppender<D> {
  private PositionOutputStream stream = null;
  private DataFileWriter<D> writer = null;
  private long numRecords = 0L;

  AvroFileAppender(Schema schema, OutputFile file,
                   Function<Schema, DatumWriter<?>> createWriterFunc,
                   CodecFactory codec, Map<String, String> metadata,
                   boolean overwrite) throws IOException {
    this.stream = overwrite ? file.createOrOverwrite() : file.create();
    this.writer = newAvroWriter(schema, stream, createWriterFunc, codec, metadata);
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
  public long length() {
    if (stream != null) {
      try {
        return stream.getPos();
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to get stream length");
      }
    }
    throw new RuntimeIOException("Failed to get stream length: no open stream");
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
      Schema schema, PositionOutputStream stream, Function<Schema, DatumWriter<?>> createWriterFunc,
      CodecFactory codec, Map<String, String> metadata) throws IOException {
    DataFileWriter<D> writer = new DataFileWriter<>(
        (DatumWriter<D>) createWriterFunc.apply(schema));

    writer.setCodec(codec);

    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      writer.setMeta(entry.getKey(), entry.getValue());
    }

    return writer.create(schema, stream);
  }
}
