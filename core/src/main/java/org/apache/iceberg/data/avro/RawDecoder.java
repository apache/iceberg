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
package org.apache.iceberg.data.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.message.MessageDecoder;
import org.apache.iceberg.avro.ProjectionDatumReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class RawDecoder<D> extends MessageDecoder.BaseDecoder<D> {
  private static final ThreadLocal<BinaryDecoder> DECODER = new ThreadLocal<>();

  private final DatumReader<D> reader;

  /**
   * Creates a new {@link MessageDecoder} that constructs datum instances described by the {@link
   * Schema readSchema}.
   *
   * <p>The {@code readSchema} is used for the expected schema and the {@code writeSchema} is the
   * schema used to decode buffers. The {@code writeSchema} must be the schema that was used to
   * encode all buffers decoded by this class.
   */
  public RawDecoder(
      org.apache.iceberg.Schema readSchema,
      Function<Schema, DatumReader<?>> readerFunction,
      Schema writeSchema) {
    this.reader = new ProjectionDatumReader<>(readerFunction, readSchema, ImmutableMap.of(), null);
    this.reader.setSchema(writeSchema);
  }

  @Override
  public D decode(InputStream stream, D reuse) {
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(stream, DECODER.get());
    DECODER.set(decoder);
    try {
      return reader.read(reuse, decoder);
    } catch (IOException e) {
      throw new UncheckedIOException("Decoding datum failed", e);
    }
  }
}
