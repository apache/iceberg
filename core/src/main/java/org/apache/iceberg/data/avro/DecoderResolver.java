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
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.iceberg.avro.ValueReader;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.MapMaker;

/**
 * Resolver to resolve {@link Decoder} to a {@link ResolvingDecoder}. This class uses a {@link
 * ThreadLocal} for caching {@link ResolvingDecoder}.
 */
public class DecoderResolver {

  @VisibleForTesting
  static final ThreadLocal<Map<Schema, Map<Schema, ResolvingDecoder>>> DECODER_CACHES =
      ThreadLocal.withInitial(() -> new MapMaker().weakKeys().makeMap());

  private DecoderResolver() {}

  public static <T> T resolveAndRead(
      Decoder decoder, Schema readSchema, Schema fileSchema, ValueReader<T> reader, T reuse)
      throws IOException {
    ResolvingDecoder resolver = DecoderResolver.resolve(decoder, readSchema, fileSchema);
    T value = reader.read(resolver, reuse);
    resolver.drain();
    return value;
  }

  @VisibleForTesting
  static ResolvingDecoder resolve(Decoder decoder, Schema readSchema, Schema fileSchema)
      throws IOException {
    Map<Schema, Map<Schema, ResolvingDecoder>> cache = DECODER_CACHES.get();
    Map<Schema, ResolvingDecoder> fileSchemaToResolver =
        cache.computeIfAbsent(readSchema, k -> new WeakHashMap<>());

    ResolvingDecoder resolver =
        fileSchemaToResolver.computeIfAbsent(fileSchema, schema -> newResolver(readSchema, schema));

    resolver.configure(decoder);

    return resolver;
  }

  private static ResolvingDecoder newResolver(Schema readSchema, Schema fileSchema) {
    try {
      return DecoderFactory.get().resolvingDecoder(fileSchema, readSchema, null);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
