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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.IcebergDecoder;
import org.apache.iceberg.data.avro.IcebergEncoder;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;

class TestVariantWriterConcurrency {
  private static final int THREADS = 2;
  private static final int VALUES = 20_000;
  private static final long TIMEOUT_SECONDS = 60;
  private static final VariantMetadata METADATA = Variants.emptyMetadata();
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.VariantType.get()));

  @Test
  void concurrentWritersDoNotShareBuffers() throws Exception {
    // one writer used by all threads, as in a shared writer tree
    ValueWriter<Variant> variantWriter = ValueWriters.variants();
    List<byte[]> results = runConcurrently(thread -> () -> write(variantWriter, thread));

    for (int thread = 0; thread < THREADS; thread += 1) {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(results.get(thread), null);
      ValueReader<Variant> reader = ValueReaders.variants();
      for (int row = 0; row < VALUES; row += 1) {
        Variant variant = reader.read(decoder, null);
        assertThat(variant.value().asPrimitive().get()).isEqualTo(value(thread, row));
      }

      assertThat(decoder.isEnd()).isTrue();
    }
  }

  @Test
  void concurrentEncoderCallsDoNotShareBuffers() throws Exception {
    // IcebergEncoder builds one writer tree and can be used from several threads
    IcebergEncoder<Record> encoder = new IcebergEncoder<>(SCHEMA);
    List<List<ByteBuffer>> results = runConcurrently(thread -> () -> encode(encoder, thread));

    IcebergDecoder<Record> decoder = new IcebergDecoder<>(SCHEMA);
    for (int thread = 0; thread < THREADS; thread += 1) {
      List<ByteBuffer> messages = results.get(thread);
      assertThat(messages).hasSize(VALUES);
      for (int row = 0; row < VALUES; row += 1) {
        Record record = decoder.decode(messages.get(row));
        assertThat(record.getField("id")).isEqualTo(row);
        Variant variant = (Variant) record.getField("data");
        assertThat(variant.value().asPrimitive().get()).isEqualTo(value(thread, row));
      }
    }
  }

  private static <T> List<T> runConcurrently(IntFunction<Callable<T>> task) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    try {
      CyclicBarrier start = new CyclicBarrier(THREADS);
      List<Future<T>> futures = Lists.newArrayList();
      for (int thread = 0; thread < THREADS; thread += 1) {
        Callable<T> callable = task.apply(thread);
        futures.add(
            executor.submit(
                () -> {
                  start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                  return callable.call();
                }));
      }

      List<T> results = Lists.newArrayList();
      for (Future<T> future : futures) {
        results.add(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      }

      return results;
    } finally {
      executor.shutdownNow();
    }
  }

  private static byte[] write(ValueWriter<Variant> variantWriter, int thread) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    for (int row = 0; row < VALUES; row += 1) {
      variantWriter.write(Variant.of(METADATA, Variants.of(value(thread, row))), encoder);
    }

    encoder.flush();
    return out.toByteArray();
  }

  private static List<ByteBuffer> encode(IcebergEncoder<Record> encoder, int thread)
      throws Exception {
    List<ByteBuffer> messages = Lists.newArrayList();
    for (int row = 0; row < VALUES; row += 1) {
      Record record = GenericRecord.create(SCHEMA);
      record.setField("id", row);
      record.setField("data", Variant.of(METADATA, Variants.of(value(thread, row))));
      messages.add(encoder.encode(record));
    }

    return messages;
  }

  private static String value(int thread, int row) {
    return String.format("thread-%d-row-%08d", thread, row);
  }
}
