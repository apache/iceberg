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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;

public class TestVariantWriterConcurrency {
  private static final int WRITERS = 2;
  private static final int VALUES = 20_000;
  private static final VariantMetadata METADATA = Variants.emptyMetadata();

  @Test
  public void testConcurrentWritersDoNotShareBuffers() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(WRITERS);
    try {
      CyclicBarrier start = new CyclicBarrier(WRITERS);
      List<Future<byte[]>> results = Lists.newArrayList();
      for (int writer = 0; writer < WRITERS; writer += 1) {
        int id = writer;
        results.add(executor.submit(() -> write(id, start)));
      }

      for (int writer = 0; writer < WRITERS; writer += 1) {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(results.get(writer).get(), null);
        ValueReader<Variant> reader = ValueReaders.variants();
        for (int row = 0; row < VALUES; row += 1) {
          Variant variant = reader.read(decoder, null);
          assertThat(variant.value().asPrimitive().get()).isEqualTo(value(writer, row));
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static byte[] write(int writer, CyclicBarrier start) throws Exception {
    // each call builds its own writer, as each Avro file writer does
    ValueWriter<Variant> variantWriter = ValueWriters.variants();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    start.await();
    for (int row = 0; row < VALUES; row += 1) {
      try {
        variantWriter.write(Variant.of(METADATA, Variants.of(value(writer, row))), encoder);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    encoder.flush();
    return out.toByteArray();
  }

  private static String value(int writer, int row) {
    return String.format("writer-%d-row-%08d", writer, row);
  }
}
