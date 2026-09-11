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
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.io.OutputFile;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

final class ParquetFileWriterFactory {

  private ParquetFileWriterFactory() {}

  static ParquetFileWriterWrapper create(
      OutputFile file,
      MessageType schema,
      ParquetFileWriter.Mode mode,
      long rowGroupSize,
      int maxPaddingSize,
      CompressionCodecName codec)
      throws IOException {
    String client =
        System.getProperty(Parquet.PARQUET_CLIENT, Parquet.PARQUET_CLIENT_DEFAULT.name());
    Parquet.ParquetClient parquetClient =
        Parquet.ParquetClient.valueOf(client.toUpperCase(Locale.US));

    switch (parquetClient) {
      case NATIVE -> {
        NativeParquetFileWriter nativeWriter = NativeParquetFileWriter.create(file, schema, codec);
        return new NativeParquetFileWriterWrapper(nativeWriter);
      }
      case HADOOP -> {
        ParquetFileWriter hadoopWriter =
            new ParquetFileWriter(ParquetIO.file(file), schema, mode, rowGroupSize, maxPaddingSize);
        return new HadoopParquetFileWriterWrapper(hadoopWriter);
      }
      default -> throw new IllegalArgumentException("Unsupported Parquet client: " + client);
    }
  }

  interface ParquetFileWriterWrapper extends AutoCloseable {
    void start() throws IOException;

    void appendFile(org.apache.parquet.io.InputFile file) throws IOException;

    void end(Map<String, String> metadata) throws IOException;
  }

  private static class NativeParquetFileWriterWrapper implements ParquetFileWriterWrapper {
    private final NativeParquetFileWriter writer;

    NativeParquetFileWriterWrapper(NativeParquetFileWriter writer) {
      this.writer = writer;
    }

    @Override
    public void start() throws IOException {
      // Native writer writes magic bytes in constructor, no separate start needed
    }

    @Override
    public void appendFile(org.apache.parquet.io.InputFile file) throws IOException {
      writer.appendFile(file);
    }

    @Override
    public void end(Map<String, String> metadata) throws IOException {
      writer.end(metadata);
    }

    @Override
    public void close() throws Exception {
      writer.close();
    }
  }

  private static class HadoopParquetFileWriterWrapper implements ParquetFileWriterWrapper {
    private final ParquetFileWriter writer;

    HadoopParquetFileWriterWrapper(ParquetFileWriter writer) {
      this.writer = writer;
    }

    @Override
    public void start() throws IOException {
      writer.start();
    }

    @Override
    public void appendFile(org.apache.parquet.io.InputFile file) throws IOException {
      writer.appendFile(file);
    }

    @Override
    public void end(Map<String, String> metadata) throws IOException {
      writer.end(metadata);
    }

    @Override
    public void close() throws Exception {
      writer.close();
    }
  }
}
