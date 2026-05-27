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
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.metadata.ParquetMetadataUtil;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;

final class ParquetFileReaderFactory {

  private ParquetFileReaderFactory() {}

  static ParquetFileReaderWrapper open(InputFile file, ParquetReadOptions options)
      throws IOException {
    String client =
        System.getProperty(Parquet.PARQUET_CLIENT, Parquet.PARQUET_CLIENT_DEFAULT.name());
    Parquet.ParquetClient parquetClient =
        Parquet.ParquetClient.valueOf(client.toUpperCase(Locale.US));

    switch (parquetClient) {
      case NATIVE -> {
        NativeParquetFileReader nativeReader = NativeParquetFileReader.open(file, options);
        return new NativeParquetFileReaderWrapper(nativeReader);
      }
      case HADOOP -> {
        ParquetFileReader hadoopReader = ParquetFileReader.open(ParquetIO.file(file), options);
        return new HadoopParquetFileReaderWrapper(hadoopReader);
      }
      default -> throw new IllegalArgumentException("Unsupported Parquet client: " + parquetClient);
    }
  }

  interface ParquetFileReaderWrapper extends AutoCloseable {
    org.apache.iceberg.parquet.metadata.ParquetMetadata footer();
  }

  private static class NativeParquetFileReaderWrapper implements ParquetFileReaderWrapper {
    private final NativeParquetFileReader reader;

    NativeParquetFileReaderWrapper(NativeParquetFileReader reader) {
      this.reader = reader;
    }

    @Override
    public org.apache.iceberg.parquet.metadata.ParquetMetadata footer() {
      return reader.footer();
    }

    @Override
    public void close() throws Exception {
      reader.close();
    }
  }

  private static class HadoopParquetFileReaderWrapper implements ParquetFileReaderWrapper {
    private final ParquetFileReader reader;

    HadoopParquetFileReaderWrapper(ParquetFileReader reader) {
      this.reader = reader;
    }

    @Override
    public org.apache.iceberg.parquet.metadata.ParquetMetadata footer() {
      return ParquetMetadataUtil.fromHadoopMetadata(reader.getFooter());
    }

    @Override
    public void close() throws Exception {
      reader.close();
    }
  }
}
