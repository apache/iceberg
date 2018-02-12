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

package com.netflix.iceberg.parquet;

import com.google.common.base.Preconditions;
import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.FileAppender;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import java.io.IOException;

public class ParquetWriteAdapter<D> implements FileAppender<D> {
  private ParquetWriter<D> writer = null;
  private ParquetMetadata footer = null;

  public ParquetWriteAdapter(ParquetWriter<D> writer) throws IOException {
    this.writer = writer;
  }

  @Override
  public void add(D datum) {
    try {
      writer.write(datum);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write record %s", datum);
    }
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkArgument(footer != null, "Cannot produce metrics until closed");
    return ParquetMetrics.fromMetadata(footer);
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
      this.footer = writer.getFooter();
      this.writer = null;
    }
  }
}
