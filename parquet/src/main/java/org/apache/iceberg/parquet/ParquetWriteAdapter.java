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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class ParquetWriteAdapter<D> implements FileAppender<D> {
  private ParquetWriter<D> writer;
  private MetricsConfig metricsConfig;
  private ParquetMetadata footer;

  public ParquetWriteAdapter(ParquetWriter<D> writer, MetricsConfig metricsConfig) {
    this.writer = writer;
    this.metricsConfig = metricsConfig;
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
    Preconditions.checkState(footer != null, "Cannot produce metrics until closed");
    return ParquetUtil.footerMetrics(footer, metricsConfig);
  }

  @Override
  public long length() {
    return writer.getDataSize();
  }

  @Override
  public List<Long> splitOffsets() {
    return ParquetUtil.getSplitOffsets(writer.getFooter());
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
