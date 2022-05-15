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
package org.apache.iceberg.orc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.orc.OrcFile;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create a file appender for ORC. */
class OrcFileAppender<D> implements FileAppender<D> {
  private static final Logger LOG = LoggerFactory.getLogger(OrcFileAppender.class);

  private final int batchSize;
  private final OutputFile file;
  private final Writer writer;
  private final VectorizedRowBatch batch;
  private final int avgRowByteSize;
  private final OrcRowWriter<D> valueWriter;
  private boolean isClosed = false;
  private final Configuration conf;
  private final MetricsConfig metricsConfig;

  OrcFileAppender(
      Schema schema,
      OutputFile file,
      BiFunction<Schema, TypeDescription, OrcRowWriter<?>> createWriterFunc,
      Configuration conf,
      Map<String, byte[]> metadata,
      int batchSize,
      MetricsConfig metricsConfig) {
    this.conf = conf;
    this.file = file;
    this.batchSize = batchSize;
    this.metricsConfig = metricsConfig;

    TypeDescription orcSchema = ORCSchemaUtil.convert(schema);

    this.avgRowByteSize =
        OrcSchemaVisitor.visitSchema(orcSchema, new EstimateOrcAvgWidthVisitor()).stream()
            .reduce(Integer::sum)
            .orElse(0);
    if (avgRowByteSize == 0) {
      LOG.warn("The average length of the rows appears to be zero.");
    }

    this.batch = orcSchema.createRowBatch(this.batchSize);

    OrcFile.WriterOptions options = OrcFile.writerOptions(conf).useUTCTimestamp(true);
    if (file instanceof HadoopOutputFile) {
      options.fileSystem(((HadoopOutputFile) file).getFileSystem());
    }
    options.setSchema(orcSchema);
    this.writer = ORC.newFileWriter(file, options, metadata);
    this.valueWriter = newOrcRowWriter(schema, orcSchema, createWriterFunc);
  }

  @Override
  public void add(D datum) {
    try {
      valueWriter.write(datum, batch);
      if (batch.size == this.batchSize) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    } catch (IOException ioe) {
      throw new UncheckedIOException(
          String.format("Problem writing to ORC file %s", file.location()), ioe);
    }
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(isClosed, "Cannot return metrics while appending to an open file.");
    return OrcMetrics.fromWriter(writer, valueWriter.metrics(), metricsConfig);
  }

  @Override
  public long length() {
    if (isClosed) {
      return file.toInputFile().getLength();
    }

    long estimateMemory = writer.estimateMemory();

    long dataLength = 0;
    try {
      List<StripeInformation> stripes = writer.getStripes();
      if (!stripes.isEmpty()) {
        StripeInformation stripeInformation = stripes.get(stripes.size() - 1);
        dataLength =
            stripeInformation != null
                ? stripeInformation.getOffset() + stripeInformation.getLength()
                : 0;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format(
              "Can't get Stripe's length from the file writer with path: %s.", file.location()),
          e);
    }

    // This value is estimated, not actual.
    return (long)
        Math.ceil(dataLength + (estimateMemory + (long) batch.size * avgRowByteSize) * 0.2);
  }

  @Override
  public List<Long> splitOffsets() {
    Preconditions.checkState(isClosed, "File is not yet closed");
    try {
      List<StripeInformation> stripes = writer.getStripes();
      return Collections.unmodifiableList(Lists.transform(stripes, StripeInformation::getOffset));
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to get stripe information from writer for: %s", file.location()),
          e);
    }
  }

  @Override
  public void close() throws IOException {
    if (!isClosed) {
      try {
        if (batch.size > 0) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      } finally {
        writer.close();
        this.isClosed = true;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <D> OrcRowWriter<D> newOrcRowWriter(
      Schema schema,
      TypeDescription orcSchema,
      BiFunction<Schema, TypeDescription, OrcRowWriter<?>> createWriterFunc) {
    return (OrcRowWriter<D>) createWriterFunc.apply(schema, orcSchema);
  }
}
