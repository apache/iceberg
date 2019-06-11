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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

/**
 * Create a file appender for ORC.
 */
class OrcFileAppender<D> implements FileAppender<D> {
  private final int batchSize;
  private final Schema schema;
  private final ColumnMap columnIds;
  private final OutputFile file;
  private final Writer writer;
  private final VectorizedRowBatch batch;
  private final OrcValueWriter<D> valueWriter;
  private boolean isClosed = false;
  private final Configuration conf;

  OrcFileAppender(Schema schema, OutputFile file,
                  Function<TypeDescription, OrcValueWriter<?>> createWriterFunc,
                  Configuration conf, Map<String, byte[]> metadata,
                  int batchSize) {
    this.conf = conf;
    this.file = file;
    this.batchSize = batchSize;
    this.schema = schema;
    this.columnIds = new ColumnMap();
    TypeDescription orcSchema = TypeConversion.toOrc(this.schema, this.columnIds);
    batch = orcSchema.createRowBatch(this.batchSize);

    OrcFile.WriterOptions options = OrcFile.writerOptions(conf);
    options.setSchema(orcSchema);
    writer = newOrcWriter(file, columnIds, options, metadata);
    valueWriter = newOrcValueWriter(orcSchema, createWriterFunc);
  }

  @Override
  public void add(D datum) {
    try {
      valueWriter.write(datum, batch);
      if (batch.size == this.batchSize) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    } catch (IOException e) {
      throw new RuntimeException("Problem writing to ORC file " + file.location(), e);
    }
  }

  @Override
  public Metrics metrics() {
    try {
      long rows = writer.getNumberOfRows();
      ColumnStatistics[] stats = writer.getStatistics();
      // we don't currently have columnSizes or distinct counts.
      Map<Integer, Long> valueCounts = new HashMap<>();
      Map<Integer, Long> nullCounts = new HashMap<>();
      Map<ColumnMap.IcebergColumn, TypeDescription> icebertToOrc = columnIds.inverse();
      for (Types.NestedField field : schema.columns()) {
        TypeDescription orcCol = icebertToOrc.get(
            ColumnMap.newIcebergColumn(field.fieldId(), field.isRequired()));
        valueCounts.put(field.fieldId(), stats[orcCol.getId()].getNumberOfValues());
        nullCounts.put(field.fieldId(), rows - stats[orcCol.getId()].getNumberOfValues());
      }
      return new Metrics(rows, null, valueCounts, nullCounts);
    } catch (IOException e) {
      throw new RuntimeException("Can't get statistics for " + file.location(), e);
    }
  }

  @Override
  public long length() {
    Preconditions.checkState(isClosed,
        "Cannot return length while appending to an open file.");
    return writer.getRawDataSize();
  }

  @Override
  public List<Long> splitOffsets() {
    Preconditions.checkState(isClosed, "File is not yet closed");
    Reader reader;
    try {
      reader = OrcFile.createReader(new Path(file.location()), new OrcFile.ReaderOptions(conf));
    } catch (IOException e) {
      throw new RuntimeIOException("Cannot read file " + file.location(), e);
    }
    List<StripeInformation> stripes = reader.getStripes();
    return Collections.unmodifiableList(Lists.transform(stripes, StripeInformation::getOffset));
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

  private static Writer newOrcWriter(OutputFile file,
                                     ColumnMap columnIds,
                                     OrcFile.WriterOptions options, Map<String, byte[]> metadata) {
    final Path locPath = new Path(file.location());
    final Writer writer;

    try {
      writer = OrcFile.createWriter(locPath, options);
    } catch (IOException e) {
      throw new RuntimeException("Can't create file " + locPath, e);
    }

    // Store column mapping Iceberg -> ORC in file metadata
    writer.addUserMetadata(ORC.COLUMN_NUMBERS_ATTRIBUTE, columnIds.serialize());
    metadata.forEach((key, value) -> writer.addUserMetadata(key, ByteBuffer.wrap(value)));

    return writer;
  }

  @SuppressWarnings("unchecked")
  private static <D> OrcValueWriter<D> newOrcValueWriter(
      TypeDescription schema, Function<TypeDescription, OrcValueWriter<?>> createWriterFunc) {
    return (OrcValueWriter<D>) createWriterFunc.apply(schema);
  }
}
