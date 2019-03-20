/*
 * Copyright 2018 Hortonworks
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
package org.apache.iceberg.orc;

import com.google.common.base.Preconditions;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Create a file appender for ORC.
 */
public class OrcFileAppender implements FileAppender<VectorizedRowBatch> {
  private final Writer writer;
  private final TypeDescription orcSchema;
  private final ColumnIdMap columnIds = new ColumnIdMap();
  private final Path path;
  private boolean isClosed = false;

  public static final String COLUMN_NUMBERS_ATTRIBUTE = "iceberg.column.ids";

  OrcFileAppender(Schema schema,
                  OutputFile file,
                  OrcFile.WriterOptions options,
                  Map<String,byte[]> metadata) {
    orcSchema = TypeConversion.toOrc(schema, columnIds);
    options.setSchema(orcSchema);
    path = new Path(file.location());
    try {
      writer = OrcFile.createWriter(path, options);
    } catch (IOException e) {
      throw new RuntimeException("Can't create file " + path, e);
    }
    writer.addUserMetadata(COLUMN_NUMBERS_ATTRIBUTE, columnIds.serialize());
    metadata.forEach(
        (key,value) -> writer.addUserMetadata(key, ByteBuffer.wrap(value)));
  }

  @Override
  public void add(VectorizedRowBatch datum) {
    try {
      writer.addRowBatch(datum);
    } catch (IOException e) {
      throw new RuntimeException("Problem writing to ORC file " + path, e);
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
      Integer[] icebergIds = new Integer[orcSchema.getMaximumId() + 1];
      for(TypeDescription type: columnIds.keySet()) {
        icebergIds[type.getId()] = columnIds.get(type);
      }
      for(int c=1; c < stats.length; ++c) {
        if (icebergIds[c] != null) {
          valueCounts.put(icebergIds[c], stats[c].getNumberOfValues());
        }
      }
      for(TypeDescription child: orcSchema.getChildren()) {
        int c = child.getId();
        if (icebergIds[c] != null) {
          nullCounts.put(icebergIds[c], rows - stats[c].getNumberOfValues());
        }
      }
      return new Metrics(rows, null, valueCounts, nullCounts);
    } catch (IOException e) {
      throw new RuntimeException("Can't get statistics " + path, e);
    }
  }

  @Override
  public long length() {
    Preconditions.checkState(isClosed,
        "Cannot return length while appending to an open file.");
    return writer.getRawDataSize();
  }

  @Override
  public void close() throws IOException {
    if (!isClosed) {
      this.isClosed = true;
      writer.close();
    }
  }

  public TypeDescription getSchema() {
    return orcSchema;
  }
}
