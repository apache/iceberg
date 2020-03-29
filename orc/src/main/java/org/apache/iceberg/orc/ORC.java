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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.ReaderOptions;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class ORC {

  private static final String VECTOR_ROW_BATCH_SIZE = "iceberg.orc.vectorbatch.size";

  private ORC() {
  }

  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  public static class WriteBuilder {
    private final OutputFile file;
    private final Configuration conf;
    private Schema schema = null;
    private Function<TypeDescription, OrcValueWriter<?>>  createWriterFunc;
    private Map<String, byte[]> metadata = new HashMap<>();

    private WriteBuilder(OutputFile file) {
      this.file = file;
      if (file instanceof HadoopOutputFile) {
        this.conf = new Configuration(((HadoopOutputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }
    }

    public WriteBuilder metadata(String property, String value) {
      metadata.put(property, value.getBytes(StandardCharsets.UTF_8));
      return this;
    }

    public WriteBuilder config(String property, String value) {
      conf.set(property, value);
      return this;
    }

    public WriteBuilder createWriterFunc(Function<TypeDescription, OrcValueWriter<?>> writerFunction) {
      this.createWriterFunc = writerFunction;
      return this;
    }

    public WriteBuilder setAll(Map<String, String> properties) {
      properties.forEach(conf::set);
      return this;
    }

    public WriteBuilder schema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public WriteBuilder overwrite() {
      return overwrite(true);
    }

    public WriteBuilder overwrite(boolean enabled) {
      OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(conf, enabled);
      return this;
    }

    public <D> FileAppender<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      return new OrcFileAppender<>(schema,
          this.file, createWriterFunc, conf, metadata,
          conf.getInt(VECTOR_ROW_BATCH_SIZE, VectorizedRowBatch.DEFAULT_SIZE));
    }
  }

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  public static class ReadBuilder {
    private final InputFile file;
    private final Configuration conf;
    private org.apache.iceberg.Schema schema = null;
    private Long start = null;
    private Long length = null;

    private Function<TypeDescription, OrcValueReader<?>> readerFunc;

    private ReadBuilder(InputFile file) {
      Preconditions.checkNotNull(file, "Input file cannot be null");
      this.file = file;
      if (file instanceof HadoopInputFile) {
        this.conf = new Configuration(((HadoopInputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }
    }

    /**
     * Restricts the read to the given range: [start, start + length).
     *
     * @param newStart the start position for this read
     * @param newLength the length of the range this read should scan
     * @return this builder for method chaining
     */
    public ReadBuilder split(long newStart, long newLength) {
      this.start = newStart;
      this.length = newLength;
      return this;
    }

    public ReadBuilder schema(org.apache.iceberg.Schema projectSchema) {
      this.schema = projectSchema;
      return this;
    }

    public ReadBuilder caseSensitive(boolean caseSensitive) {
      OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(this.conf, caseSensitive);
      return this;
    }

    public ReadBuilder config(String property, String value) {
      conf.set(property, value);
      return this;
    }

    public ReadBuilder createReaderFunc(Function<TypeDescription, OrcValueReader<?>> readerFunction) {
      this.readerFunc = readerFunction;
      return this;
    }

    public <D> CloseableIterable<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      return new OrcIterable<>(file, conf, schema, start, length, readerFunc);
    }
  }

  static Reader newFileReader(String location, ReaderOptions readerOptions) {
    try {
      return OrcFile.createReader(new Path(location), readerOptions);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to open file: %s", location);
    }
  }

  static Reader newFileReader(InputFile file, Configuration config) {
    ReaderOptions readerOptions = OrcFile.readerOptions(config);
    if (file instanceof HadoopInputFile) {
      readerOptions.filesystem(((HadoopInputFile) file).getFileSystem());
    }
    return newFileReader(file.location(), readerOptions);
  }
}
