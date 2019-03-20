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
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ORC {
  private ORC() {
  }

  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  public static class WriteBuilder {
    private final OutputFile file;
    private final Configuration conf;
    private Schema schema = null;
    private Map<String, byte[]> metadata = new HashMap<>();

    private WriteBuilder(OutputFile file) {
      this.file = file;
      if (file instanceof HadoopOutputFile) {
        conf = new Configuration(((HadoopOutputFile) file).getConf());
      } else {
        conf = new Configuration();
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

    public WriteBuilder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public OrcFileAppender build() {
      OrcFile.WriterOptions options =
          OrcFile.writerOptions(conf);
      return new OrcFileAppender(schema, file, options, metadata);
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

    private ReadBuilder(InputFile file) {
      Preconditions.checkNotNull(file, "Input file cannot be null");
      this.file = file;
      if (file instanceof HadoopInputFile) {
        conf = new Configuration(((HadoopInputFile) file).getConf());
      } else {
        conf = new Configuration();
      }
    }

    /**
     * Restricts the read to the given range: [start, start + length).
     *
     * @param start the start position for this read
     * @param length the length of the range this read should scan
     * @return this builder for method chaining
     */
    public ReadBuilder split(long start, long length) {
      this.start = start;
      this.length = length;
      return this;
    }

    public ReadBuilder schema(org.apache.iceberg.Schema schema) {
      this.schema = schema;
      return this;
    }

    public ReadBuilder config(String property, String value) {
      conf.set(property, value);
      return this;
    }

    public OrcIterator build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      try {
        Path path = new Path(file.location());
        Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
        ColumnIdMap columnIds = new ColumnIdMap();
        TypeDescription orcSchema = TypeConversion.toOrc(schema, columnIds);
        Reader.Options options = reader.options();
        if (start != null) {
          options.range(start, length);
        }
        options.schema(orcSchema);
        return new OrcIterator(path, orcSchema, reader.rows(options));
      } catch (IOException e) {
        throw new RuntimeException("Can't open " + file.location(), e);
      }
    }
  }
}
