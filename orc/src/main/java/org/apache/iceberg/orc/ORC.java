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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
    private BiFunction<Schema, TypeDescription, OrcRowWriter<?>> createWriterFunc;
    private Map<String, byte[]> metadata = new HashMap<>();
    private MetricsConfig metricsConfig;

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

    public WriteBuilder createWriterFunc(BiFunction<Schema, TypeDescription, OrcRowWriter<?>> writerFunction) {
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

    public WriteBuilder metricsConfig(MetricsConfig newMetricsConfig) {
      this.metricsConfig = newMetricsConfig;
      return this;
    }

    public <D> FileAppender<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      return new OrcFileAppender<>(schema,
          this.file, createWriterFunc, conf, metadata,
          conf.getInt(VECTOR_ROW_BATCH_SIZE, VectorizedRowBatch.DEFAULT_SIZE), metricsConfig);
    }
  }

  public static DataWriteBuilder writeData(OutputFile file) {
    return new DataWriteBuilder(file);
  }

  public static class DataWriteBuilder {
    private final WriteBuilder appenderBuilder;
    private final String location;
    private PartitionSpec spec = null;
    private StructLike partition = null;
    private EncryptionKeyMetadata keyMetadata = null;
    private SortOrder sortOrder = null;

    private DataWriteBuilder(OutputFile file) {
      this.appenderBuilder = write(file);
      this.location = file.location();
    }

    public DataWriteBuilder forTable(Table table) {
      schema(table.schema());
      withSpec(table.spec());
      setAll(table.properties());
      metricsConfig(MetricsConfig.fromProperties(table.properties()));
      return this;
    }

    public DataWriteBuilder schema(Schema newSchema) {
      appenderBuilder.schema(newSchema);
      return this;
    }

    public DataWriteBuilder set(String property, String value) {
      appenderBuilder.config(property, value);
      return this;
    }

    public DataWriteBuilder setAll(Map<String, String> properties) {
      appenderBuilder.setAll(properties);
      return this;
    }

    public DataWriteBuilder meta(String property, String value) {
      appenderBuilder.metadata(property, value);
      return this;
    }

    public DataWriteBuilder overwrite() {
      return overwrite(true);
    }

    public DataWriteBuilder overwrite(boolean enabled) {
      appenderBuilder.overwrite(enabled);
      return this;
    }

    public DataWriteBuilder metricsConfig(MetricsConfig newMetricsConfig) {
      appenderBuilder.metricsConfig(newMetricsConfig);
      return this;
    }

    public DataWriteBuilder createWriterFunc(BiFunction<Schema, TypeDescription, OrcRowWriter<?>> writerFunction) {
      appenderBuilder.createWriterFunc(writerFunction);
      return this;
    }

    public DataWriteBuilder withSpec(PartitionSpec newSpec) {
      this.spec = newSpec;
      return this;
    }

    public DataWriteBuilder withPartition(StructLike newPartition) {
      this.partition = newPartition;
      return this;
    }

    public DataWriteBuilder withKeyMetadata(EncryptionKeyMetadata metadata) {
      this.keyMetadata = metadata;
      return this;
    }

    public DataWriteBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder;
      return this;
    }

    public <T> DataWriter<T> build() {
      Preconditions.checkArgument(spec != null, "Cannot create data writer without spec");
      Preconditions.checkArgument(spec.isUnpartitioned() || partition != null,
          "Partition must not be null when creating data writer for partitioned spec");

      FileAppender<T> fileAppender = appenderBuilder.build();
      return new DataWriter<>(fileAppender, FileFormat.ORC, location, spec, partition, keyMetadata, sortOrder);
    }
  }

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  public static class ReadBuilder {
    private final InputFile file;
    private final Configuration conf;
    private Schema schema = null;
    private Long start = null;
    private Long length = null;
    private Expression filter = null;
    private boolean caseSensitive = true;
    private NameMapping nameMapping = null;

    private Function<TypeDescription, OrcRowReader<?>> readerFunc;
    private Function<TypeDescription, OrcBatchReader<?>> batchedReaderFunc;
    private int recordsPerBatch = VectorizedRowBatch.DEFAULT_SIZE;

    private ReadBuilder(InputFile file) {
      Preconditions.checkNotNull(file, "Input file cannot be null");
      this.file = file;
      if (file instanceof HadoopInputFile) {
        this.conf = new Configuration(((HadoopInputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }

      // We need to turn positional schema evolution off since we use column name based schema evolution for projection
      this.conf.setBoolean(OrcConf.FORCE_POSITIONAL_EVOLUTION.getHiveConfName(), false);
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

    public ReadBuilder project(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public ReadBuilder caseSensitive(boolean newCaseSensitive) {
      OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(this.conf, newCaseSensitive);
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    public ReadBuilder config(String property, String value) {
      conf.set(property, value);
      return this;
    }

    public ReadBuilder createReaderFunc(Function<TypeDescription, OrcRowReader<?>> readerFunction) {
      Preconditions.checkArgument(this.batchedReaderFunc == null,
          "Reader function cannot be set since the batched version is already set");
      this.readerFunc = readerFunction;
      return this;
    }

    public ReadBuilder filter(Expression newFilter) {
      this.filter = newFilter;
      return this;
    }

    public ReadBuilder createBatchedReaderFunc(Function<TypeDescription, OrcBatchReader<?>> batchReaderFunction) {
      Preconditions.checkArgument(this.readerFunc == null,
          "Batched reader function cannot be set since the non-batched version is already set");
      this.batchedReaderFunc = batchReaderFunction;
      return this;
    }

    public ReadBuilder recordsPerBatch(int numRecordsPerBatch) {
      this.recordsPerBatch = numRecordsPerBatch;
      return this;
    }

    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    public <D> CloseableIterable<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      return new OrcIterable<>(file, conf, schema, nameMapping, start, length, readerFunc, caseSensitive, filter,
          batchedReaderFunc, recordsPerBatch);
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
    ReaderOptions readerOptions = OrcFile.readerOptions(config).useUTCTimestamp(true);
    if (file instanceof HadoopInputFile) {
      readerOptions.filesystem(((HadoopInputFile) file).getFileSystem());
    }
    return newFileReader(file.location(), readerOptions);
  }
}
