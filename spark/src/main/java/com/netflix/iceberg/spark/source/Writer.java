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

package com.netflix.iceberg.spark.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.iceberg.AppendFiles;
import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.DataFiles;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.hadoop.HadoopInputFile;
import com.netflix.iceberg.hadoop.HadoopOutputFile;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.orc.ORC;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.spark.data.SparkAvroWriter;
import com.netflix.iceberg.spark.data.SparkOrcWriter;
import com.netflix.iceberg.transforms.Transform;
import com.netflix.iceberg.transforms.Transforms;
import com.netflix.iceberg.types.Types.StringType;
import com.netflix.iceberg.util.Tasks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.netflix.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static com.netflix.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
import static com.netflix.iceberg.TableProperties.OBJECT_STORE_ENABLED;
import static com.netflix.iceberg.TableProperties.OBJECT_STORE_ENABLED_DEFAULT;
import static com.netflix.iceberg.TableProperties.OBJECT_STORE_PATH;
import static com.netflix.iceberg.spark.SparkSchemaUtil.convert;

// TODO: parameterize DataSourceWriter with subclass of WriterCommitMessage
class Writer implements DataSourceWriter, SupportsWriteInternalRow {
  private static final Transform<String, Integer> HASH_FUNC = Transforms
      .bucket(StringType.get(), Integer.MAX_VALUE);
  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

  private final Table table;
  private final Configuration conf;
  private final FileFormat format;

  Writer(Table table, Configuration conf, FileFormat format) {
    this.table = table;
    this.conf = conf;
    this.format = format;
  }

  @Override
  public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
    return new WriterFactory(table.spec(), format, dataLocation(), table.properties(), conf);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    AppendFiles append = table.newAppend();

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      append.appendFile(file);
    }

    LOG.info("Appending {} files to {}", numFiles, table);
    long start = System.currentTimeMillis();
    append.commit(); // abort is automatically called if this fails
    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    FileSystem fs;
    try {
      fs = new Path(table.location()).getFileSystem(conf);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }

    Tasks.foreach(files(messages))
        .retry(propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */ )
        .throwFailureWhenFinished()
        .run(file -> {
          try {
            fs.delete(new Path(file.path().toString()), false /* not recursive */ );
          } catch (IOException e) {
            throw new RuntimeIOException(e);
          }
        });
  }

  private Iterable<DataFile> files(WriterCommitMessage[] messages) {
    if (messages.length > 0) {
      return concat(transform(Arrays.asList(messages), message -> message != null
          ? ImmutableList.copyOf(((TaskCommit) message).files())
          : ImmutableList.of()));
    }
    return ImmutableList.of();
  }

  private int propertyAsInt(String property, int defaultValue) {
    Map<String, String> properties = table.properties();
    String value = properties.get(property);
    if (value != null) {
      return Integer.parseInt(properties.get(property));
    }
    return defaultValue;
  }

  private String dataLocation() {
    return new Path(new Path(table.location()), "data").toString();
  }

  @Override
  public String toString() {
    return String.format("IcebergWrite(table=%s, type=%s, format=%s)",
        table, table.schema().asStruct(), format);
  }


  private static class TaskCommit implements WriterCommitMessage {
    private final DataFile[] files;

    TaskCommit() {
      this.files = new DataFile[0];
    }

    TaskCommit(DataFile file) {
      this.files = new DataFile[] { file };
    }

    TaskCommit(List<DataFile> files) {
      this.files = files.toArray(new DataFile[files.size()]);
    }

    DataFile[] files() {
      return files;
    }
  }

  private static class WriterFactory implements DataWriterFactory<InternalRow> {
    private final PartitionSpec spec;
    private final FileFormat format;
    private final String dataLocation;
    private final Map<String, String> properties;
    private final SerializableConfiguration conf;
    private final String uuid = UUID.randomUUID().toString();

    private transient Path dataPath = null;

    WriterFactory(PartitionSpec spec, FileFormat format, String dataLocation,
                  Map<String, String> properties, Configuration conf) {
      this.spec = spec;
      this.format = format;
      this.dataLocation = dataLocation;
      this.properties = properties;
      this.conf = new SerializableConfiguration(conf);
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, int attemptNumber) {
      String filename = format.addExtension(String.format("%05d-%d-%s",
          partitionId, attemptNumber, uuid));
      AppenderFactory<InternalRow> factory = new SparkAppenderFactory();
      if (spec.fields().isEmpty()) {
        return new UnpartitionedWriter(lazyDataPath(), filename, format, conf.value(), factory);

      } else {
        Path baseDataPath = lazyDataPath(); // avoid calling this in the output path function
        Function<PartitionKey, Path> outputPathFunc = key ->
            new Path(new Path(baseDataPath, key.toPath()), filename);

        boolean useObjectStorage = (
            Boolean.parseBoolean(properties.get(OBJECT_STORE_ENABLED)) ||
            OBJECT_STORE_ENABLED_DEFAULT
        );

        if (useObjectStorage) {
          // try to get db and table portions of the path for context in the object store
          String context = pathContext(baseDataPath);
          String objectStore = properties.get(OBJECT_STORE_PATH);
          Preconditions.checkNotNull(objectStore,
              "Cannot use object storage, missing location: " + OBJECT_STORE_PATH);
          Path objectStorePath = new Path(objectStore);

          outputPathFunc = key -> {
            String partitionAndFilename = key.toPath() + "/" + filename;
            int hash = HASH_FUNC.apply(partitionAndFilename);
            return new Path(objectStorePath,
                String.format("%08x/%s/%s", hash, context, partitionAndFilename));
          };
        }

        return new PartitionedWriter(spec, format, conf.value(), factory, outputPathFunc);
      }
    }

    private static String pathContext(Path dataPath) {
      Path parent = dataPath.getParent();
      if (parent != null) {
        // remove the data folder
        if (dataPath.getName().equals("data")) {
          return pathContext(parent);
        }

        return parent.getName() + "/" + dataPath.getName();
      }

      return dataPath.getName();
    }

    private Path lazyDataPath() {
      if (dataPath == null) {
        this.dataPath = new Path(dataLocation);
      }
      return dataPath;
    }

    private class SparkAppenderFactory implements AppenderFactory<InternalRow> {
      public FileAppender<InternalRow> newAppender(OutputFile file, FileFormat format) {
        Schema schema = spec.schema();
        try {
          switch (format) {
            case PARQUET:
              String jsonSchema = convert(schema).json();
              return Parquet.write(file)
                  .writeSupport(new ParquetWriteSupport())
                  .set("org.apache.spark.sql.parquet.row.attributes", jsonSchema)
                  .set("spark.sql.parquet.writeLegacyFormat", "false")
                  .set("spark.sql.parquet.binaryAsString", "false")
                  .set("spark.sql.parquet.int96AsTimestamp", "false")
                  .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
                  .setAll(properties)
                  .schema(schema)
                  .build();

            case AVRO:
              return Avro.write(file)
                  .createWriterFunc(ignored -> new SparkAvroWriter(schema))
                  .setAll(properties)
                  .schema(schema)
                  .build();

            case ORC: {
              @SuppressWarnings("unchecked")
              SparkOrcWriter writer = new SparkOrcWriter(ORC.write(file)
                  .schema(schema)
                  .build());
              return writer;
            }
            default:
              throw new UnsupportedOperationException("Cannot write unknown format: " + format);
          }
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
      }
    }
  }

  private interface AppenderFactory<T> {
    FileAppender<T> newAppender(OutputFile file, FileFormat format);
  }

  private static class UnpartitionedWriter implements DataWriter<InternalRow>, Closeable {
    private final Path file;
    private final Configuration conf;
    private FileAppender<InternalRow> appender = null;
    private Metrics metrics = null;

    UnpartitionedWriter(Path dataPath, String filename, FileFormat format,
                        Configuration conf, AppenderFactory<InternalRow> factory) {
      this.file = new Path(dataPath, filename);
      this.appender = factory.newAppender(HadoopOutputFile.fromPath(file, conf), format);
      this.conf = conf;
    }

    @Override
    public void write(InternalRow record) {
      appender.add(record);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      Preconditions.checkArgument(appender != null, "Commit called on a closed writer: %s", this);

      close();

      if (metrics.recordCount() == 0L) {
        FileSystem fs = file.getFileSystem(conf);
        fs.delete(file, false);
        return new TaskCommit();
      }

      InputFile inFile = HadoopInputFile.fromPath(file, conf);
      DataFile dataFile = DataFiles.fromInputFile(inFile, null, metrics);

      return new TaskCommit(dataFile);
    }

    @Override
    public void abort() throws IOException {
      Preconditions.checkArgument(appender != null, "Abort called on a closed writer: %s", this);

      close();

      FileSystem fs = file.getFileSystem(conf);
      fs.delete(file, false);
    }

    @Override
    public void close() throws IOException {
      if (this.appender != null) {
        this.appender.close();
        this.metrics = appender.metrics();
        this.appender = null;
      }
    }
  }

  private static class PartitionedWriter implements DataWriter<InternalRow> {
    private final Set<PartitionKey> completedPartitions = Sets.newHashSet();
    private final List<DataFile> completedFiles = Lists.newArrayList();
    private final PartitionSpec spec;
    private final FileFormat format;
    private final Configuration conf;
    private final AppenderFactory<InternalRow> factory;
    private final Function<PartitionKey, Path> outputPathFunc;
    private final PartitionKey key;

    private PartitionKey currentKey = null;
    private FileAppender<InternalRow> currentAppender = null;
    private Path currentPath = null;

    PartitionedWriter(PartitionSpec spec, FileFormat format, Configuration conf,
                      AppenderFactory<InternalRow> factory,
                      Function<PartitionKey, Path> outputPathFunc) {
      this.spec = spec;
      this.format = format;
      this.conf = conf;
      this.factory = factory;
      this.outputPathFunc = outputPathFunc;
      this.key = new PartitionKey(spec);
    }

    @Override
    public void write(InternalRow row) throws IOException {
      key.partition(row);

      if (!key.equals(currentKey)) {
        closeCurrent();

        if (completedPartitions.contains(key)) {
          PartitionKey existingKey = Iterables.find(completedPartitions, key::equals, null);
          LOG.warn("Duplicate key: {} == {}", existingKey, key);
          // if rows are not correctly grouped, detect and fail the write
          throw new IllegalStateException("Already closed file for partition: " + key.toPath());
        }

        this.currentKey = key.copy();
        this.currentPath = outputPathFunc.apply(currentKey);
        OutputFile file = HadoopOutputFile.fromPath(currentPath, conf);
        this.currentAppender = factory.newAppender(file, format);
      }

      currentAppender.add(row);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      closeCurrent();
      return new TaskCommit(completedFiles);
    }

    @Override
    public void abort() throws IOException {
      FileSystem fs = currentPath.getFileSystem(conf);

      // clean up files created by this writer
      Tasks.foreach(completedFiles)
          .throwFailureWhenFinished()
          .noRetry()
          .run(file -> fs.delete(new Path(file.path().toString())), IOException.class);

      if (currentAppender != null) {
        currentAppender.close();
        this.currentAppender = null;
        fs.delete(currentPath);
      }
    }

    private void closeCurrent() throws IOException {
      if (currentAppender != null) {
        currentAppender.close();
        // metrics are only valid after the appender is closed
        Metrics metrics = currentAppender.metrics();
        this.currentAppender = null;

        InputFile inFile = HadoopInputFile.fromPath(currentPath, conf);
        DataFile dataFile = DataFiles.builder(spec)
            .withInputFile(inFile)
            .withPartition(currentKey)
            .withMetrics(metrics)
            .build();

        completedPartitions.add(currentKey);
        completedFiles.add(dataFile);
      }
    }
  }
}
