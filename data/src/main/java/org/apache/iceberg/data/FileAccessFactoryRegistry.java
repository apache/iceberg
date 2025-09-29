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
package org.apache.iceberg.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAccessFactory;
import org.apache.iceberg.io.FileAccessFactory.Combiner;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.iceberg.io.WriteBuilder;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A registry that manages file-format-specific readers and writers through a unified file access
 * factory interface.
 *
 * <p>This registry provides access to {@link ReadBuilder}s for data consumption and various writer
 * builders:
 *
 * <ul>
 *   <li>{@link WriteBuilder} for basic file writing,
 *   <li>{@link DataWriteBuilder} for data files,
 *   <li>{@link EqualityDeleteWriteBuilder} for equality deletes,
 *   <li>{@link PositionDeleteWriteBuilder} for position deletes.
 * </ul>
 *
 * The appropriate builder is selected based on {@link FileFormat} and object model name.
 *
 * <p>File access factories are registered through {@link
 * #registerFileAccessFactory(FileAccessFactory)} and used for creating readers and writers. Read
 * builders are returned directly from the factory. Write builders may be wrapped in specialized
 * content file writer implementations depending on the requested builder type.
 */
public final class FileAccessFactoryRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(FileAccessFactoryRegistry.class);
  // The list of classes which are used for registering the reader and writer builders
  private static final List<String> CLASSES_TO_REGISTER =
      ImmutableList.of(
          "org.apache.iceberg.arrow.vectorized.ArrowReader",
          "org.apache.iceberg.flink.data.FlinkObjectModels",
          "org.apache.iceberg.spark.source.SparkObjectModels");
  public static final String MULTI_THREADED = "multi-threaded";

  private static final Map<Pair<FileFormat, String>, FileAccessFactory<?, ?>>
      FILE_ACCESS_FACTORIES = Maps.newConcurrentMap();

  /**
   * Registers a file access factory with this registry.
   *
   * <p>File access factories create readers and writers for specific combinations of file formats
   * (Parquet, ORC, Avro) and object models ("generic", "spark", "flink", etc.). Registering custom
   * factories allows integration of new data processing engines for the supported file formats with
   * Iceberg's file access mechanisms.
   *
   * <p>Each factory must be uniquely identified by its combination of file format and object model
   * name. This uniqueness constraint prevents ambiguity when selecting factories for read and write
   * operations.
   *
   * @param fileAccessFactory the factory implementation to register
   * @throws IllegalArgumentException if a factory is already registered for the combination of
   *     {@link FileAccessFactory#format()} and {@link FileAccessFactory#objectModelName()}
   */
  @SuppressWarnings("CatchBlockLogException")
  public static void registerFileAccessFactory(FileAccessFactory<?, ?> fileAccessFactory) {
    Pair<FileFormat, String> key =
        Pair.of(fileAccessFactory.format(), fileAccessFactory.objectModelName());
    if (FILE_ACCESS_FACTORIES.containsKey(key)) {
      throw new IllegalArgumentException(
          String.format(
              "File access factory %s clashes with %s. Both serves %s",
              fileAccessFactory.getClass(), FILE_ACCESS_FACTORIES.get(key), key));
    }

    FILE_ACCESS_FACTORIES.put(key, fileAccessFactory);
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void registerSupportedFormats() {
    GenericObjectModels.register();

    // Uses dynamic methods to call the `register` for the listed classes
    for (String classToRegister : CLASSES_TO_REGISTER) {
      try {
        DynMethods.builder("register").impl(classToRegister).buildStaticChecked().invoke();
      } catch (NoSuchMethodException e) {
        // failing to register a factory is normal and does not require a stack trace
        LOG.info("Unable to register {} for data files: {}", classToRegister, e.getMessage());
      }
    }
  }

  static {
    registerSupportedFormats();
  }

  private FileAccessFactoryRegistry() {}

  /**
   * Returns a reader builder for the specified file format and object model.
   *
   * <p>The returned {@link ReadBuilder} provides a fluent interface for configuring how data is
   * read from the input file and converted to the output objects. The builder supports
   * configuration options like schema projection, predicate pushdown, batch size and encryption.
   *
   * @param format the file format (Parquet, Avro, ORC) that determines the parsing implementation
   * @param objectModelName identifier for the expected output data representation (generic, spark,
   *     flink, etc.)
   * @param inputFile source file to read data from
   * @param <D> the type of data records the reader will produce
   * @return a configured reader builder for the specified format and object model
   */
  public static <D> ReadBuilder<?, D> readBuilder(
      FileFormat format, String objectModelName, InputFile inputFile) {
    FileAccessFactory<?, D> factory = factoryFor(format, objectModelName);
    return factory.readBuilder(inputFile);
  }

  public static <D> ReadBuilder<?, D> readBuilder(
      FileFormat format, String objectModelName, Pair<InputFile, Integer[]>[] inputFiles) {
    FileAccessFactory<?, D> factory = factoryFor(format, objectModelName);
    return new CombinedReadBuilder<>(
        Arrays.stream(inputFiles)
            .map(
                p ->
                    Pair.<ReadBuilder<?, D>, Integer[]>of(
                        factory.readBuilder(p.first()), p.second()))
            .collect(Collectors.toSet()),
        factory.combiner());
  }

  private static class CombinedReadBuilder<X> implements ReadBuilder<CombinedReadBuilder<X>, X> {
    private final Set<Pair<ReadBuilder<?, X>, Integer[]>> readBuilders;
    private final BiFunction<Schema, Integer[][], Combiner<X>> combinerBuilder;
    private Schema schema;
    private boolean multiThreaded = false;

    private CombinedReadBuilder(
        Set<Pair<ReadBuilder<?, X>, Integer[]>> readBuilders,
        BiFunction<Schema, Integer[][], Combiner<X>> newCombinerBuilder) {
      this.readBuilders = readBuilders;
      this.combinerBuilder = newCombinerBuilder;
    }

    @Override
    public CombinedReadBuilder<X> split(long newStart, long newLength) {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public CombinedReadBuilder<X> project(Schema newSchema) {
      readBuilders.forEach(
          pair ->
              pair.first()
                  .project(
                      new Schema(
                          Arrays.stream(pair.second())
                              .map(newSchema::findField)
                              .collect(Collectors.toList()))));
      this.schema = newSchema;
      return this;
    }

    @Override
    public CombinedReadBuilder<X> filter(Expression newFilter, boolean filterCaseSensitive) {
      readBuilders.forEach(pair -> pair.first().filter(newFilter, filterCaseSensitive));
      return this;
    }

    @Override
    public CombinedReadBuilder<X> filter(Expression newFilter) {
      readBuilders.forEach(pair -> pair.first().filter(newFilter));
      return this;
    }

    @Override
    public CombinedReadBuilder<X> set(String key, String value) {
      readBuilders.forEach(pair -> pair.first().set(key, value));
      if (MULTI_THREADED.equals(key)) {
        this.multiThreaded = Boolean.parseBoolean(value);
      }

      return this;
    }

    @Override
    public CombinedReadBuilder<X> set(Map<String, String> properties) {
      readBuilders.forEach(pair -> pair.first().set(properties));
      return this;
    }

    @Override
    public CombinedReadBuilder<X> reuseContainers() {
      readBuilders.forEach(pair -> pair.first().reuseContainers());
      return this;
    }

    @Override
    public CombinedReadBuilder<X> constantFieldAccessors(Map<Integer, ?> constantFieldAccessors) {
      readBuilders.forEach(pair -> pair.first().constantFieldAccessors(constantFieldAccessors));
      return this;
    }

    @Override
    public CombinedReadBuilder<X> nameMapping(NameMapping newNameMapping) {
      readBuilders.forEach(pair -> pair.first().nameMapping(newNameMapping));
      return this;
    }

    @Override
    public CombinedReadBuilder<X> fileEncryptionKey(ByteBuffer encryptionKey) {
      readBuilders.forEach(pair -> pair.first().fileEncryptionKey(encryptionKey));
      return this;
    }

    @Override
    public CombinedReadBuilder<X> fileAADPrefix(ByteBuffer aadPrefix) {
      readBuilders.forEach(pair -> pair.first().fileAADPrefix(aadPrefix));
      return this;
    }

    @Override
    public CloseableIterable<X> build() {
      Combiner<X> combiner =
          combinerBuilder.apply(
              schema, readBuilders.stream().map(Pair::second).toArray(Integer[][]::new));
      return FileAccessFactory.combiner(
          readBuilders.stream().map(pair -> pair.first().build()).collect(Collectors.toList()),
          combiner,
          multiThreaded);
    }
  }

  /**
   * Returns a writer builder for appending data to the specified output file.
   *
   * <p>The returned builder produces a {@link FileAppender} that accepts records defined by the
   * specified object model and persists them using the given file format. Data is written to the
   * output file, but this basic writer does not collect or return {@link ContentFile} metadata.
   *
   * @param format the file format used for writing
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> input schema type required by the writer for data conversion
   * @param <D> the type of data records the writer will accept
   * @return a configured writer builder for creating the appender
   */
  public static <E, D> WriteBuilder<?, E, D> writeBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    FileAccessFactory<E, D> factory = factoryFor(format, objectModelName);
    return factory.dataWriteBuilder(outputFile.encryptingOutputFile());
  }

  public static <E, D> WriteBuilder<?, E, D> writeBuilder(
      FileFormat format,
      String objectModelName,
      Pair<EncryptedOutputFile, Integer[]>[] outputFiles) {
    FileAccessFactory<E, D> factory = factoryFor(format, objectModelName);
    return new CombinedWriteBuilder<>(
        Arrays.stream(outputFiles)
            .map(
                p ->
                    Pair.<WriteBuilder<?, E, D>, Integer[]>of(
                        factory.dataWriteBuilder(p.first().encryptingOutputFile()), p.second()))
            .collect(Collectors.toSet()),
        factory.narrower());
  }

  private static class CombinedWriteBuilder<F, X>
      implements WriteBuilder<CombinedWriteBuilder<F, X>, F, X> {
    private final Set<Pair<WriteBuilder<?, F, X>, Integer[]>> writeBuilders;
    private final BiFunction<Schema, Integer[], FileAccessFactory.Narrower<X>> narrower;
    private Schema fileSchema;
    private boolean multiThreaded = false;

    private CombinedWriteBuilder(
        Set<Pair<WriteBuilder<?, F, X>, Integer[]>> writeBuilders,
        BiFunction<Schema, Integer[], FileAccessFactory.Narrower<X>> narrower) {
      this.writeBuilders = writeBuilders;
      this.narrower = narrower;
    }

    @Override
    public CombinedWriteBuilder<F, X> fileSchema(Schema newSchema) {
      writeBuilders.forEach(
          pair ->
              pair.first()
                  .fileSchema(
                      new Schema(
                          Arrays.stream(pair.second())
                              .map(newSchema::findField)
                              .collect(Collectors.toList()))));
      fileSchema = newSchema;
      return this;
    }

    @Override
    public CombinedWriteBuilder<F, X> set(String property, String value) {
      writeBuilders.forEach(pair -> pair.first().set(property, value));
      if (MULTI_THREADED.equals(property)) {
        this.multiThreaded = Boolean.parseBoolean(value);
      }

      return this;
    }

    @Override
    public CombinedWriteBuilder<F, X> meta(String property, String value) {
      writeBuilders.forEach(pair -> pair.first().meta(property, value));
      return this;
    }

    @Override
    public CombinedWriteBuilder<F, X> metricsConfig(MetricsConfig newMetricsConfig) {
      writeBuilders.forEach(pair -> pair.first().metricsConfig(newMetricsConfig));
      return this;
    }

    @Override
    public CombinedWriteBuilder<F, X> overwrite() {
      writeBuilders.forEach(pair -> pair.first().overwrite());
      return this;
    }

    @Override
    public CombinedWriteBuilder<F, X> dataSchema(F newDataSchema) {
      writeBuilders.forEach(pair -> pair.first().dataSchema(newDataSchema));
      return this;
    }

    @Override
    public CombinedWriteBuilder<F, X> fileEncryptionKey(ByteBuffer encryptionKey) {
      writeBuilders.forEach(pair -> pair.first().fileEncryptionKey(encryptionKey));
      return this;
    }

    @Override
    public CombinedWriteBuilder<F, X> fileAADPrefix(ByteBuffer aadPrefix) {
      writeBuilders.forEach(pair -> pair.first().fileAADPrefix(aadPrefix));
      return this;
    }

    @Override
    public FileAppender<X> build() throws IOException {
      List<Pair<FileAppender<X>, FileAccessFactory.Narrower<X>>> appenders =
          Lists.newArrayListWithCapacity(writeBuilders.size());
      for (Pair<WriteBuilder<?, F, X>, Integer[]> pair : writeBuilders) {
        appenders.add(Pair.of(pair.first().build(), narrower.apply(fileSchema, pair.second())));
      }

      return multiThreaded
          ? new MultiThreadedFileAppender(appenders)
          : new SingleThreadedFileAppender(appenders);
    }

    private class SingleThreadedFileAppender implements FileAppender<X> {
      private final List<Pair<FileAppender<X>, FileAccessFactory.Narrower<X>>> appenders;

      private SingleThreadedFileAppender(
          List<Pair<FileAppender<X>, FileAccessFactory.Narrower<X>>> appenders) {
        this.appenders = appenders;
      }

      @Override
      public void add(X record) {
        appenders.forEach(pair -> pair.first().add(pair.second().narrow(record)));
      }

      @Override
      public Metrics metrics() {
        long rowCount = 0;
        Map<Integer, Long> columnSizes = Maps.newHashMap();
        Map<Integer, Long> valueCounts = Maps.newHashMap();
        Map<Integer, Long> nullValueCounts = Maps.newHashMap();
        Map<Integer, Long> nanValueCounts = Maps.newHashMap();
        Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
        Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

        for (Pair<FileAppender<X>, FileAccessFactory.Narrower<X>> pair : appenders) {
          Metrics metrics = pair.first().metrics();
          rowCount = metrics.recordCount() != null ? rowCount + metrics.recordCount() : rowCount;
          if (metrics.columnSizes() != null) {
            columnSizes.putAll(metrics.columnSizes());
          }

          if (metrics.valueCounts() != null) {
            valueCounts.putAll(metrics.valueCounts());
          }

          if (metrics.nullValueCounts() != null) {
            nullValueCounts.putAll(metrics.nullValueCounts());
          }

          if (metrics.nanValueCounts() != null) {
            nanValueCounts.putAll(metrics.nanValueCounts());
          }

          if (metrics.lowerBounds() != null) {
            lowerBounds.putAll(metrics.lowerBounds());
          }

          if (metrics.upperBounds() != null) {
            upperBounds.putAll(metrics.upperBounds());
          }
        }

        return new Metrics(
            rowCount,
            columnSizes,
            valueCounts,
            nullValueCounts,
            nanValueCounts,
            lowerBounds,
            upperBounds);
      }

      @Override
      public long length() {
        return appenders.stream().mapToLong(pair -> pair.first().length()).sum();
      }

      @Override
      public void close() throws IOException {
        for (Pair<FileAppender<X>, FileAccessFactory.Narrower<X>> pair : appenders) {
          pair.first().close();
        }
      }

      @Override
      public String toString() {
        return "CombinedFileAppender{" + "appenders=" + appenders + '}';
      }
    }

    public static final Object EOF = new Object();

    private class MultiThreadedFileAppender implements FileAppender<X> {
      private final List<Pair<FileAppender<X>, FileAccessFactory.Narrower<X>>> appenders;
      private final ExecutorService executorService;
      private final List<BlockingQueue<Object>> queues;
      private final CountDownLatch finished;
      private boolean working = false;

      private MultiThreadedFileAppender(
          List<Pair<FileAppender<X>, FileAccessFactory.Narrower<X>>> appenders) {
        this.appenders = appenders;
        this.executorService =
            Executors.newFixedThreadPool(
                appenders.size(),
                new ThreadFactory() {
                  private final AtomicInteger threadNumber = new AtomicInteger(1);

                  @Override
                  public Thread newThread(Runnable r) {
                    return new Thread(
                        r, "MultiThreadedFileAppender-" + threadNumber.getAndIncrement());
                  }
                });
        this.queues =
            appenders.stream()
                .map(i -> Queues.newLinkedBlockingDeque(100))
                .collect(Collectors.toList());
        this.finished = new CountDownLatch(appenders.size());
      }

      @Override
      public void add(X record) {
        queues.forEach(
            q -> {
              try {
                q.put(record);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });
        if (!working) {
          working = true;
          for (int i = 0; i < appenders.size(); i++) {
            final BlockingQueue<Object> queue = queues.get(i);
            final Pair<FileAppender<X>, FileAccessFactory.Narrower<X>> appender = appenders.get(i);
            final int index = i;
            executorService.execute(
                () -> {
                  try {
                    while (true) {
                      Object next = queue.take();
                      if (next == EOF) {
                        finished.countDown();
                        return;
                      }
                      appender.first().add(appender.second().narrow((X) next));
                    }
                  } catch (Exception e) {
                    LOG.error("Error processing records in appender {}", index, e);
                  }
                });
          }
        }
      }

      @Override
      public Metrics metrics() {
        long rowCount = 0;
        Map<Integer, Long> columnSizes = Maps.newHashMap();
        Map<Integer, Long> valueCounts = Maps.newHashMap();
        Map<Integer, Long> nullValueCounts = Maps.newHashMap();
        Map<Integer, Long> nanValueCounts = Maps.newHashMap();
        Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
        Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

        for (Pair<FileAppender<X>, FileAccessFactory.Narrower<X>> pair : appenders) {
          Metrics metrics = pair.first().metrics();
          rowCount = metrics.recordCount() != null ? rowCount + metrics.recordCount() : rowCount;
          if (metrics.columnSizes() != null) {
            columnSizes.putAll(metrics.columnSizes());
          }

          if (metrics.valueCounts() != null) {
            valueCounts.putAll(metrics.valueCounts());
          }

          if (metrics.nullValueCounts() != null) {
            nullValueCounts.putAll(metrics.nullValueCounts());
          }

          if (metrics.nanValueCounts() != null) {
            nanValueCounts.putAll(metrics.nanValueCounts());
          }

          if (metrics.lowerBounds() != null) {
            lowerBounds.putAll(metrics.lowerBounds());
          }

          if (metrics.upperBounds() != null) {
            upperBounds.putAll(metrics.upperBounds());
          }
        }

        return new Metrics(
            rowCount,
            columnSizes,
            valueCounts,
            nullValueCounts,
            nanValueCounts,
            lowerBounds,
            upperBounds);
      }

      @Override
      public long length() {
        return appenders.stream().mapToLong(pair -> pair.first().length()).sum();
      }

      @Override
      public void close() throws IOException {
        queues.forEach(
            queue -> {
              try {
                queue.put(EOF);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            });
        try {
          finished.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for appender to finish", e);
        }

        for (Pair<FileAppender<X>, FileAccessFactory.Narrower<X>> pair : appenders) {
          pair.first().close();
        }

        executorService.shutdown();
      }

      @Override
      public String toString() {
        return "CombinedFileAppender{" + "appenders=" + appenders + '}';
      }
    }
  }

  /**
   * Returns a writer builder for generating a {@link DataFile}.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the provided file format. Unlike basic writers, this writer
   * collects file metadata during the writing process and generates a {@link DataFile} that can be
   * used for table operations.
   *
   * @param format the file format used for writing
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> input schema type required by the writer for data conversion
   * @param <D> the type of data records the writer will accept
   * @return a configured data write builder for creating a {@link DataWriter}
   */
  public static <E, D> DataWriteBuilder<?, E, D> dataWriteBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    FileAccessFactory<E, D> factory = factoryFor(format, objectModelName);
    WriteBuilder<?, E, D> writeBuilder =
        factory.equalityDeleteWriteBuilder(outputFile.encryptingOutputFile());
    return ContentFileWriteBuilderImpl.forDataFile(
        writeBuilder, outputFile.encryptingOutputFile().location(), format);
  }

  /**
   * Creates a writer builder for generating a {@link DeleteFile} with equality deletes.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the given file format. The writer persists equality delete
   * records that identify rows to be deleted based on the configured equality fields, producing a
   * {@link DeleteFile} that can be used for table operations.
   *
   * @param format the file format used for writing
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> input schema type required by the writer for data conversion
   * @param <D> the type of data records the writer will accept
   * @return a configured delete write builder for creating an {@link EqualityDeleteWriter}
   */
  public static <E, D> EqualityDeleteWriteBuilder<?, E, D> equalityDeleteWriteBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    FileAccessFactory<E, D> factory = factoryFor(format, objectModelName);
    WriteBuilder<?, E, D> writeBuilder =
        factory.equalityDeleteWriteBuilder(outputFile.encryptingOutputFile());
    return ContentFileWriteBuilderImpl.forEqualityDelete(
        writeBuilder, outputFile.encryptingOutputFile().location(), format);
  }

  /**
   * Creates a writer builder for generating a {@link DeleteFile} with position-based deletes.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the given file format. The writer accepts {@link PositionDelete}
   * records that identify rows to be deleted by file path and position, producing a {@link
   * DeleteFile} that can be used for table operations.
   *
   * @param format the file format used for writing
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> input schema type required by the writer for data conversion
   * @param <D> the type of data records contained in the {@link PositionDelete} that the writer
   *     will accept
   * @return a configured delete write builder for creating a {@link PositionDeleteWriter}
   */
  public static <E, D> PositionDeleteWriteBuilder<?, E, D> positionDeleteWriteBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    FileAccessFactory<E, D> factory = factoryFor(format, objectModelName);
    WriteBuilder<?, E, PositionDelete<D>> writeBuilder =
        factory.positionDeleteWriteBuilder(outputFile.encryptingOutputFile());
    return ContentFileWriteBuilderImpl.forPositionDelete(
        writeBuilder, outputFile.encryptingOutputFile().location(), format);
  }

  @SuppressWarnings("unchecked")
  private static <E, D> FileAccessFactory<E, D> factoryFor(
      FileFormat format, String objectModelName) {
    return ((FileAccessFactory<E, D>) FILE_ACCESS_FACTORIES.get(Pair.of(format, objectModelName)));
  }
}
